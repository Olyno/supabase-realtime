defmodule Realtime.Broker.Nats do
  @moduledoc """
  NATS backed implementation of `Realtime.Broker`.

  Publishes tenant-scoped messages to a NATS subject and subscribes a single
  consumer per node. The consumer forwards messages to the local Phoenix.PubSub
  instance so that existing channel dispatchers keep working unchanged.

  This is intentionally a thin layer: the goal is to replace the inter-node
  gen_rpc transport with NATS without rewriting the broadcast logic.
  """

  use GenServer
  use Realtime.Logs

  alias Realtime.Telemetry

  @behaviour Realtime.Broker

  @default_host "127.0.0.1"
  @default_port 4222

  defstruct [:conn, :pubsub, :consumer_name]

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @impl Realtime.Broker
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [Keyword.put_new(opts, :name, __MODULE__)]},
      type: :worker,
      restart: :permanent
    }
  end

  @impl Realtime.Broker
  def publish(topic, message, opts \\ []) do
    broker_pid = Keyword.get(opts, :broker_pid) || Process.whereis(__MODULE__)

    if broker_pid do
      GenServer.call(broker_pid, {:publish, topic, message, opts})
    else
      {:error, :broker_not_running}
    end
  end

  @impl Realtime.Broker
  def subscribe(topic, _opts \\ []) do
    Phoenix.PubSub.subscribe(Realtime.PubSub, topic)
  end

  @impl Realtime.Broker
  def unsubscribe(topic) do
    Phoenix.PubSub.unsubscribe(Realtime.PubSub, topic)
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    Process.flag(:fullsweep_after, 20)

    host = Keyword.get(opts, :host, @default_host)
    port = Keyword.get(opts, :port, @default_port)
    token = Keyword.get(opts, :token)
    pubsub = Keyword.get(opts, :pubsub, Realtime.PubSub)
    consumer_name = Keyword.get(opts, :consumer_name, consumer_name())

    connection_opts =
      %{host: host, port: port, tls: false}
      |> maybe_put(:token, token)

    case Gnat.start_link(connection_opts) do
      {:ok, conn} ->
        {:ok, _sid} = Gnat.sub(conn, self(), "realtime.>")

        Logger.info("Connected to NATS broker at #{host}:#{port} as consumer #{consumer_name}")

        {:ok, %__MODULE__{conn: conn, pubsub: pubsub, consumer_name: consumer_name}}

      {:error, reason} ->
        log_error("NatsConnectionFailed", %{host: host, port: port, reason: inspect(reason)})
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:publish, topic, message, opts}, _from, state) do
    subject = to_subject(topic)
    dispatcher = Keyword.get(opts, :dispatcher)
    payload = :erlang.term_to_binary({topic, message, dispatcher})

    case Gnat.pub(state.conn, subject, payload) do
      :ok ->
        Telemetry.execute([:realtime, :broker, :nats, :publish], %{}, %{topic: topic})
        {:reply, :ok, state}

      {:error, reason} = error ->
        log_error("NatsPublishFailed", %{topic: topic, reason: reason})
        {:reply, error, state}
    end
  end

  @impl true
  def handle_info({:msg, %{topic: subject, body: payload}}, state) do
    try do
      {topic, message, dispatcher} = :erlang.binary_to_term(payload)

      if dispatcher do
        Phoenix.PubSub.local_broadcast(state.pubsub, topic, message, dispatcher)
      else
        Phoenix.PubSub.local_broadcast(state.pubsub, topic, message)
      end

      Telemetry.execute([:realtime, :broker, :nats, :receive], %{}, %{topic: topic})
    rescue
      error ->
        log_error("NatsDecodeFailed", %{subject: subject, error: inspect(error)})
    end

    {:noreply, state}
  end

  def handle_info(%{event: "disconnect", payload: _}, state) do
    Logger.warning("NATS broker connection closed, attempting reconnect")
    {:stop, :nats_disconnected, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  def to_subject(topic), do: "realtime." <> topic

  defp consumer_name do
    node()
    |> Atom.to_string()
    |> String.replace(["@", ".", ":"], "_")
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
