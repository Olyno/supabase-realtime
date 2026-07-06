nats_host = System.get_env("NATS_HOST", "127.0.0.1")
nats_port = String.to_integer(System.get_env("NATS_PORT", "4222"))
nats_token = System.get_env("NATS_TOKEN")

connection_opts =
  %{host: nats_host, port: nats_port, tls: false}
  |> then(fn opts -> if nats_token, do: Map.put(opts, :token, nats_token), else: opts end)

{:ok, conn} = Gnat.start_link(connection_opts)

message = %{event: "broadcast", payload: %{"user" => "bench", "id" => 1}}
topic = "realtime:bench:topic"
nats_payload = :erlang.term_to_binary({topic, message, nil})

# Start the Realtime broker wrapper.
{:ok, _broker} =
  Realtime.Broker.Nats.start_link(
    host: nats_host,
    port: nats_port,
    token: nats_token,
    pubsub: Realtime.PubSub,
    name: Realtime.Broker.Nats.Bench
  )

# Spawn a dedicated NATS receiver so publishes don't block on receive.
receiver =
  spawn(fn ->
    {:ok, _} = Gnat.sub(conn, self(), "realtime.bench.topic")

    receive do
      :stop -> :ok
    after
      :infinity -> :ok
    end
  end)

Process.sleep(100)

Benchee.run(
  %{
    "raw gnat publish" => fn ->
      :ok = Gnat.pub(conn, "realtime.bench.topic", nats_payload)
    end,
    "realtime broker nats publish" => fn ->
      :ok = Realtime.Broker.Nats.publish(topic, message, broker_pid: Realtime.Broker.Nats.Bench)
    end
  },
  warmup: 1,
  time: 5,
  memory_time: 1
)

send(receiver, :stop)
