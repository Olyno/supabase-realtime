defmodule RealtimeWeb.TenantBroadcaster do
  @moduledoc """
  Broadcasts tenant messages either through the default Phoenix.PubSub stack
  (backed by gen_rpc for inter-node fan-out) or through a configurable broker
  such as NATS.
  """

  alias Phoenix.PubSub

  @type message_type :: :broadcast | :presence | :postgres_changes

  @spec pubsub_direct_broadcast(
          node :: node(),
          tenant_id :: String.t(),
          PubSub.topic(),
          PubSub.message(),
          PubSub.dispatcher(),
          message_type
        ) ::
          :ok
  def pubsub_direct_broadcast(node, tenant_id, topic, message, dispatcher, message_type) do
    collect_payload_size(tenant_id, message, message_type)

    if broker_enabled?() do
      broker().publish(topic, message, dispatcher: dispatcher)
    else
      do_direct_broadcast(node, topic, message, dispatcher)
    end

    :ok
  end

  # Remote
  defp do_direct_broadcast(node, topic, message, dispatcher) when node != node() do
    PubSub.direct_broadcast(node, Realtime.PubSub, topic, message, dispatcher)
  end

  # Local
  defp do_direct_broadcast(_node, topic, message, dispatcher) do
    PubSub.local_broadcast(Realtime.PubSub, topic, message, dispatcher)
  end

  @spec pubsub_broadcast(tenant_id :: String.t(), PubSub.topic(), PubSub.message(), PubSub.dispatcher(), message_type) ::
          :ok
  def pubsub_broadcast(tenant_id, topic, message, dispatcher, message_type) do
    collect_payload_size(tenant_id, message, message_type)

    if broker_enabled?() do
      broker().publish(topic, message, dispatcher: dispatcher)
    else
      PubSub.broadcast(Realtime.PubSub, topic, message, dispatcher)
    end

    :ok
  end

  @spec pubsub_broadcast_from(
          tenant_id :: String.t(),
          from :: pid,
          PubSub.topic(),
          PubSub.message(),
          PubSub.dispatcher(),
          message_type
        ) ::
          :ok
  def pubsub_broadcast_from(tenant_id, from, topic, message, dispatcher, message_type) do
    collect_payload_size(tenant_id, message, message_type)

    if broker_enabled?() do
      broker().publish(topic, message, dispatcher: dispatcher)
    else
      PubSub.broadcast_from(Realtime.PubSub, from, topic, message, dispatcher)
    end

    :ok
  end

  defp broker_enabled? do
    Application.get_env(:realtime, :broker_enabled, false) and
      sufficient_nodes_for_broker?()
  end

  defp sufficient_nodes_for_broker? do
    min_nodes = Application.get_env(:realtime, :broker_min_nodes, 0)

    if min_nodes <= 1 do
      true
    else
      region = Application.get_env(:realtime, :region)
      length(Realtime.Nodes.region_nodes(region)) >= min_nodes
    end
  end

  defp broker do
    Application.get_env(:realtime, :broker, Realtime.Broker.Nats)
  end

  @payload_size_event [:realtime, :tenants, :payload, :size]

  @spec collect_payload_size(tenant_id :: String.t(), payload :: term, message_type :: message_type) :: :ok
  def collect_payload_size(tenant_id, payload, message_type) when is_struct(payload) do
    # Extracting from struct so the __struct__ bit is not calculated as part of the payload
    collect_payload_size(tenant_id, Map.from_struct(payload), message_type)
  end

  def collect_payload_size(tenant_id, payload, message_type) do
    :telemetry.execute(@payload_size_event, %{size: :erlang.external_size(payload)}, %{
      tenant: tenant_id,
      message_type: message_type
    })
  end
end
