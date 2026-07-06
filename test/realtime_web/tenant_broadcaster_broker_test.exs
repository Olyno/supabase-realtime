defmodule RealtimeWeb.TenantBroadcasterBrokerTest do
  use ExUnit.Case, async: false

  import Mimic

  alias Phoenix.Socket.Broadcast
  alias RealtimeWeb.TenantBroadcaster

  setup :verify_on_exit!

  setup do
    previous_broker_enabled = Application.get_env(:realtime, :broker_enabled)
    previous_broker = Application.get_env(:realtime, :broker)
    previous_broker_min_nodes = Application.get_env(:realtime, :broker_min_nodes)

    Application.put_env(:realtime, :broker_enabled, true)
    Application.put_env(:realtime, :broker, Realtime.Broker.Nats)
    Application.put_env(:realtime, :broker_min_nodes, 0)

    on_exit(fn ->
      Application.put_env(:realtime, :broker_enabled, previous_broker_enabled)
      Application.put_env(:realtime, :broker, previous_broker)
      Application.put_env(:realtime, :broker_min_nodes, previous_broker_min_nodes)
    end)

    :ok
  end

  describe "broker mode" do
    test "pubsub_broadcast routes through broker" do
      tenant_id = "tenant-#{System.unique_integer([:positive])}"
      topic = "test-topic"
      message = %Broadcast{topic: topic, event: "an event", payload: %{"a" => "b"}}

      expect(Realtime.Broker.Nats, :publish, fn ^topic, ^message, opts ->
        assert opts[:dispatcher] == Phoenix.PubSub
        :ok
      end)

      assert :ok = TenantBroadcaster.pubsub_broadcast(tenant_id, topic, message, Phoenix.PubSub, :broadcast)
    end

    test "pubsub_broadcast_from routes through broker" do
      tenant_id = "tenant-#{System.unique_integer([:positive])}"
      topic = "test-topic"
      message = %Broadcast{topic: topic, event: "an event", payload: %{"a" => "b"}}

      expect(Realtime.Broker.Nats, :publish, fn ^topic, ^message, opts ->
        assert opts[:dispatcher] == Phoenix.PubSub
        :ok
      end)

      assert :ok =
               TenantBroadcaster.pubsub_broadcast_from(tenant_id, self(), topic, message, Phoenix.PubSub, :broadcast)
    end

    test "pubsub_direct_broadcast routes through broker" do
      tenant_id = "tenant-#{System.unique_integer([:positive])}"
      topic = "test-topic"
      message = %Broadcast{topic: topic, event: "an event", payload: %{"a" => "b"}}

      expect(Realtime.Broker.Nats, :publish, fn ^topic, ^message, opts ->
        assert opts[:dispatcher] == Phoenix.PubSub
        :ok
      end)

      assert :ok =
               TenantBroadcaster.pubsub_direct_broadcast(node(), tenant_id, topic, message, Phoenix.PubSub, :broadcast)
    end
  end

  describe "hybrid broker mode" do
    test "falls back to PubSub when cluster is smaller than BROKER_MIN_NODES" do
      tenant_id = "tenant-#{System.unique_integer([:positive])}"
      topic = "test-topic"
      message = %Broadcast{topic: topic, event: "an event", payload: %{"a" => "b"}}

      Application.put_env(:realtime, :broker_min_nodes, 5)
      region = Application.get_env(:realtime, :region)

      expect(Realtime.Nodes, :region_nodes, fn ^region -> [node()] end)
      reject(&Realtime.Broker.Nats.publish/3)

      assert :ok = TenantBroadcaster.pubsub_broadcast(tenant_id, topic, message, Phoenix.PubSub, :broadcast)
    end

    test "uses broker when cluster size reaches BROKER_MIN_NODES" do
      tenant_id = "tenant-#{System.unique_integer([:positive])}"
      topic = "test-topic"
      message = %Broadcast{topic: topic, event: "an event", payload: %{"a" => "b"}}

      Application.put_env(:realtime, :broker_min_nodes, 2)
      region = Application.get_env(:realtime, :region)

      expect(Realtime.Nodes, :region_nodes, fn ^region ->
        [node(), :"peer@127.0.0.1"]
      end)

      expect(Realtime.Broker.Nats, :publish, fn ^topic, ^message, _opts -> :ok end)

      assert :ok = TenantBroadcaster.pubsub_broadcast(tenant_id, topic, message, Phoenix.PubSub, :broadcast)
    end
  end
end
