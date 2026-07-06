defmodule Realtime.Broker.NatsTest do
  use ExUnit.Case, async: false

  alias Realtime.Broker.Nats

  describe "child_spec/1" do
    test "returns a supervisor child spec" do
      assert %{
               id: Nats,
               start: {Nats, :start_link, [[name: Nats]]},
               type: :worker,
               restart: :permanent
             } = Nats.child_spec([])
    end
  end

  describe "subject encoding" do
    test "prefixes Phoenix topic with realtime namespace" do
      assert Nats.to_subject("tenant:public:foo") == "realtime.tenant:public:foo"
    end
  end

  describe "publish/3" do
    @moduletag :nats

    setup do
      {:ok, pid} = Nats.start_link(host: "127.0.0.1", port: 4222)

      on_exit(fn ->
        if Process.alive?(pid), do: GenServer.stop(pid)
      end)

      {:ok, pid: pid}
    end

    test "publishes encoded message to Gnat", %{pid: pid} do
      assert :ok =
               Nats.publish("tenant:public:foo", %{event: "test"},
                 dispatcher: SomeDispatcher,
                 broker_pid: pid
               )
    end
  end
end
