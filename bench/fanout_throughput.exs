defmodule Bench.FanoutThroughput do
  @moduledoc """
  Compare broadcast throughput between :rpc.cast (N calls per message)
  and NATS (1 publish per message) when fanning out to K peer nodes.

  The main node sends M broadcast messages. Each peer counts how many messages
  it receives. We measure the total time for the main node to send all messages
  and verify that every peer received all M messages.
  """

  @node_count 20
  @message_count 500

  def run do
    ensure_net_kernel!()
    peers = for _ <- 1..@node_count, do: start_peer()
    nodes = Enum.map(peers, fn {_, _, node} -> node end)

    IO.puts("Started #{@node_count} peer nodes\n")

    rpc_ms = benchmark_rpc(nodes)
    nats_ms = benchmark_nats(peers)

    IO.puts("\n=== Summary ===")
    IO.puts("Nodes:           #{@node_count}")
    IO.puts("Messages:        #{@message_count}")
    IO.puts("RPC total time:  #{Float.round(rpc_ms, 2)} ms")
    IO.puts("NATS total time: #{Float.round(nats_ms, 2)} ms")
    IO.puts("Speedup:         #{Float.round(rpc_ms / nats_ms, 2)}x")

    Enum.each(peers, fn {pid, _, _} -> :peer.stop(pid) end)
  end

  defp ensure_net_kernel! do
    node_name = :"main@127.0.0.1"

    case :net_kernel.start([node_name]) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
      {:error, reason} -> raise "Failed to start net_kernel: #{inspect(reason)}"
    end

    true = :erlang.set_cookie(:cookie)
  end

  defp start_peer do
    {:ok, pid, node} =
      :peer.start_link(%{
        name: :peer.random_name(),
        host: ~c"127.0.0.1",
        longnames: true,
        connection: :standard_io
      })

    true = :peer.call(pid, :erlang, :set_cookie, [:cookie])
    :ok = :peer.call(pid, :code, :add_paths, [:code.get_path()])
    {pid, nil, node}
  end

  defp benchmark_rpc(nodes) do
    true = Node.connect(hd(nodes))
    Process.sleep(100)

    main = self()

    # Each peer starts a counter process. It receives :ping, increments, and
    # replies to {:count, reply_to} with the current count.
    counter_pids =
      Enum.map(nodes, fn node ->
        {{pid, nil}, _} =
          :rpc.call(
            node,
            Code,
            :eval_quoted,
            [
              quote do
                pid =
                  spawn(fn ->
                    loop = fn count, loop ->
                      receive do
                        :ping -> loop.(count + 1, loop)
                        {:count, reply_to} ->
                          send(reply_to, count)
                          loop.(count, loop)
                      after
                        10_000 -> loop.(count, loop)
                      end
                    end

                    loop.(0, loop)
                  end)

                {pid, nil}
              end
            ]
          )

        pid
      end)

    Process.sleep(200)

    {rpc_us, _} =
      :timer.tc(fn ->
        for _ <- 1..@message_count,
            {node, counter_pid} <- Enum.zip(nodes, counter_pids) do
          true = :rpc.cast(node, Kernel, :send, [counter_pid, :ping])
        end
      end)

    # Wait for all casts to be processed.
    Process.sleep(1000)

    # Verify all peers received all messages.
    Enum.zip(nodes, counter_pids)
    |> Enum.each(fn {node, counter_pid} ->
      :rpc.cast(node, Kernel, :send, [counter_pid, {:count, self()}])

      receive do
        count when count == @message_count -> :ok
        other -> raise "RPC peer #{node} got #{other} messages, expected #{@message_count}"
      after
        1000 -> raise "RPC counter timeout"
      end
    end)

    rpc_ms = rpc_us / 1000

    IO.puts("RPC broadcast:")
    IO.puts("  total send time: #{Float.round(rpc_ms, 2)} ms")
    IO.puts("  sends/sec:       #{Float.round(@message_count * 1000 / rpc_ms, 2)}")
    IO.puts("  per node:        #{Float.round(rpc_ms / @node_count, 3)} ms")

    rpc_ms
  end

  defp benchmark_nats(peers) do
    nats_host = System.get_env("NATS_HOST", "127.0.0.1")
    nats_port = String.to_integer(System.get_env("NATS_PORT", "4222"))
    nats_token = System.get_env("NATS_TOKEN")

    connection_opts =
      %{host: nats_host, port: nats_port, tls: false}
      |> then(fn opts -> if nats_token, do: Map.put(opts, :token, nats_token), else: opts end)

    {:ok, conn} = Gnat.start_link(connection_opts)

    # Each peer starts a counter and a NATS subscriber to "realtime.fanout".
    counter_pids =
      Enum.map(peers, fn {pid, _, _node} ->
        {{counter_pid, nil}, _} =
          :peer.call(
            pid,
            Code,
            :eval_quoted,
            [
              quote do
                connection_opts = unquote(Macro.escape(connection_opts))

                counter =
                  spawn(fn ->
                    loop = fn count, loop ->
                      receive do
                        :inc -> loop.(count + 1, loop)
                        {:count, reply_to} ->
                          send(reply_to, count)
                          loop.(count, loop)
                      after
                        10_000 -> loop.(count, loop)
                      end
                    end

                    loop.(0, loop)
                  end)

                spawn(fn ->
                  {:ok, peer_conn} = Gnat.start_link(connection_opts)
                  {:ok, _} = Gnat.sub(peer_conn, self(), "realtime.fanout")

                  loop = fn loop, peer_conn ->
                    receive do
                      {:msg, %{topic: "realtime.fanout"}} ->
                        send(counter, :inc)
                        loop.(loop, peer_conn)
                    after
                      10_000 -> loop.(loop, peer_conn)
                    end
                  end

                  loop.(loop, peer_conn)
                end)

                {counter, nil}
              end
            ]
          )

        counter_pid
      end)

    Process.sleep(500)

    {nats_us, _} =
      :timer.tc(fn ->
        for _ <- 1..@message_count do
          :ok = Gnat.pub(conn, "realtime.fanout", "ping")
        end
      end)

    # Wait for all messages to be delivered.
    Process.sleep(1000)

    # Verify all peers received all messages.
    Enum.zip(peers, counter_pids)
    |> Enum.each(fn {{pid, _, node}, counter_pid} ->
      :peer.call(pid, Kernel, :send, [counter_pid, {:count, self()}])

      receive do
        count when count == @message_count -> :ok
        other -> raise "NATS peer #{node} got #{other} messages, expected #{@message_count}"
      after
        1000 -> raise "NATS counter timeout"
      end
    end)

    nats_ms = nats_us / 1000

    IO.puts("\nNATS broadcast:")
    IO.puts("  total send time: #{Float.round(nats_ms, 2)} ms")
    IO.puts("  sends/sec:       #{Float.round(@message_count * 1000 / nats_ms, 2)}")
    IO.puts("  per node:        #{Float.round(nats_ms / @node_count, 3)} ms")

    nats_ms
  end
end

Bench.FanoutThroughput.run()
