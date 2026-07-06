defmodule Bench.Fanout do
  @moduledoc """
  Compare fan-out broadcast efficiency between :rpc.abcast and NATS.

  K peer nodes are started. One message is broadcast from the main node to all K
  peers. Each peer acknowledges back. The total time to receive K acknowledgments
  is measured.

  With :rpc.abcast, the main node sends directly to each peer (linear cost).
  With NATS, the main node publishes once to the broker and the broker fans out
  to all K subscribers (constant send cost).
  """

  @node_count 20
  @rounds 10

  def run do
    ensure_net_kernel!()
    peers = for _ <- 1..@node_count, do: start_peer()
    nodes = Enum.map(peers, fn {_, _, node} -> node end)

    IO.puts("Started #{@node_count} peer nodes: #{inspect(nodes)}\n")

    rpc_ms = benchmark_rpc(nodes)
    nats_ms = benchmark_nats(peers)

    IO.puts("\n=== Summary ===")
    IO.puts("Nodes: #{@node_count}")
    IO.puts("RPC abcast fan-out: #{Float.round(rpc_ms, 2)} ms average")
    IO.puts("NATS fan-out:       #{Float.round(nats_ms, 2)} ms average")
    IO.puts("Speedup:            #{Float.round(rpc_ms / nats_ms, 2)}x")

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

    # Start a drainer on each peer that replies :pong to the main node.
    drain_pids =
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
                    loop = fn loop ->
                      receive do
                        {:ping, reply_to} ->
                          send(reply_to, :pong)
                          loop.(loop)
                      after
                        5000 -> loop.(loop)
                      end
                    end

                    loop.(loop)
                  end)

                {pid, nil}
              end
            ]
          )

        pid
      end)

    Process.sleep(200)
    main = self()

    times =
      for _ <- 1..@rounds do
        {t, _} =
          :timer.tc(fn ->
            for {node, drain_pid} <- Enum.zip(nodes, drain_pids) do
              true = :rpc.cast(node, Kernel, :send, [drain_pid, {:ping, main}])
            end

            for _ <- 1..@node_count do
              receive do
                :pong -> :ok
              after
                5000 -> raise "RPC fan-out timeout"
              end
            end
          end)

        t
      end

    avg_us = Enum.sum(times) / length(times)
    avg_ms = avg_us / 1000

    IO.puts("RPC abcast fan-out:")
    IO.puts("  rounds:    #{@rounds}")
    IO.puts("  average:   #{Float.round(avg_ms, 2)} ms")
    IO.puts("  per node:  #{Float.round(avg_ms / @node_count, 3)} ms")
    avg_ms
  end

  defp benchmark_nats(peers) do
    nats_host = System.get_env("NATS_HOST", "127.0.0.1")
    nats_port = String.to_integer(System.get_env("NATS_PORT", "4222"))
    nats_token = System.get_env("NATS_TOKEN")

    connection_opts =
      %{host: nats_host, port: nats_port, tls: false}
      |> then(fn opts -> if nats_token, do: Map.put(opts, :token, nats_token), else: opts end)

    {:ok, conn} = Gnat.start_link(connection_opts)

    # Start a subscriber on each peer. All subscribe to the same subject so one
    # publish fans out to every peer.
    Enum.with_index(peers, fn {pid, _, node}, idx ->
      response_subject = "realtime.fanout.response.#{idx}"

      setup =
        quote do
          connection_opts = unquote(Macro.escape(connection_opts))
          response_subject = unquote(response_subject)

          spawn(fn ->
            {:ok, peer_conn} = Gnat.start_link(connection_opts)
            {:ok, _} = Gnat.sub(peer_conn, self(), "realtime.fanout")

            loop = fn loop, peer_conn ->
              receive do
                {:msg, %{topic: "realtime.fanout"}} ->
                  :ok = Gnat.pub(peer_conn, response_subject, "pong")
                  loop.(loop, peer_conn)
              after
                5000 -> loop.(loop, peer_conn)
              end
            end

            loop.(loop, peer_conn)
          end)
        end

      {_, _} = :peer.call(pid, Code, :eval_quoted, [setup])
      response_subject
    end)

    response_subjects =
      for idx <- 0..(@node_count - 1), do: "realtime.fanout.response.#{idx}"

    # Main node subscribes to all response subjects.
    for subject <- response_subjects do
      {:ok, _} = Gnat.sub(conn, self(), subject)
    end

    Process.sleep(500)

    times =
      for _ <- 1..@rounds do
        {t, _} =
          :timer.tc(fn ->
            :ok = Gnat.pub(conn, "realtime.fanout", "ping")

            for _ <- 1..@node_count do
              receive do
                {:msg, %{topic: "realtime.fanout.response." <> _}} -> :ok
              after
                5000 -> raise "NATS fan-out timeout"
              end
            end
          end)

        t
      end

    avg_us = Enum.sum(times) / length(times)
    avg_ms = avg_us / 1000

    IO.puts("\nNATS fan-out:")
    IO.puts("  rounds:    #{@rounds}")
    IO.puts("  average:   #{Float.round(avg_ms, 2)} ms")
    IO.puts("  per node:  #{Float.round(avg_ms / @node_count, 3)} ms")
    avg_ms
  end
end

Bench.Fanout.run()
