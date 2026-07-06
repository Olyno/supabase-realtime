defmodule Bench.MultiNode do
  @moduledoc """
  Compare inter-node messaging between :rpc (proxy for gen_rpc) and NATS.

  Scenarios:
  - Roundtrip: publish on main node, peer echoes back, measure end-to-end latency.
  - Fire-and-forget: publish as fast as possible, measure raw send throughput.

  NOTE: This is a local loopback benchmark. Production numbers will differ due to
  real network latency and the fan-out advantages of NATS.
  """

  @roundtrips 100

  def run do
    ensure_net_kernel!()
    {:ok, pid, node} = start_peer()

    IO.puts("Peer node started: #{node}")
    :ok = :peer.call(pid, :code, :add_paths, [:code.get_path()])

    measure_rpc(node)
    measure_nats(pid)

    :peer.stop(pid)
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
    {:ok, pid, node}
  end

  defp measure_rpc(node) do
    true = Node.connect(node)

    # Local drainer for RPC fire-and-forget casts.
    drain_pid =
      spawn(fn ->
        loop = fn loop ->
          receive do
            :ping -> loop.(loop)
          after
            5000 -> loop.(loop)
          end
        end

        loop.(loop)
      end)

    # Roundtrip benchmark (manual loop for reliability).
    {rpc_rt_us, _} =
      :timer.tc(fn ->
        for _ <- 1..@roundtrips do
          :pong = :rpc.call(node, Kernel, :send, [self(), :pong], 5000)

          receive do
            :pong -> :ok
          after
            2000 -> raise "timeout"
          end
        end
      end)

    IO.puts("\nrpc call roundtrip: #{@roundtrips} roundtrips in #{rpc_rt_us} μs")
    IO.puts("  average: #{Float.round(rpc_rt_us / @roundtrips, 2)} μs")
    IO.puts("  ips:     #{Float.round(@roundtrips * 1_000_000 / rpc_rt_us, 2)}")

    # Fire-and-forget benchmark via Benchee.
    Benchee.run(
      %{
        "rpc cast fire-and-forget" => fn ->
          true = :rpc.cast(node, Kernel, :send, [drain_pid, :ping])
          :ok
        end
      },
      warmup: 1,
      time: 5
    )

    Process.exit(drain_pid, :kill)
  end

  defp measure_nats(pid) do
    nats_host = System.get_env("NATS_HOST", "127.0.0.1")
    nats_port = String.to_integer(System.get_env("NATS_PORT", "4222"))
    nats_token = System.get_env("NATS_TOKEN")

    connection_opts =
      %{host: nats_host, port: nats_port, tls: false}
      |> then(fn opts -> if nats_token, do: Map.put(opts, :token, nats_token), else: opts end)

    {:ok, conn} = Gnat.start_link(connection_opts)

    # Evaluate echo loop on the peer node via quoted expression.
    echo_setup =
      quote do
        connection_opts = unquote(Macro.escape(connection_opts))

        echo_loop = fn loop, peer_conn ->
          receive do
            {:msg, %{topic: "realtime.bench.request"}} ->
              :ok = Gnat.pub(peer_conn, "realtime.bench.response", "pong")
              loop.(loop, peer_conn)
          after
            5000 -> loop.(loop, peer_conn)
          end
        end

        spawn(fn ->
          case Gnat.start_link(connection_opts) do
            {:ok, peer_conn} ->
              case Gnat.sub(peer_conn, self(), "realtime.bench.request") do
                {:ok, _} -> echo_loop.(echo_loop, peer_conn)
                error -> IO.inspect(error, label: "peer subscribe failed")
              end

            error ->
              IO.inspect(error, label: "peer NATS connect failed")
          end
        end)

        :ok
      end

    {:ok, _} = :peer.call(pid, Code, :eval_quoted, [echo_setup])
    Process.sleep(500)

    # Subscribe on main node and run roundtrip benchmark manually.
    {:ok, _} = Gnat.sub(conn, self(), "realtime.bench.response")
    Process.sleep(500)

    # Sanity check.
    :ok = Gnat.pub(conn, "realtime.bench.request", "ping")

    receive do
      {:msg, %{topic: "realtime.bench.response"}} ->
        IO.puts("nats sanity check passed")

      other ->
        IO.inspect(other, label: "unexpected nats message")
    after
      2000 -> raise "nats sanity check timeout"
    end

    {nats_rt_us, _} =
      :timer.tc(fn ->
        for _ <- 1..@roundtrips do
          :ok = Gnat.pub(conn, "realtime.bench.request", "ping")

          receive do
            {:msg, %{topic: "realtime.bench.response"}} -> :ok
          after
            2000 -> raise "timeout"
          end
        end
      end)

    IO.puts("\nnats roundtrip: #{@roundtrips} roundtrips in #{nats_rt_us} μs")
    IO.puts("  average: #{Float.round(nats_rt_us / @roundtrips, 2)} μs")
    IO.puts("  ips:     #{Float.round(@roundtrips * 1_000_000 / nats_rt_us, 2)}")

    # Fire-and-forget benchmark via Benchee.
    Benchee.run(
      %{
        "nats fire-and-forget" => fn ->
          :ok = Gnat.pub(conn, "realtime.bench.request", "ping")
        end
      },
      warmup: 1,
      time: 5
    )
  end
end

Bench.MultiNode.run()
