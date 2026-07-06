defmodule Realtime.GenRpc do
  @moduledoc """
  RPC module for Realtime using Erlang `:erpc`.
  """

  use Realtime.Logs

  alias Realtime.Telemetry

  @type result :: any | {:error, :rpc_error, reason :: any}

  @doc """
  Broadcasts the message `msg` asynchronously to the registered process `name` on the specified `nodes`.
  """
  @spec abcast([node], atom, any, keyword()) :: :ok
  def abcast(nodes, name, msg, _opts) when is_list(nodes) and is_atom(name) do
    Enum.each(nodes, fn target_node ->
      if target_node == node() or target_node in Node.list() do
        :erpc.cast(target_node, Kernel, :send, [{name, target_node}, msg])
      end
    end)

    :ok
  end

  @doc """
  Fire and forget apply(mod, func, args) on one node.
  """
  @spec cast(node, module, atom, list(any), keyword()) :: :ok
  def cast(node, mod, func, args, _opts \\ [])
      when is_atom(node) and is_atom(mod) and is_atom(func) and is_list(args) do
    if node == node() or node in Node.list() do
      :erpc.cast(node, mod, func, args)
    end

    :ok
  end

  @doc """
  Fire and forget apply(mod, func, args) on all nodes.
  """
  @spec multicast(module, atom, list(any), keyword()) :: :ok
  def multicast(mod, func, args, _opts \\ []) when is_atom(mod) and is_atom(func) and is_list(args) do
    [node() | Node.list()]
    |> Enum.each(fn target_node -> :erpc.cast(target_node, mod, func, args) end)

    :ok
  end

  @doc """
  Calls node to apply(mod, func, args).

  Options:

  - `:tenant_id` - Tenant ID for logging, defaults to nil
  - `:timeout` - timeout in milliseconds for the RPC call, defaults to 5000ms
  """
  @spec call(node, module, atom, list(any), keyword()) :: result
  def call(node, mod, func, args, opts)
      when is_atom(node) and is_atom(mod) and is_atom(func) and is_list(args) and is_list(opts) do
    if node == node() or node in Node.list() do
      do_call(node, mod, func, args, opts)
    else
      log_rpc_error(node, mod, func, :badnode, opts)
      {:error, :rpc_error, :badnode}
    end
  end

  @doc """
  Evaluates apply(mod, func, args) on all nodes.

  Options:

  - `:timeout` - timeout for the RPC call, defaults to 5000ms
  - `:tenant_id` - tenant ID for telemetry and logging, defaults to nil
  """
  @spec multicall(module, atom, list(any), keyword()) :: [{node, result}]
  def multicall(mod, func, args, opts \\ []) when is_atom(mod) and is_atom(func) and is_list(args) and is_list(opts) do
    timeout = Keyword.get(opts, :timeout, default_rpc_timeout())
    nodes = [node() | Node.list()]

    nodes
    |> Enum.map(fn target_node ->
      {latency, response} = :timer.tc(fn -> safe_erpc_call(target_node, mod, func, args, timeout) end)
      {target_node, latency, response}
    end)
    |> Enum.map(fn {target_node, latency, response} ->
      handle_response(target_node, mod, func, response, latency, opts)
    end)
  end

  defp do_call(node, mod, func, args, opts) do
    timeout = Keyword.get(opts, :timeout, default_rpc_timeout())

    {latency, response} = :timer.tc(fn -> safe_erpc_call(node, mod, func, args, timeout) end)

    case handle_response(node, mod, func, response, latency, opts) do
      {_node, result} -> result
      result -> result
    end
  end

  defp handle_response(node, _mod, _func, {:ok, {:error, _reason} = result}, latency, _opts) do
    telemetry_failure(node, latency)
    {node, result}
  end

  defp handle_response(node, _mod, _func, {:ok, result}, latency, _opts) do
    telemetry_success(node, latency)
    {node, result}
  end

  defp handle_response(node, mod, func, {:error, reason}, latency, opts) do
    reason = normalize_erpc_error(reason)

    log_rpc_error(node, mod, func, reason, opts)
    telemetry_failure(node, latency)

    {node, {:error, :rpc_error, reason}}
  end

  defp safe_erpc_call(node, mod, func, args, timeout) do
    {:ok, :erpc.call(node, mod, func, args, timeout)}
  catch
    _kind, reason -> {:error, reason}
  end

  defp normalize_erpc_error({:erpc, reason}), do: reason
  defp normalize_erpc_error({:exception, exception, _stack}), do: exception
  defp normalize_erpc_error(reason), do: reason

  defp log_rpc_error(node, mod, func, reason, opts) do
    tenant_id = Keyword.get(opts, :tenant_id)

    log_error(
      "ErrorOnRpcCall",
      %{target: node, mod: mod, func: func, error: reason},
      project: tenant_id,
      external_id: tenant_id
    )
  end

  defp telemetry_success(node, latency) do
    Telemetry.execute(
      [:realtime, :rpc],
      %{latency: latency},
      %{origin_node: node(), target_node: node, success: true, mechanism: :erpc}
    )
  end

  defp telemetry_failure(node, latency) do
    Telemetry.execute(
      [:realtime, :rpc],
      %{latency: latency},
      %{origin_node: node(), target_node: node, success: false, mechanism: :erpc}
    )
  end

  defp default_rpc_timeout, do: Application.get_env(:realtime, :rpc_timeout, 5_000)
end
