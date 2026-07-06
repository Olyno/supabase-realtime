defmodule Realtime.Broker do
  @moduledoc """
  Behaviour for a message broker abstraction used to decouple database write load
  from socket broadcast load.

  Implementations are expected to provide at least publish/subscribe semantics
  scoped by tenant. The default path keeps the existing Phoenix.PubSub + gen_rpc
  stack; the NATS implementation can be enabled via configuration to offload
  inter-node broadcast.
  """

  @type topic :: String.t()
  @type message :: term()
  @type opts :: keyword()

  @callback child_spec(opts()) :: Supervisor.child_spec()
  @callback publish(topic(), message(), opts()) :: :ok | {:error, term()}
  @callback subscribe(topic(), opts()) :: :ok | {:error, term()}
  @callback unsubscribe(topic()) :: :ok
end
