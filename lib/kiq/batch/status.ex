defmodule Kiq.Batch.Status do
  @moduledoc false

  @type t :: %__MODULE__{
          bid: binary(),
          callbacks: map(),
          failures: non_neg_integer(),
          pending: non_neg_integer(),
          queue: binary(),
          total: non_neg_integer()
        }

  defstruct [:bid, :queue, :callbacks, failures: 0, pending: 0, total: 0]

  @spec complete?(status :: t()) :: boolean()
  def complete?(%__MODULE__{pending: pending, failures: failures}), do: pending == failures

  @spec success?(status :: t()) :: boolean()
  def success?(%__MODULE__{pending: pending}), do: pending == 0
end
