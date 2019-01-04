defmodule Kiq.Batch.Status do
  @moduledoc """
  Combines a summary of the batch's execution status and information for
  enqueueing batch callbacks.
  """

  @type t :: %__MODULE__{
          bid: binary(),
          callbacks: map(),
          failures: non_neg_integer(),
          pending: non_neg_integer(),
          queue: binary(),
          total: non_neg_integer()
        }

  defstruct [:bid, :queue, :callbacks, failures: 0, pending: 0, total: 0]

  @doc """
  Check whether a batch is considered complete.

  When the number of pending jobs is equal to the number of failed jobs then
  the batch is complete.
  """
  @spec complete?(status :: t()) :: boolean()
  def complete?(%__MODULE__{pending: pending, failures: failures}), do: pending == failures

  @doc """
  Check whether a batch is considered successful.

  When there aren't any more pending jobs then the batch is successful.
  """
  @spec success?(status :: t()) :: boolean()
  def success?(%__MODULE__{pending: pending}), do: pending == 0
end
