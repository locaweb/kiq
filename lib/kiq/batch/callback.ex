defmodule Kiq.Batch.Callback do
  @moduledoc """
  Behaviour specifying batch callback functions.

  All callbacks will be passed a `Kiq.Batch.Status` struct and a map of user
  provided arguments. The callback's return value doesn't matter, but if an
  exception is raised then the callback will be retried.
  """

  alias Kiq.Batch.Status

  @doc """
  Called when all jobs in a batch have completed successfully.
  """
  @callback handle_success(status :: Status.t(), args :: map()) :: any()

  @doc """
  Called when all jobs in a batch have been executed at least once, regardless
  of whether they were successful or not.
  """
  @callback handle_complete(status :: Status.t(), args :: map()) :: any()
end
