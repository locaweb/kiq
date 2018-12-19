defmodule Kiq.Naming do
  @moduledoc false

  @spec backup_name(binary(), binary()) :: binary()
  def backup_name(id, queue), do: "queue:backup|#{id}|#{queue}"

  @spec queue_name(binary()) :: binary()
  def queue_name(queue), do: "queue:#{queue}"

  @spec unlock_name(binary()) :: binary()
  def unlock_name(token), do: "unique:#{token}"

  # Batches

  @spec batch_name(binary()) :: binary()
  def batch_name(bid), do: "b-#{bid}"

  @spec batch_jobs_name(binary()) :: binary()
  def batch_jobs_name(bid), do: "b-#{bid}-jobs"

  @spec batch_pubsub_name(binary()) :: binary()
  def batch_pubsub_name(bid), do: "batch-#{bid}"

  @spec batch_fail_name(binary()) :: binary()
  def batch_fail_name(bid), do: "batch-#{bid}-failinfo"
end
