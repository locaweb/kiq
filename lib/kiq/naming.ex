defmodule Kiq.Naming do
  @moduledoc false

  @spec backup_name(binary(), binary()) :: binary()
  def backup_name(id, queue), do: "queue:backup|#{id}|#{queue}"

  @spec batch_name(binary()) :: binary()
  def batch_name(batch_id), do: "b-#{batch_id}"

  @spec batch_jobs_name(binary()) :: binary()
  def batch_jobs_name(batch_id), do: "b-#{batch_id}-jobs"

  @spec queue_name(binary()) :: binary()
  def queue_name(queue), do: "queue:#{queue}"

  @spec unlock_name(binary()) :: binary()
  def unlock_name(token), do: "unique:#{token}"
end
