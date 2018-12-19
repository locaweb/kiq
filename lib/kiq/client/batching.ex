defmodule Kiq.Client.Batching do
  @moduledoc false

  import Redix
  import Kiq.Naming

  alias Kiq.{Batch, Job, Timestamp}
  alias Kiq.Batch.Status

  @type conn :: GenServer.server()

  @spec enqueue(conn(), list(Batch.t()) | Batch.t()) :: :ok
  def enqueue(conn, batches) when is_list(batches) do
    for batch <- batches, do: enqueue(conn, batch)

    :ok
  end

  def enqueue(conn, %Batch{jobs: [_ | _]} = batch) do
    commands =
      []
      |> with_batch_commands(batch)
      |> with_parent_commands(batch)
      |> with_job_commands(batch)

    noreply_pipeline!(conn, commands)
  end

  @spec add_success(conn(), bid :: binary(), jid :: binary()) :: Status.t()
  def add_success(conn, bid, jid) when is_binary(bid) and is_binary(jid) do
    commands = [
      ["PUBLISH", batch_pubsub_name(bid), "+"],
      ["HINCRBY", batch_name(bid), "pending", -1],
      ["HDEL", batch_fail_name(bid), jid],
      ["HLEN", batch_fail_name(bid)],
      ["SREM", batch_jobs_name(bid), jid],
      ["HINCRBY", batch_name(bid), "total", 0],
      ["HGET", batch_name(bid), "cbq"],
      ["HGET", batch_name(bid), "callbacks"]
    ]

    [_, pending, _, failures, _, total, queue, callbacks] = pipeline!(conn, commands)

    %Status{
      bid: bid,
      callbacks: Jason.decode!(callbacks),
      pending: pending,
      failures: failures,
      queue: queue,
      total: total
    }
  end

  # Helpers

  defp with_batch_commands(commands, %Batch{bid: bid} = batch) do
    key = batch_name(bid)
    jobs_count = length(batch.jobs)
    job_ids = Enum.map(batch.jobs, & &1.jid)
    jobs_key = batch_jobs_name(bid)
    encoded_callbacks = Jason.encode!(batch.callbacks)

    batch_commands = [
      ["HMSET", key, "created_at", batch.created_at, "callbacks", encoded_callbacks],
      ["HMSET", key, "description", batch.description, "parent", batch.parent_bid],
      ["HMSET", key, "cbq", batch.queue, "pending", jobs_count, "total", jobs_count],
      ["EXPIRE", key, batch.expires_in],
      ["SADD", jobs_key, job_ids],
      ["EXPIRE", jobs_key, batch.expires_in]
    ]

    commands ++ batch_commands
  end

  defp with_parent_commands(commands, %Batch{parent_bid: nil}) do
    commands
  end

  defp with_parent_commands(commands, %Batch{parent_bid: parent_bid, expires_in: expires_in}) do
    parent_commands = [
      ["HINCRBY", "b-#{parent_bid}", "kids", "1"],
      ["EXPIRE", "b-#{parent_bid}", expires_in]
    ]

    commands ++ parent_commands
  end

  defp with_job_commands(commands, %Batch{bid: bid, jobs: jobs}) do
    queues =
      jobs
      |> Enum.map(& &1.queue)
      |> Enum.uniq()

    job_commands =
      Enum.map(jobs, fn %Job{queue: queue} = job ->
        job = %{job | bid: bid, enqueued_at: Timestamp.unix_now()}

        ["LPUSH", queue_name(queue), Job.encode(job)]
      end)

    commands ++ [["SADD" | ["queues" | queues]]] ++ job_commands
  end
end
