defmodule Kiq.Client.Batching do
  @moduledoc false

  import Redix, only: [noreply_pipeline: 2]
  import Kiq.Naming, only: [batch_name: 1, batch_jobs_name: 1, queue_name: 1]

  alias Kiq.{Batch, Job}

  @type conn :: GenServer.server()

  @spec enqueue(conn(), Batch.t()) :: {:ok, Batch.t()}
  def enqueue(conn, %Batch{jobs: [_ | _]} = batch) do
    commands =
      []
      |> with_batch_commands(batch)
      |> with_parent_commands(batch)
      |> with_job_commands(batch)

    with :ok <- noreply_pipeline(conn, commands) do
      {:ok, batch}
    end
  end

  defp with_batch_commands(commands, %Batch{bid: bid} = batch) do
    key = batch_name(bid)
    jobs_count = length(batch.jobs)
    job_ids = Enum.map(batch.jobs, & &1.id)
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

    # TODO: Set the bid on the job

    job_commands = Enum.map(jobs, fn %Job{queue: queue} = job ->
      job = %{job | enqueued_at: Timestamp.unix_now()}

      ["LPUSH", queue_name(queue), Job.encode(job)]
    end)

    commands ++ ["SADD" | ["queues" | queues]] ++ job_commands
  end
end
