defmodule Kiq.Client.Batching do
  @moduledoc false

  import Redix
  import Kiq.Naming

  alias Kiq.{Batch, Util}
  alias Kiq.Batch.Status
  alias Kiq.Client.{Locking, Queueing}

  @type conn :: GenServer.server()

  @one_month 60 * 60 * 24 * 30

  @spec enqueue(conn(), list(Batch.t()) | Batch.t()) :: :ok
  def enqueue(conn, batches) when is_list(batches) do
    for batch <- batches, do: enqueue(conn, batch)

    :ok
  end

  def enqueue(_conn, %Batch{jobs: []}), do: :ok

  def enqueue(conn, %Batch{jobs: [_ | _]} = batch) do
    commands =
      []
      |> with_batch_commands(batch)
      |> with_parent_commands(batch)
      |> with_job_commands(batch)

    noreply_pipeline!(conn, commands)
  end

  @spec add_success(conn(), binary(), binary()) :: Status.t()
  def add_success(conn, bid, jid) when is_binary(bid) and is_binary(jid) do
    commands = [
      ["HINCRBY", batch_name(bid), "pending", "-1"],
      ["HDEL", batch_fail_name(bid), jid],
      ["HLEN", batch_fail_name(bid)],
      ["SREM", batch_jobs_name(bid), jid],
      ["HINCRBY", batch_name(bid), "total", "0"],
      ["HGET", batch_name(bid), "cbq"],
      ["HGET", batch_name(bid), "callbacks"]
    ]

    [pending, _, failures, _, total, queue, callbacks] = pipeline!(conn, commands)

    %Status{
      bid: bid,
      callbacks: Jason.decode!(callbacks),
      pending: pending,
      failures: failures,
      queue: queue,
      total: total
    }
  end

  @spec add_failure(conn(), binary(), binary(), Exception.t()) :: Status.t()
  def add_failure(conn, bid, jid, error) when is_binary(bid) and is_binary(jid) do
    error_info = Jason.encode!([Util.error_name(error), Exception.message(error)])

    commands = [
      ["HSET", batch_fail_name(bid), jid, error_info],
      ["EXPIRE", batch_fail_name(bid), to_string(@one_month)],
      ["HINCRBY", batch_name(bid), "pending", "0"],
      ["HLEN", batch_fail_name(bid)],
      ["HINCRBY", batch_name(bid), "total", "0"],
      ["HGET", batch_name(bid), "cbq"],
      ["HGET", batch_name(bid), "callbacks"]
    ]

    [_, _, pending, failures, total, queue, callbacks] = pipeline!(conn, commands)

    %Status{
      bid: bid,
      callbacks: Jason.decode!(callbacks),
      pending: pending,
      failures: failures,
      queue: queue,
      total: total
    }
  end

  @spec locked?(conn(), binary(), :complete | :success) :: boolean()
  def locked?(conn, bid, event) when is_binary(bid) and is_atom(event) do
    Locking.locked?(conn, batch_lock_name(bid, event), bid, @one_month)
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
      ["EXPIRE", key, to_string(@one_month)],
      ["SADD", jobs_key, job_ids],
      ["EXPIRE", jobs_key, to_string(@one_month)]
    ]

    commands ++ batch_commands
  end

  defp with_parent_commands(commands, %Batch{parent_bid: nil}) do
    commands
  end

  defp with_parent_commands(commands, %Batch{parent_bid: parent_bid}) do
    parent_commands = [
      ["HINCRBY", "b-#{parent_bid}", "kids", "1"],
      ["EXPIRE", "b-#{parent_bid}", to_string(@one_month)]
    ]

    commands ++ parent_commands
  end

  # The `enqueue_command/1` function composes commands using the enqueue script
  # in lua. The script ensures that enqueueing a job in or out of a batch
  # behaves identically, i.e. unique locks are respected and jobs are scheduled.
  defp with_job_commands(commands, %Batch{bid: bid, jobs: jobs}) do
    job_commands = for job <- jobs, do: Queueing.enqueue_command(%{job | bid: bid})

    commands ++ job_commands
  end
end
