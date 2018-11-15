defmodule Kiq.JobTest do
  use Kiq.Case, async: true

  alias Kiq.{Job, Timestamp}

  doctest Job

  describe "decode/1" do
    test "all object keys are atomized" do
      job = Job.decode(~s({"class":"MyWorker","args":{"a":1,"b":2}}))

      assert job.class == "MyWorker"
      assert job.args == %{a: 1, b: 2}
    end
  end

  describe "encode/1" do
    test "transient and nil values are omitted" do
      decoded =
        [pid: self(), args: [1, 2], queue: "default"]
        |> job()
        |> Job.encode()
        |> Job.decode()

      assert decoded.queue == "default"
      assert decoded.args == [1, 2]
      assert decoded.jid
      assert decoded.class
      refute decoded.pid
      refute decoded.failed_at
    end

    test "retry_count values are only retained when greater than 0" do
      refute encode(retry_count: 0) =~ "retry_count"
      assert encode(retry_count: 1) =~ "retry_count"
    end
  end

  describe "apply_unique/1" do
    property "job with any args can generate a valid unique_token" do
      check all class <- binary(min_length: 1),
                queue <- binary(min_length: 1),
                args <- list_of(one_of([boolean(), integer(), binary()])) do
        job = job(args: args, class: class, queue: queue, unique_for: 100)

        assert Job.apply_unique(job).unique_token =~ ~r/\A[a-z0-9]{40}\z/
      end
    end

    test "jobs with a unique_for value have a future unique_at date applied" do
      job = job(unique_for: 10)

      assert Job.apply_unique(job).unlocks_at > Timestamp.unix_now()
    end
  end

  describe "apply_expiry/1" do
    test "jobs with an expires_in property have a future expires_at date applied" do
      job = job(expires_in: 10)

      assert Job.apply_expiry(job).expires_at > Timestamp.unix_now()
    end
  end

  defp encode(args) do
    args
    |> job()
    |> Job.encode()
  end
end
