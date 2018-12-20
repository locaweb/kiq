defmodule Kiq.Batch do
  @moduledoc false

  alias Kiq.{Job, Timestamp, Util}

  @type t :: %__MODULE__{
          bid: binary(),
          callbacks: %{complete: list(map()), success: list(map())},
          created_at: Timestamp.t(),
          description: binary(),
          jobs: list(Job.t()),
          parent_bid: binary(),
          queue: binary()
        }

  @type event :: :complete | :success
  @type meta :: map() | Keyword.t()

  @enforce_keys [:bid, :created_at]
  defstruct [
    :bid,
    :created_at,
    :description,
    :parent_bid,
    :queue,
    callbacks: %{complete: [], success: []},
    jobs: []
  ]

  @doc """
  Create a new batch struct to gather and coordinate jobs.

      iex> Kiq.Batch.new(description: "Coordinated Jobs") |> Map.take([:description])
      %{description: "Coordinated Jobs"}
  """
  @spec new(args :: map() | Keyword.t()) :: t()
  def new(args \\ %{}) when is_map(args) or is_list(args) do
    args =
      args
      |> Enum.into(%{})
      |> Map.put_new(:bid, Util.random_id())
      |> Map.put_new(:created_at, Timestamp.unix_now())

    struct!(__MODULE__, args)
  end

  @doc """
  Add a new job to the batch. When the batch is enqueued for processing each
  job is also enqueued.

  Jobs in the batch _do not_ need to be from the same worker or even the same
  queue.

  ## Example

      Kiq.Batch.new()
      |> Kiq.Batch.add_job(WorkerA.new([1])
      |> Kiq.Batch.add_job(WorkerB.new([2])
      |> Kiq.Batch.add_job(WorkerC.new([3])
  """
  @spec add_job(batch :: t(), job :: Job.t()) :: t()
  def add_job(%__MODULE__{jobs: jobs} = batch, %Job{} = job) do
    %{batch | jobs: [job | jobs]}
  end

  @doc """
  Add a callback module and optional metadata to the batch.

  When the batch has finished running it will enqueue a separate job for every
  callback provided. Callbacks may be attached to two different types of event:

  * `:complete` — Triggered when the batch has finished processing, regardless
    of whether every job was successful.
  * `:success` — Triggered when the batch has finished and all of the jobs ran
  successfully.

  The provided module _must_ export the correct handler functions and the meta
  _must_ be an enumerable. The meta value will always be converted into a map.

  ## Example

      def MyHandler do
        def handle_complete(status, meta) do
          # Work with the status and meta
        end

        def handle_success(status, meta) do
          # Work with the status and meta
        end
      end

      Kiq.Batch.new()
      |> Kiq.Batch.add_callback(:complete, MyHandler, user_id: 1)
      |> Kiq.Batch.add_callback(:success, MyHandler, special_value: "OK")
  """
  @spec add_callback(batch :: t(), event :: event(), module :: module(), meta :: meta()) :: t()
  def add_callback(%__MODULE__{callbacks: callbacks} = batch, event, module, meta \\ [])
      when event in [:complete, :success] and is_atom(module) do
    mapping = %{module => Enum.into(meta, %{})}

    %{batch | callbacks: Map.update(callbacks, event, [mapping], &[mapping | &1])}
  end
end
