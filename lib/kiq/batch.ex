defmodule Kiq.Batch do
  @moduledoc ~S"""
  Jobs are organized into batches so that execution progress can be tracked
  as a group.

  Batches may also have optional callbacks that are executed when all jobs are
  finished. Additionally, batches may be nested, which enables modeling
  multi-stage heirarchical workflows, i.e. a batch has multiple sub-batches and
  each of those batches themselves have sub-batches.

  ## Creating Simple Batches

  When users sign up your application coordinates with several external
  services. For the sake of isolation each interaction is wrapped in a separate
  worker. After all of the interactions are complete you want to notify the
  user that their information is ready. We can wrap all of the workers and a
  callback together in a single batch:

      Kiq.Batch.new(description: "User Signup: #{new_user_id}")
      |> Kiq.Batch.add_job(MyApp.OnboardWorker.new([new_user_id]))
      |> Kiq.Batch.add_job(MyApp.ProfileWorker.new([new_user_id]))
      |> Kiq.Batch.add_job(MyApp.AnalyzeWorker.new([new_user_id]))
      |> Kiq.Batch.add_callback(:success, MyApp.OnboardCallbacks, user_id: new_user_id)

  First we create a new batch using `Kiq.Batch.new/1`, providing an optional
  description that will be shown as the batch's label in the UI. Next we use
  `Kiq.Batch.add_job/2` to add jobs for our various workers, piping each call
  through our batch. Finally we add a single success callback using
  `Kiq.Batch.add_callback/4`, providing the module where our `success` callback
  is defined and some optional metadata.

  When all of the jobs have completed successfully the module's
  `handle_success/2` callback will be called. Here is an example of a success
  callback that simply logs out that onboarding is finished:

      defmodule MyApp.OnboardCallbacks do
        @behaviour Kiq.Batch.Callback

        @impl true
        def handle_success(status, %{"user_id" => user_id}) do
          Logger.info("Onboarding Finished for User #{user_id}")
        end
      end

  ### Callback Notes

  Callbacks are executed by `Kiq.Batch.CallbackWorker` just like any other job.
  That means that callbacks are enqueued will be retried if there are any
  errors.  Normally this is helpful and provides additional reliability, but it
  means that callbacks aren't guaranteed to run immediately after the batch's
  jobs finish.

  Addtionally, in a hybrid Kiq/Sidekiq setup be sure that callbacks are
  executed in the correct queue. For example, if Sidekiq is executing jobs in
  the `default` and Kiq is executing jobs in the `primary` queue you must
  ensure that Kiq batch callbacks are pushed into the `primary` queue.
  Sidekiq's callback specifications aren't compatible with Kiq's, and
  visa-versa.
  """

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

  ## Options

  Most attributes of a batch are auto-generated or modified through batch
  functions. However, the following options may be supplied:

    * `description` — An optional free form description of what the batch
    * `queue` — The queue where callback jobs will be executed, defaults to "default".

  ## Examples

      iex> Kiq.Batch.new(description: "Coordinated Jobs") |> Map.take([:description])
      %{description: "Coordinated Jobs"}

      iex> Kiq.Batch.new(queue: "callback-queue") |> Map.take([:queue])
      %{queue: "callback-queue"}
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
