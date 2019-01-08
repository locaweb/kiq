defmodule Kiq do
  @moduledoc ~S"""
  Kiq is a robust and extensible job processing queue that aims for
  compatibility with Sidekiq Enterprise.

  Job queuing, processing and reporting are all built on GenStage. That means
  maximum parallelism with the safety of backpressure as jobs are processed.

  ## Usage

  Kiq isn't an application that must be started. Similarly to Ecto, you define
  one or more Kiq modules within your application. This allows multiple
  supervision trees with entirely different configurations.

  Run the generator to create a new `Kiq` supervisor for your application:

      mix kiq.gen.supervisor -m MyApp.Kiq

  Alternatively, manually define a `Kiq` module for your application:

      defmodule MyApp.Kiq do
        use Kiq, queues: [default: 25, events: 50]
      end

  Include the module in your application's supervision tree:

      defmodule MyApp.Application do
        @moduledoc false

        use Application

        alias MyApp.{Endpoint, Kiq, Repo}

        def start(_type, _args) do
          children = [
            {Repo, []},
            {Endpoint, []},
            {Kiq, []}
          ]

          Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
        end
      end

  ## Configuration

  Kiq is used to start one or more supervision trees in your application. That
  means there isn't a central Kiq "app" to configure, instead each supervision
  tree may be configured independently.

  Configuration options pass through a couple of locations, accumulating
  overrides until finally passing through the `init/2` callback. Options are
  accumulated in this order:

  1. Options passed into via the `use` macro. These should be constant compile
     time options, i.e. `extra_reporters`.

  2. Options passed to `start_link/1` by the application's supervision tree.
     These should also be values suitable for compile time.

  3. Injected by the `init/2` callback inside your application's Kiq instance.
     This is where runtime configuration such as the Redis URL or environment
     specific options should be passed. The options can come from any dynamic
     source such as Mix Config, Vault, Etcd, environment variables, etc.

  The default `init/2` implementation uses `System.get_env/1` to read the
  `REDIS_URL` on boot. The default callback can be overridden to pull in
  additional configuration. For example, to grab values from
  `Application.get_env/2`:

      def init(_reason, opts) do
        for_env = Application.get_env(:my_app, :kiq, [])

        opts =
          opts
          |> Keyword.merge(for_env)
          |> Keyword.put(:client_opts, [redis_url: System.get_env("REDIS_URL")])

        {:ok, opts}
      end

  The `opts` argument contains all configuration from stages 1 and 2 (the `use`
  macro and the call to `start_link/1`).

  ### Supervisor Configuration

  These configuration options must be provided to the supervision tree on startup:

  * `:client_opts` — A keyword list of options passed to each `Redix`
    connection.  This *must* contain the key `:redis_url`, which is passed to
    `Redix.start_link/1` as the first argument.

  * `:dead_limit` — The maximum number of jobs that will be retained in the dead
    set. Jobs beyond the limit are pruned any time a new job is moved to the dead
    set. The default is `10_000`.

  * `:dead_timeout` — The maximum amount of time that a job will remain in the
    dead set before being purged, specified in seconds. The default is 6 months.

  * `:extra_reporters` — Additional reporters that your application will use to
    report errors, track external stats, etc.

  * `:fetch_interval` — How frequently to poll for new jobs. Polling only
    happens when consumers aren't actively requesting new jobs.

  * `:flush_interval` - How frequently locally enqueued jobs will be pushed to
    Redis. This defaults to `10ms`, though it will back-off by a factor of `1.5`
    if there are any connection errors.

  * `:periodics` — A list of job scheduling tuples in the form `{schedule,
    worker}` or `{schedule, worker, options}`. See
    [Periodic Jobs](#module-periodic-jobs) for details.

  * `:pool_size` — Controls the number of Redis connections available to Kiq,
    defaults to 5.

  * `:queues` — A keyword list of queues where each entry is the name of the
    queue and the concurrency setting. For example, setting `[default: 10,
    exports: 5, media: 5]` would start the queues `default`, `exports` and
    `media` with a combined concurrency of 20. The concurrency setting
    specifies how many jobs _each queue_ will run concurrently.

  * `:schedulers` — A list of schedulers to run. The default schedulers are
    "retry" which is used to retry failed jobs with a backoff and "schedule",
    used to enqueue jobs at a specific time in the future. Set this to an empty
    list to disable all schedulers and allow Sidekiq to handle enqueuing
    retries and scheduled jobs.

  * `:server?` — Whether to start the queue supervisors and start processing
    jobs or only start the client. This setting is useful for testing or
    deploying your application's web and workers separately.

  * `:test_mode` — Either `:disabled` or `:sandbox`. See
    [Testing](#module-testing) for details.

  ### Runtime Configuration

  These configuration options may be provided when starting a Kiq supervisor,
  but may also be set dynamically at runtime:

  * `:quiet` — Instruct each queue to stop processing new jobs. Any in-progress
  jobs will keep running uninterrupted.

  For example, to quiet all queue producers:

      MyKiq.configure(quiet: true)

  ## Testing

  Kiq has special considerations to facilitate isolated and asynchronous testing.

  For testing Kiq should be configured in `:sandbox` mode and have the server
  disabled. This can be done specifically for the test environment by adding
  config to `config/test.exs`:

      config :my_app, :kiq, server?: false, test_mode: :sandbox

  Running in `:sandbox` mode ensures that enqueued jobs stay in memory and are
  never flushed to Redis. This allows your tests to use `Kiq.Testing` to make
  quick assertions about which jobs have or haven't been enqueued. See the docs
  for `Kiq.Testing` for more details and usage.

  ## Reliable Push

  Reliable push replicates the safety aspects of Sidekiq Pro's [reliability
  client][rely]. To guard against network errors or other Redis issues the
  client buffers all jobs locally. At a frequent interval the jobs are flushed
  to Redis. If there are any errors while flushing the jobs will remain in
  memory until flushing can be retried later.

  ### Caveats

    * The local job buffer is stored in memory, if the server is restarted
      suddently some jobs may be lost.

    * There isn't any limit on the number of jobs that can be buffered.
      However, to conserve space jobs are stored compressed.

  [rely]: https://github.com/mperham/sidekiq/wiki/Pro-Reliability-Client

  ## Private Queues

  Kiq tries to prevent all job loss through private queues, a variant of the
  [Super Fetch][super] mechanism available in Sidekiq Pro. When jobs are
  executed they are backed up to a private queue specific to the server
  processing the job. If the processor crashes or the application is terminated
  before the job is finished the jobs remain backed up. On startup, and
  periodically afterward, jobs in any dead private queues are pushed back to
  the public queue for re-execution.

  This solution is well suited to containerized environments and autoscaling.

  [super]: https://github.com/mperham/sidekiq/wiki/Pro-Reliability-Server#super_fetch

  ## Unique Jobs

  Kiq supports Sidekiq Enterprise's [unique jobs][uniq]. This feature prevents
  enqueueing duplicate jobs while an original job is still pending. The
  operations that attempt to enforce uniqueness are _not_ atomic—uniquess is
  not guaranteed and should be considered best effort.

  Enable unique jobs for a worker by setting a unique time period:

      use Kiq.Worker, unique_for: :timer.minutes(5)

  ### Unlock Policy

  By default unique jobs will hold the unique lock until the job has ran
  successfully. This policy ensures that jobs will remain locked if there are
  any errors, but it is prone to race conditions in certain situations for
  longer running jobs.

  Generally it is best to stick with the default `:success` policy. However, if
  your job is effected by the race condition you can change your worker's
  policy:

      use Kiq.Worker, unique_for: :timer.minutes(60), unique_until: :start

  ### Caveats

    * Note that job uniqueness is calculated from the `class`, `args`, and
      `queue`. This means that jobs with identical args may be added to different
      queues.

    * Unique jobs enqueued by Sidekiq will be unlocked by Kiq, but they may not
      use the same lock value. This is due to differences between hashing Erlang
      terms and Ruby objects. To help ensure uniqueness always enqueue unique jobs
      from either Sidekiq or Kiq.

  [uniq]: https://github.com/mperham/sidekiq/wiki/Ent-Unique-Jobs

  ## Expiring Jobs

  Kiq supports Sidekiq Pro's [expiring jobs][expi]. Expiring jobs won't be ran
  after a configurable amount of time. The expiration period is set with the
  `expires_in` option, which accepts millisecond values identically
  `unique_for`.

  Set the expiration for a worker using a relative time:

      use Kiq.Worker, expires_in: :timer.hours(1)

  Expiration time applies after the scheduled time. That means scheduling a job
  to run in an hour, with an expiration of 30 minutes, will expire in one hour
  and 30 minutes.

  [expi]: https://github.com/mperham/sidekiq/wiki/Pro-Expiring-Jobs

  ## Periodic Jobs

  Kiq supports Sidekiq Enterprise's [Periodic Jobs][peri]. This allows jobs to
  be registered with a schedule and enqueued automatically. Jobs are registered
  as `{crontab, worker}` or `{crontab, worker, options}` using the `:periodics`
  attribute:

      use Kiq, periodics: [
        {"* * * * *", MyApp.MinuteWorker},
        {"0 * * * *", MyApp.HourlyWorker},
        {"0 0 * * *", MyApp.DailyWorker, retry: 1},
      ]

  These jobs would be executed as follows:

    * `MyApp.MinuteWorker` - Executed once every minute
    * `MyApp.HourlyWorker` - Executed at the first minute of every hour
    * `MyApp.DailyWorker` - Executed at midnight every day

  The crontab format, as parsed by `Kiq.Parser.Crontab`, respects all [standard
  rules][cron] and has one minute resolution. That means it isn't possible to
  enqueue a job ever N seconds.

  ### Caveats

    * All schedules are evaluated as UTC, the local timezone is never taken
      into account.

    * Periodic jobs registered in Kiq _aren't_ visible in the Loop panel of the
      Sidekiq Dashboard. This is due to the way loop data is stored by Sidekiq
      and can't be worked around.

    * This is an alternative to using using a separate scheduler such as
      [Quantum][quan]. However, unlike Quantum, Kiq doesn't support node based
      clustering, instead it uses Redis to coordinate and distrubte work. This
      means workers can scale horizontally even in a restricted environment like
      Heroku.

  [peri]: https://github.com/mperham/sidekiq/wiki/Ent-Periodic-Jobs
  [cron]: https://en.wikipedia.org/wiki/Cron#Overview
  [quan]: https://github.com/quantum-elixir/quantum-core

  ## Instrumentation & Metrics

  The instrumentation reporter provides integration with [Telemetry][tele], a
  dispatching library for metrics. It is easy to report Kiq metrics to any
  backend by attaching to `:kiq` events.

  For exmaple, to log out the timing for all successful jobs:

      defmodule KiqJobLogger do
        require Logger

        def handle_event([:kiq, :job, :success], timing, metadata, _config) do
          Logger.info("[#{metadata.queue}] #{metadata.class} finished in #{timing}")
        end
      end

      Telemetry.attach([:kiq, :job, :success], KiqJobLogger, :handle_event, nil)

  Here is a reference for the available metrics:

  | event     | name                     | value  | metadata                  |
  | --------- | ------------------------ | ------ | ------------------------- |
  | `started` | `[:kiq, :job, :started]` | 1      | `:class, :queue`          |
  | `success` | `[:kiq, :job, :success]` | timing | `:class, :queue`          |
  | `aborted` | `[:kiq, :job, :aborted]` | 1      | `:class, :queue, :reason` |
  | `failure` | `[:kiq, :job, :failure]` | 1      | `:class, :queue, :error`  |

  [tele]: https://hexdocs.pm/telemetry
  """

  alias Kiq.{Batch, Client, Job, Timestamp}

  @type enqueue_args :: map() | Keyword.t() | Batch.t() | Job.t()
  @type enqueue_opts :: [in: pos_integer(), at: DateTime.t()]

  @doc """
  Starts the client and possibly the supervision tree, returning `{:ok, pid}` when startup is
  successful.

  Returns `{:error, {:already_started, pid}}` if the tree is already started or `{:error, term}`
  in case anything else goes wrong.

  ## Options

  Any options passed to `start_link` will be merged with those provided in the `use` block.
  """
  @callback start_link(opts :: Keyword.t()) :: Supervisor.on_start()

  @doc """
  A callback executed when the supervision tree is started and possibly when configuration is
  read.

  The first argument is the context of the callback being invoked. In most circumstances this
  will be `:supervisor`. The second argument is a keyword list of the combined options passed
  to `use/1` and `start_link/1`.

  Application configuration is _not_ passed into the `init/2` callback. To use application
  config the callback must be overridden and merged manually.
  """
  @callback init(reason :: :supervisor, opts :: Keyword.t()) :: {:ok, Keyword.t()} | :ignore

  @doc """
  Clear all enqueued, scheduled and backup jobs.

  All known queues are cleared, even if they aren't listed in the current configuration.
  """
  @callback clear() :: :ok

  @doc """
  Set runtime configuration values.

  See the "Runtime Configuration" section in the `Kiq` module documentation.
  """
  @callback configure(Keyword.t()) :: :ok

  @doc """
  Enqueue a job to be processed asynchronously.

  Jobs can be enqueued from `Job` structs, maps or keyword lists.

  ## Options

  * `in` - The amount of time in seconds to wait before processing the job. This must be a
    positive integer.
  * `at` - A specific `DateTime` in the future when the job should be processed.

  ## Examples

      # Enqueue a job to be processed immediately
      MyJob.new([1, 2]) |> MyKiq.enqueue()

      # Enqueue a job in one minute
      MyJob.new([1, 2]) |> MyKiq.enqueue(in: 60)

      # Enqueue a job some time in the future
      MyJob.new([1, 2]) |> MyKiq.enqueue(at: ~D[2020-09-20 12:00:00])

      # Enqueue a job from scratch, without using a worker module
      MyKiq.enqueue(class: "ExternalWorker", args: [1])
  """
  @callback enqueue(enqueue_args(), enqueue_opts()) ::
              {:ok, Batch.t() | Job.t()} | {:error, Exception.t()}

  @doc false
  defmacro __using__(opts) do
    quote do
      @behaviour Kiq

      @client_name Module.concat(__MODULE__, "Client")
      @pool_name Module.concat(__MODULE__, "Pool")
      @registry_name Module.concat(__MODULE__, "Registry")
      @reporter_name Module.concat(__MODULE__, "Reporter")
      @senator_name Module.concat(__MODULE__, "Senator")
      @supervisor_name Module.concat(__MODULE__, "Supervisor")

      @opts unquote(opts)
            |> Keyword.put(:main, __MODULE__)
            |> Keyword.put(:name, @supervisor_name)
            |> Keyword.put(:client_name, @client_name)
            |> Keyword.put(:pool_name, @pool_name)
            |> Keyword.put(:registry_name, @registry_name)
            |> Keyword.put(:reporter_name, @reporter_name)
            |> Keyword.put(:senator_name, @senator_name)

      @doc false
      def child_spec(opts) do
        %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, type: :supervisor}
      end

      @impl Kiq
      def start_link(opts \\ []) do
        @opts
        |> Keyword.merge(opts)
        |> Kiq.Supervisor.start_link()
      end

      @impl Kiq
      def init(reason, opts) when is_atom(reason) and is_list(opts) do
        client_opts = [redis_url: System.get_env("REDIS_URL")]

        {:ok, Keyword.put(opts, :client_opts, client_opts)}
      end

      @impl Kiq
      def clear do
        Kiq.Client.clear(@client_name)
      end

      @impl Kiq
      def configure(opts) when is_list(opts) do
        Kiq.configure(@registry_name, Keyword.take(opts, [:quiet]))
      end

      @impl Kiq
      def enqueue(enqueue_args, enqueue_opts \\ [])
          when is_map(enqueue_args) or is_list(enqueue_args) do
        Kiq.enqueue(@client_name, enqueue_args, enqueue_opts)
      end

      defoverridable Kiq
    end
  end

  @doc false
  def enqueue(client, %Job{} = job, opts) do
    Client.store(client, with_opts(job, opts))
  end

  def enqueue(client, %Batch{} = batch, []) do
    Client.store(client, batch)
  end

  def enqueue(client, args, opts) do
    job =
      args
      |> Job.new()
      |> with_opts(opts)

    Client.store(client, job)
  end

  defp with_opts(job, []), do: job
  defp with_opts(job, at: timestamp), do: %{job | at: timestamp}
  defp with_opts(job, in: seconds), do: %{job | at: Timestamp.unix_in(seconds)}

  @doc false
  def configure(registry, opts) do
    Registry.dispatch(registry, :config, fn entries ->
      for {pid, _} <- entries, do: send(pid, {:configure, opts})
    end)
  end
end
