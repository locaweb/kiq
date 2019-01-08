defmodule Kiq.Worker do
  @moduledoc """
  Defines a behavior and macro to guide the creation of worker modules.

  Worker modules do the work of processing a job. At a minimum they must define
  a `perform` function, which will be called with the arguments that were
  enqueued with the `Kiq.Job`.

  ## Defining Workers

  Define a worker to process jobs in the `events` queue:

      defmodule MyApp.Workers.Business do
        use Kiq.Worker, queue: "events", retry: 10, dead: false

        @impl Kiq.Worker
        def perform(args) do
          IO.inspect(args)
        end
      end

  The `perform/1` function will always receive a list of arguments. In this
  example the worker will simply inspect any arguments that are provided.

  ## Enqueuing Jobs

  All workers implement a `new/1` function that converts a list of arguments
  into a `Kiq.Job` that is suitable for enqueuing:

      ["doing", "business"]
      |> MyApp.Workers.Business.new()
      |> MyApp.Kiq.enqueue()
  """

  alias Kiq.Job

  @type args :: list(any())
  @type opts :: [
          queue: binary(),
          dead: boolean(),
          expires_in: pos_integer(),
          retry: boolean(),
          unique_for: pos_integer(),
          unique_until: binary()
        ]

  @doc """
  Build a job for this worker using all default options.

  Any additional arguments that are provided will be merged into the job.
  """
  @callback new(args :: args()) :: Job.t()

  @doc """
  The `perform/1` function is called with the enqueued arguments.

  The return value is not important.
  """
  @callback perform(args :: args()) :: any()

  @allowed_opts [:queue, :dead, :expires_in, :retry, :unique_for, :unique_until]

  defmacro __using__(opts) do
    opts =
      opts
      |> Keyword.take(@allowed_opts)
      |> Keyword.put_new(:queue, "default")

    quote do
      alias Kiq.Worker

      @behaviour Worker

      @impl Worker
      def new(args) when is_list(args) do
        Worker.new(__MODULE__, args, unquote(opts))
      end

      @impl Worker
      def perform(args) when is_list(args) do
        :ok
      end

      defoverridable Worker
    end
  end

  @doc false
  @spec new(module(), args(), opts()) :: Job.t()
  def new(module, args, opts) do
    opts
    |> Keyword.put(:args, args)
    |> Keyword.put(:class, module)
    |> Job.new()
  end
end
