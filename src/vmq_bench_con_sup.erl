-module(vmq_bench_con_sup).

-behaviour(supervisor).

%% API functions
-export([start_link/0,
         start_consumers/1]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_consumers([Config|Rest]) ->
    MaxConcurrency = proplists:get_value(max_concurrency, Config, 1),
    start_consumer(MaxConcurrency, Config),
    start_consumers(Rest);
start_consumers([]) -> ok.

start_consumer(0, _) -> ok;
start_consumer(N, Config) ->
    {ok, _} = supervisor:start_child(?MODULE, [Config]),
    timer:sleep(10),
    start_consumer(N - 1, Config).


%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    {ok, {{simple_one_for_one, 5, 10},
          [{vmq_bench_con, {vmq_bench_con, start_link, []},
                            permanent, 5000, worker, [vmq_bench_con]}]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
