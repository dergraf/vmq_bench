-module(vmq_bench_con_sup).

-behaviour(supervisor).

%% API functions
-export([start_link/0,
         start_consumers/1,
         start_consumers/3,
         start_consumer/4]).

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
    spawn(
      fun() ->
              Nodes = proplists:get_value(nodes, Config),
              MaxConcurrency = proplists:get_value(max_concurrency, Config, 1),
              Sleep = proplists:get_value(setup_every, Config, 10),
              case Nodes of
                  undefined ->
                      start_consumers(MaxConcurrency, Sleep, Config);
                  _ ->
                      io:format("--- spawn consumer on remote nodes ~p~n", [Nodes]),
                      [rpc:cast(Node, ?MODULE, start_consumers, [MaxConcurrency, Sleep, Config]) || Node <- Nodes]
              end
      end),
    start_consumers(Rest);
start_consumers([]) -> ok.

start_consumers(MaxConcurrency, Sleep, Config) ->
    Topics = proplists:get_value(topics, Config, [{"/test/topic", 0}]),
    start_consumer(MaxConcurrency, Sleep, Topics, lists:keydelete(topics, 1, Config)).

start_consumer(0, _, _, _) -> ok;
start_consumer(N, Sleep, [T|Topics], Config) ->
    {ok, _} = supervisor:start_child(?MODULE, [N, [{topic, T}|Config]]),
    timer:sleep(Sleep),
    start_consumer(N - 1, Sleep, Topics ++ [T], Config).


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

