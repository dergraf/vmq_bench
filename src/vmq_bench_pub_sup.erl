-module(vmq_bench_pub_sup).

-behaviour(supervisor).

%% API functions
-export([start_link/0,
         start_publishers/1,
         start_publisher/3]).

%% Supervisor callbacks
-export([init/1]).

-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args},
                                     permanent, 5000, Type, [Mod]}).

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

start_publishers([Config|Rest]) ->
    Nodes = proplists:get_value(nodes, Config),
    MaxConcurrency = proplists:get_value(max_concurrency, Config, 1),
    Topics = proplists:get_value(topics, Config, []),
    case Nodes of
        undefined ->
            start_publisher(MaxConcurrency, Topics, lists:keydelete(topics, 1, Config));
        _ ->
            [rpc:cast(Node, ?MODULE, start_publisher, [MaxConcurrency, Topics, lists:keydelete(topics, 1, Config)])
             || Node <- Nodes]
    end,
    start_publishers(Rest);
start_publishers([]) -> ok.

start_publisher(0, _, _) -> ok;
start_publisher(N, [T|Topics], Config) ->
    {ok, _} = supervisor:start_child(?MODULE, [[{topic, T}|Config]]),
    timer:sleep(10),
    start_publisher(N - 1, Topics ++ [T], Config).


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
          [{vmq_bench_pub, {vmq_bench_pub, start_link, []},
                            permanent, 5000, worker, [vmq_bench_pub]}]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
