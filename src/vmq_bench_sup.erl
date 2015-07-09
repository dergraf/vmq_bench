-module(vmq_bench_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         add_stats_collector/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Args), {I, {I, start_link, Args}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

add_stats_collector(Config) ->
    supervisor:start_child(?MODULE, ?CHILD(vmq_bench_stats_collector, worker, [Config])).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, { {one_for_one, 5, 10},
           [
            ?CHILD(vmq_bench_pub_sup, supervisor, []),
            ?CHILD(vmq_bench_con_sup, supervisor, [])]}}.

