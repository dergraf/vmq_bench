-module(vmq_bench).
-export([start/0]).

%% sender config
%% -------------
%% nr of senders [int]
%% start sending upon arrival completed [bool]
%% clean_session [bool]
%% retain [bool]
%% topic [string]
%% qos [0,1,2]
%% send interval [int]
%%
%% receiver config
%% ---------------
%% nr of receivers [int]
%% clean_session [bool]
%% topic [string]
%% qos [0,1,2]


start() ->

    application:ensure_all_started(vmq_bench),
    folsom_metrics:new_counter(published_msgs),
    folsom_metrics:new_counter(published_bytes),
    folsom_metrics:new_counter(consumed_msgs),
    folsom_metrics:new_counter(consumed_bytes),
    folsom_metrics:new_histogram(latency, slide, 10),
    folsom_metrics:new_counter(nr_of_consumers),
    folsom_metrics:new_counter(nr_of_publishers),
    folsom_metrics:tag_metric(published_msgs, vmq),
    folsom_metrics:tag_metric(published_bytes, vmq),
    folsom_metrics:tag_metric(consumed_msgs, vmq),
    folsom_metrics:tag_metric(consumed_bytes, vmq),
    folsom_metrics:tag_metric(nr_of_consumers, vmq),
    folsom_metrics:tag_metric(nr_of_publishers, vmq),
    folsom_metrics:tag_metric(latency, vmq_lats),
    case init:get_argument(scenario) of
        {ok, [[ScenarioFile]]} ->
            {ok, ScenarioConfig} = file:consult(ScenarioFile),
            PublisherConfigs = proplists:get_all_values(publisher_config, ScenarioConfig),
            ConsumerConfigs = proplists:get_all_values(consumer_config, ScenarioConfig),
            ForceFeedbackConfig = proplists:get_value(force_feedback, ScenarioConfig, []),
            [MasterNode|Nodes] = proplists:get_value(test_nodes, ScenarioConfig, [node()]),
            case node() of
                MasterNode ->
                    vmq_bench_sup:add_stats_collector(ForceFeedbackConfig);
                _ ->
                    wait_till_master_ready()
            end,
            wait_till_nodes_are_ready(Nodes),

            case node() of
                MasterNode ->
                    spawn_link(
                      fun() ->
                              lists:foreach(
                                fun(PublisherConfig) ->
                                        PubImplMod = proplists:get_value(impl_mod, PublisherConfig, vmq_bench_pub),
                                        PubSupervisor = get_supervisor_mod(PubImplMod),

                                        PubSupervisor:start_publishers([PublisherConfig])
                                end, PublisherConfigs)
                               end),
                    spawn_link(fun() ->
                                       vmq_bench_con_sup:start_consumers(ConsumerConfigs)
                               end);
                _ ->
                    ignore
            end;
        _ ->
            wait_till_master_ready()
    end.


wait_till_master_ready() ->
    case global:whereis_name(vmq_bench_stats_collector) of
        undefined ->
            io:format("--- master not ready~n", []),
            timer:sleep(1000),
            wait_till_master_ready();
        P when is_pid(P) ->
            ok
    end.

wait_till_nodes_are_ready([]) -> ok;
wait_till_nodes_are_ready([Node|Nodes]) ->
    case net_adm:ping(Node) of
        pong ->
            case rpc:call(Node, erlang, whereis, [vmq_bench_stats]) of
                undefined ->
                    io:format("--- slave ~p not ready~n", [Node]),
                    timer:sleep(1000),
                    wait_till_nodes_are_ready(Nodes ++ [Node]);
                P when is_pid(P) ->
                    wait_till_nodes_are_ready(Nodes)
            end;
        pang ->
            io:format("--- slave ~p not reachable~n", [Node]),
            timer:sleep(1000),
            wait_till_nodes_are_ready(Nodes ++ [Node])
    end.


get_supervisor_mod(vmq_bench_pub) -> vmq_bench_pub_sup;
get_supervisor_mod(vmq_bench_pubsubself) -> vmq_bench_pubsubself_sup.



