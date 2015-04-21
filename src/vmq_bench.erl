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
    {ok, [[ScenarioFile]]} = init:get_argument(scenario),
    {ok, ScenarioConfig} = file:consult(ScenarioFile),
    PublisherConfigs = proplists:get_all_values(publisher_config, ScenarioConfig),
    ConsumerConfigs = proplists:get_all_values(consumer_config, ScenarioConfig),
    application:ensure_all_started(vmq_bench),
    spawn_link(fun() ->
                       vmq_bench_pub_sup:start_publishers(PublisherConfigs)
               end),
    spawn_link(fun() ->
                       vmq_bench_con_sup:start_consumers(ConsumerConfigs)
               end).

