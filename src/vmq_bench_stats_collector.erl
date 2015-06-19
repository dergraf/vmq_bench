-module(vmq_bench_stats_collector).

-behaviour(gen_server).

%% API functions
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {fd}).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    {{YY, MM, DD}, {H, M, S}} = calendar:universal_time_to_local_time(
                                  calendar:universal_time()),
    FolderName = io_lib:format("bench_~p-~p-~p_~p.~p.~p", [YY, MM, DD, H, M, S]),
    os:cmd("mkdir " ++ FolderName),
    os:cmd("rm current_bench"),
    os:cmd("ln -s " ++ FolderName ++ " current_bench"),
    FileNameStr = filename:join(FolderName, "bench.csv"),
    {ok, Fd} = file:open(FileNameStr, [append]),
    Header = io_lib:format("ts,nr_of_pub_msgs,nr_of_pub_data,nr_of_pubs,nr_of_con_msgs,nr_of_con_data,nr_of_cons,avg_latency,median_latency,variance_latency,lat_50,lat_75,lat_95,lat_99,lat_999~n", []),
    file:write(Fd, Header),
    {ok, [[ScenarioFile]]} = init:get_argument(scenario),
    os:cmd("cp " ++ ScenarioFile ++ " current_bench/bench.scenario"),
    {ok, ScenarioConfig} = file:consult(ScenarioFile),
    write_json_file(ScenarioConfig),
    erlang:send_after(1000, self(), dump),
    {ok, #state{fd=Fd}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(dump, #state{fd=Fd} = State) ->
    {Ret, _} = rpc:multicall(vmq_bench_stats, statistics, []),
    {MegaSecs, Secs, _} = os:timestamp(),
    TS = (MegaSecs * 1000000) + Secs,
    {NrOfPubs, NrOfCons,
     PubMsgCnt, PubByteCnt,
     ConMsgCnt, ConByteCnt,
     Mean, Median, StdDev, Perc50,
     Perc75, Perc95, Perc99, Perc999} = get_avg(Ret),
    file:write(Fd, io_lib:format("~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p~n",
                                 [TS,
                                  PubMsgCnt,
                                  PubByteCnt,
                                  NrOfPubs,
                                  ConMsgCnt,
                                  ConByteCnt,
                                  NrOfCons,
                                  Mean,
                                  Median,
                                  StdDev,
                                  Perc50,
                                  Perc75,
                                  Perc95,
                                  Perc99,
                                  Perc999])),

    erlang:send_after(1000, self(), dump),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
get_avg(Results) ->
    N = length(Results),
    {A,B,C,D,E,F,L} = get_avg(Results, {0,0,0,0,0,0,[]}),
    LatsStats = bear:get_statistics_subset(L,[arithmetic_mean,
                                              min,
                                              max,
                                              median,
                                              {percentile, [50, 75, 95, 99, 999]},
                                              standard_deviation,
                                              variance]),
    Percentiles = proplists:get_value(percentile, LatsStats, []),
    {trunc(A/N),
     trunc(B/N),
     trunc(C/N),
     trunc(D/N),
     trunc(E/N),
     trunc(F/N),
     trunc(proplists:get_value(arithmetic_mean, LatsStats, 0)),
     trunc(proplists:get_value(median, LatsStats, 0)),
     trunc(proplists:get_value(standard_deviation, LatsStats, 0)),
     trunc(proplists:get_value(50, Percentiles, 0)),
     trunc(proplists:get_value(75, Percentiles, 0)),
     trunc(proplists:get_value(95, Percentiles, 0)),
     trunc(proplists:get_value(99, Percentiles, 0)),
     trunc(proplists:get_value(999, Percentiles, 0))}.

get_avg([{NrOfPubs, NrOfCons,
          PublishedMsgs, PublishedBytes,
          ConsumedMsgs, ConsumedBytes, Lats}|Rest],
        {ANrOfPubs, ANrOfCons,
         APublishedMsgs, APublishedBytes,
         AConsumedMsgs, AConsumedBytes, ALats}) ->
    get_avg(Rest, {NrOfPubs + ANrOfPubs,
                   NrOfCons + ANrOfCons,
                   PublishedMsgs + APublishedMsgs,
                   PublishedBytes + APublishedBytes,
                   ConsumedMsgs + AConsumedMsgs,
                   ConsumedBytes + AConsumedBytes,
                   Lats ++ ALats});
get_avg([], Acc) -> Acc.

write_json_file(ScenarioConfig) ->
    PublisherConfigs = proplists:get_all_values(publisher_config, ScenarioConfig),
    PublisherSummary =
    lists:foldl(fun(PubConfig, Acc) ->
                        Nodes = proplists:get_value(nodes, PubConfig, [node()]),
                        NrOfPublishers = length(Nodes) * proplists:get_value(max_concurrency, PubConfig, 1),
                        Topics = proplists:get_value(topics, PubConfig, []),
                        Partitions = length(lists:usort([T || {T, _} <- Topics])),
                        QoS = [QoS || {_, QoS} <- Topics],
                        TotalTopics = length(QoS),
                        NrOfQoS0 = length([0 || 0 <- QoS]) * 100 / TotalTopics,
                        NrOfQoS1 = length([1 || 1 <- QoS]) * 100 / TotalTopics,
                        NrOfQoS2 = length([2 || 2 <- QoS]) * 100 / TotalTopics,
                        PartitionSize = NrOfPublishers / Partitions,
                        PubConnectRate = 1000 / proplists:get_value(setup_every, PubConfig, 10),
                        BatchSize = proplists:get_value(msgs_per_step, PubConfig, 1),
                        PublishRate = BatchSize * (1000 / proplists:get_value(interval, PubConfig, 1000)),
                        ConnectOpts = proplists:get_value(connect_opts, PubConfig, []),
                        KeepAlive = proplists:get_value(keepalive, ConnectOpts, 60),
                        PublishOpts = proplists:get_value(publish_opts, PubConfig, []),
                        Retain = proplists:get_value(retain, PublishOpts, false),
                        Payload = proplists:get_value(payload, PubConfig, "test-message"),
                        {MinimumSize, MaximumSize} =
                        case {lists:keyfind(max, 1, Payload),
                              lists:keyfind(min, 1, Payload)} of
                            {false, false} ->
                                S = byte_size(term_to_binary({os:timestamp(), list_to_binary(Payload)})),
                                {S, S};
                            {{_, Max}, false} ->
                                MinS = byte_size(term_to_binary({os:timestamp(), <<>>})),
                                MaxS = byte_size(term_to_binary({os:timestamp(), crypto:rand_bytes(Max)})),
                                {MinS, MaxS};
                            {false, {_, Min}} ->
                                Max = 100000,
                                MinS = byte_size(term_to_binary({os:timestamp(), crypto:rand_bytes(abs(Min - Max))})),
                                MaxS = byte_size(term_to_binary({os:timestamp(), crypto:rand_bytes(Max)})),
                                {MinS, MaxS};
                            {{_, Max}, {_, Min}} ->
                                MinS = byte_size(term_to_binary({os:timestamp(), crypto:rand_bytes(Min)})),
                                MaxS = byte_size(term_to_binary({os:timestamp(), crypto:rand_bytes(Max)})),
                                {MinS, MaxS}
                        end,
                        [[{nr_of_publishers, NrOfPublishers},
                          {nr_of_partitions, Partitions},
                          {partition_size, PartitionSize},
                          {connect_rate, PubConnectRate},
                          {keep_alive, KeepAlive},
                          {retain, Retain},
                          {qos_0, NrOfQoS0},
                          {qos_1, NrOfQoS1},
                          {qos_2, NrOfQoS2},
                          {batch_size, BatchSize},
                          {publish_rate, PublishRate},
                          {min_message_size, MinimumSize},
                          {max_message_size, MaximumSize}]|Acc]
                end, [], PublisherConfigs),

    ConsumerConfigs = proplists:get_all_values(consumer_config, ScenarioConfig),
    ConsumerSummary =
    lists:foldl(fun(ConConfig, Acc) ->
                        Nodes = proplists:get_value(nodes, ConConfig, [node()]),
                        NrOfConsumers = length(Nodes) * proplists:get_value(max_concurrency, ConConfig, 1),
                        ConConnectRate = 1000 / proplists:get_value(setup_every, ConConfig, 10),

                        Topics = proplists:get_value(topics, ConConfig, []),
                        Partitions = length(lists:usort([T || {T, _} <- Topics])),
                        QoS = [QoS || {_, QoS} <- Topics],
                        TotalTopics = length(QoS),
                        NrOfQoS0 = length([0 || 0 <- QoS]) * 100 / TotalTopics,
                        NrOfQoS1 = length([1 || 1 <- QoS]) * 100 / TotalTopics,
                        NrOfQoS2 = length([2 || 2 <- QoS]) * 100 / TotalTopics,
                        PartitionSize = NrOfConsumers / Partitions,
                        ConnectOpts = proplists:get_value(connect_opts, ConConfig, []),
                        KeepAlive = proplists:get_value(keepalive, ConnectOpts, 60),
                        [[{nr_of_consumers, NrOfConsumers},
                          {nr_of_partitions, Partitions},
                          {partition_size, PartitionSize},
                          {connect_rate, ConConnectRate},
                          {keep_alive, KeepAlive},
                          {qos_0, NrOfQoS0},
                          {qos_1, NrOfQoS1},
                          {qos_2, NrOfQoS2}]|Acc]
                end, [], ConsumerConfigs),


    Summary = [{publishers, PublisherSummary},
               {consumers, ConsumerSummary}],
    io:format("config:~n ~p", [jsx:encode(Summary)]),
    ok.
