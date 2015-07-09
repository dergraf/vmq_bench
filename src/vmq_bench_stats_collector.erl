-module(vmq_bench_stats_collector).

-behaviour(gen_server).

%% API functions
-export([start_link/1]).

%% RPC export
-export([adjust_interval/1,
         adjust_publisher_pool_size/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {fd,
                force_feedback_state,
                last_dumped=os:timestamp()
               }).
-record(collect, {
       nr_of_pubs=0,
       nr_of_cons=0,
       pub_msg_cnt=0,
       pub_byte_cnt=0,
       con_msg_cnt=0,
       con_byte_cnt=0,
       mean=0,
       median=0,
       std_dev=0,
       perc_50=0,
       perc_75=0,
       perc_95=0,
       perc_99=0,
       perc_999=0
      }).

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
start_link(Config) ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, Config, []).

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
init(Config) ->
    {{YY, MM, DD}, {H, M, S}} = calendar:universal_time_to_local_time(
                                  calendar:universal_time()),
    FolderName = io_lib:format("bench_~p-~p-~p_~p.~p.~p", [YY, MM, DD, H, M, S]),
    os:cmd("mkdir " ++ FolderName),
    os:cmd("rm current_bench"),
    os:cmd("ln -s " ++ FolderName ++ " current_bench"),
    FileNameStr = filename:join(FolderName, "bench.csv"),
    {ok, Fd} = file:open(FileNameStr, [append]),
    Header = io_lib:format("ts,nr_of_pub_msgs,nr_of_pub_data,nr_of_pubs,nr_of_con_msgs,nr_of_con_data,nr_of_cons,avg_latency,median_latency,variance_latency,lat_50,lat_75,lat_95,lat_99,lat_999,lat_ctrl~n", []),
    file:write(Fd, Header),

    FFState = init_force_feedback_state(Config),
    erlang:send_after(1000, self(), dump),
    {ok, #state{fd=Fd, force_feedback_state=FFState}}.

init_force_feedback_state(Config) ->
    FFAfter = proplists:get_value(force_feedback_after, Config, undefined),
    case FFAfter of
        undefined ->
            undefined;
        _ ->
            FFSampleSize = proplists:get_value(force_feedback_sample_size, Config, FFAfter),
            %% optimize send interval for 95% percentile <= 500ms
            FFx = proplists:get_value(force_feedback_x, Config, {send_interval, 1000}),
            FFSubState = init_force_feedback_sub_state(FFx),
            %% FFx = proplists:get_value(force_feedback_x, Config, {publisher_pool_size, 1000}),
            FFy = proplists:get_value(force_feedback_y, Config, {perc_95, 500}),
            FFSamples = init_samples(FFSampleSize),
            {FFAfter, FFAfter, FFx, FFy, FFSubState, FFSamples}
    end.

init_force_feedback_sub_state({send_interval, _}) -> [];
init_force_feedback_sub_state({publisher_pool_size, _}) ->
    case init:get_argument(scenario) of
        {ok, [[ScenarioFile]]} ->
            {ok, ScenarioConfig} = file:consult(ScenarioFile),
            [PubConfig|_] = proplists:get_all_values(publisher_config, ScenarioConfig),
            PubConfig;
        _ ->
            exit(cant_read_scenario_file)
    end.


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
handle_info(dump, #state{fd=Fd, force_feedback_state=FF, last_dumped=T1} = State) ->
    {Ret, _} = rpc:multicall(vmq_bench_stats, statistics, []),
    {MegaSecs, Secs, _} = T2 = os:timestamp(),
    TimeDiff1 = timer:now_diff(T2, T1) div 1000,
    io:format("time diff ~p~n" ,[TimeDiff1]),
    TS = (MegaSecs * 1000000) + Secs,
    #collect{
       nr_of_pubs = NrOfPubs,
       nr_of_cons = NrOfCons,
       pub_msg_cnt = PubMsgCnt,
       pub_byte_cnt = PubByteCnt,
       con_msg_cnt = ConMsgCnt,
       con_byte_cnt = ConByteCnt,
       mean = Mean,
       median = Median,
       std_dev = StdDev,
       perc_50 = Perc50,
       perc_75 = Perc75,
       perc_95 = Perc95,
       perc_99 = Perc99,
       perc_999 = Perc999
      } = CollectAvg = get_avg(Ret, TimeDiff1),
    {NewFF, {_,CtrlVar}}= maybe_force_feedback(FF, CollectAvg),
    T3 = os:timestamp(),
    TimeDiff2 = timer:now_diff(T3, T2) div 1000,
    Str = io_lib:format("~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p~n",
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
                                          Perc999,
                                          CtrlVar]),
    case TimeDiff2 > 1000 of
        true ->
            %% we don't write to file in this case, as the values are older than one second
            %% this means that the force feedback took too long
            io:format(Str),
            erlang:send_after(1000, self(), dump);
        false ->
            file:write(Fd, Str),
            erlang:send_after(1000 - TimeDiff2, self(), dump)
    end,
    {noreply, State#state{force_feedback_state=NewFF, last_dumped=T2}}.

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
get_avg(Results, TimeDiff) ->
    N = length(Results),
    {A,B,C,D,E,F,L} = get_ret_avg(Results, {0,0,0,0,0,0,[]}),
    LatsStats = bear:get_statistics_subset(L,[arithmetic_mean,
                                              min,
                                              max,
                                              median,
                                              {percentile, [50, 75, 95, 99, 999]},
                                              standard_deviation,
                                              variance]),
    Percentiles = proplists:get_value(percentile, LatsStats, []),
    #collect{
       nr_of_pubs = trunc(A/N),
       nr_of_cons = trunc(B/N),
       pub_msg_cnt = trunc(second_corrected(TimeDiff, C)/N),
       pub_byte_cnt = trunc(second_corrected(TimeDiff, D)/N),
       con_msg_cnt = trunc(second_corrected(TimeDiff, E)/N),
       con_byte_cnt = trunc(second_corrected(TimeDiff, F)/N),
       mean = trunc(proplists:get_value(arithmetic_mean, LatsStats, 0)),
       median = trunc(proplists:get_value(median, LatsStats, 0)),
       std_dev = trunc(proplists:get_value(standard_deviation, LatsStats, 0)),
       perc_50 = trunc(proplists:get_value(50, Percentiles, 0)),
       perc_75 = trunc(proplists:get_value(75, Percentiles, 0)),
       perc_95 = trunc(proplists:get_value(95, Percentiles, 0)),
       perc_99 = trunc(proplists:get_value(99, Percentiles, 0)),
       perc_999 = trunc(proplists:get_value(999, Percentiles, 0))
      }.

second_corrected(DurationInMs, Val) ->
    (1000 * Val) / DurationInMs.

get_ret_avg([{NrOfPubs, NrOfCons,
          PublishedMsgs, PublishedBytes,
          ConsumedMsgs, ConsumedBytes, Lats}|Rest],
        {ANrOfPubs, ANrOfCons,
         APublishedMsgs, APublishedBytes,
         AConsumedMsgs, AConsumedBytes, ALats}) ->
    get_ret_avg(Rest, {NrOfPubs + ANrOfPubs,
                   NrOfCons + ANrOfCons,
                   PublishedMsgs + APublishedMsgs,
                   PublishedBytes + APublishedBytes,
                   ConsumedMsgs + AConsumedMsgs,
                   ConsumedBytes + AConsumedBytes,
                   Lats ++ ALats});
get_ret_avg([], Acc) -> Acc.

get_sample_avg(Collection) ->
    get_sample_avg(Collection, length(Collection), #collect{}).
get_sample_avg([Collect|Rest], S, AccCollect) ->
    get_sample_avg(Rest, S, add_rec(Collect, AccCollect));
get_sample_avg([], S, AccCollect) ->
    div_rec(AccCollect, S).

add_rec(Rec1, Rec2) ->
    [RecName|L1] = tuple_to_list(Rec1),
    [RecName|L2] = tuple_to_list(Rec2),
    {Sum, _} = lists:mapfoldl(fun(V, I) ->
                                      {V + lists:nth(I, L2), I + 1}
                              end, 1, L1),
    list_to_tuple([RecName|Sum]).

div_rec(Rec, D) ->
    [RecName|L] = tuple_to_list(Rec),
    list_to_tuple([RecName|[trunc(E / D) || E <- L]]).

-define(LATENCY_TOLERANCE, 10). %% we tolerate a difference of 10ms

maybe_force_feedback(undefined, _) -> {undefined, 0};
maybe_force_feedback({FFAfter, FFAfterTmp, FFx, FFy, FFSt, FFSamples}, Collect)
  when FFAfterTmp > 0->
    {{FFAfter, FFAfterTmp - 1, FFx, FFy, FFSt, add_sample(Collect, FFSamples)}, FFx};
maybe_force_feedback({FFAfter, 0, FFx, {YType, UpperBound} = FFy, FFSt, FFSamples}, Collect) ->
    NewFFSamples = add_sample(Collect, FFSamples),
    #collect{nr_of_pubs=AvgNrOfPubs,
             pub_msg_cnt=AvgPubMsgCnt
            } = AvgCollect = get_sample_avg(NewFFSamples),
    AvgOptim = get_collect_val(YType, AvgCollect),
    case (AvgNrOfPubs > 0) and (AvgPubMsgCnt > 0) of
        true ->
            MaxOptim = max(AvgOptim, get_collect_val(YType, Collect)),
            {NewFFAfterTmp, {NewFFx, NewFFSt}} =
            case abs(MaxOptim - UpperBound) > ?LATENCY_TOLERANCE of
                false ->
                    {FFAfter, {FFx, FFSt}};
                true when MaxOptim > UpperBound ->
                    {1, force_feedback(FFx, FFSt, trunc(MaxOptim - UpperBound))};
                true ->
                    {2, force_feedback(FFx, FFSt, trunc(MaxOptim - UpperBound))}
            end,
            {{FFAfter, NewFFAfterTmp, NewFFx, FFy, NewFFSt, NewFFSamples}, NewFFx};
        false ->
            {{FFAfter, FFAfter, FFx, FFy, FFSt, NewFFSamples}, FFx}
    end.

get_collect_val(mean, #collect{mean=Mean}) -> Mean;
get_collect_val(median, #collect{median=Median}) -> Median;
get_collect_val(std_dev, #collect{std_dev=StdDev}) -> StdDev;
get_collect_val(perc_50, #collect{perc_50=Perc}) -> Perc;
get_collect_val(perc_75, #collect{perc_75=Perc}) -> Perc;
get_collect_val(perc_95, #collect{perc_95=Perc}) -> Perc;
get_collect_val(perc_99, #collect{perc_99=Perc}) -> Perc;
get_collect_val(perc_999, #collect{perc_999=Perc}) -> Perc.

non_zero(V) when V =< 0 -> 1;
non_zero(V) -> V.

step(V) -> non_zero(V div 10).

init_samples(undefined) -> [];
init_samples(Size) ->
    [#collect{}|| _ <- lists:seq(1, Size)].

add_sample(Collect, Samples) ->
    [_|T] = lists:reverse(Samples),
    [Collect|lists:reverse(T)].

force_feedback({send_interval, CurrentInterval}, FFSt, Change) ->
    NewInterval =
    case Change > 0 of
        true ->
            %% we need to increase the interval to lower the latency
            CurrentInterval + step(abs(Change));
        false ->
            %% we need to decrease the interval to increase latency
            CurrentInterval - non_zero(step(abs(Change)))
    end,
    rpc:eval_everywhere(?MODULE, adjust_interval, [NewInterval]),
    {{send_interval, NewInterval}, FFSt};

force_feedback({publisher_pool_size, CurrentPoolSize}, FFSt, Change) ->
    NewPoolSize =
    case Change > 0 of
        true ->
            %% we need to decrease pool size to lower the latency
            CurrentPoolSize - non_zero(step(abs(Change)));
        false ->
            %% we need to increase the pool size to increase latency
            CurrentPoolSize + step(abs(Change))
    end,
    Nodes = proplists:get_value(nodes, FFSt),
    Topics = proplists:get_value(topics, FFSt, []),
    Config = lists:keydelete(topics, 1, FFSt),
    case Nodes of
        undefined ->
            adjust_publisher_pool_size(NewPoolSize, Topics, Config);
        _ ->
            rpc:eval_everywhere(Nodes, ?MODULE, adjust_publisher_pool_size, [NewPoolSize, Topics, Config])
    end,
    {{publisher_pool_size, NewPoolSize}, FFSt}.

adjust_interval(NewInterval) ->
    Children = supervisor:which_children(vmq_bench_pub_sup),
    adjust_interval(Children, NewInterval).

adjust_interval([{_, Pid, _, _}|Rest], NewInterval) when is_pid(Pid) ->
    vmq_bench_pub:change_interval(Pid, NewInterval),
    adjust_interval(Rest, NewInterval);
adjust_interval([_|Rest], NewInterval) ->
    adjust_interval(Rest, NewInterval);
adjust_interval([], _) -> ok.


adjust_publisher_pool_size(NewPoolSize, Topics, Config) ->
    %% this could take longer if we are under high load
    spawn(fun() ->
                  NrOfPubNodes = length(proplists:get_value(nodes, Config, [node()])),
                  Children = supervisor:which_children(vmq_bench_pub_sup),
                  Diff = (length(Children) - NewPoolSize) div NrOfPubNodes,
                  case Diff > 0 of
                      true ->
                          {ChildrenToTerminate, _} = lists:split(Diff, Children),
                          lists:foreach(fun({_, ChildPid, _, _}) ->
                                                vmq_bench_pub_sup:stop_publisher(ChildPid)
                                        end, ChildrenToTerminate);
                      false ->
                          vmq_bench_pub_sup:start_publisher(abs(Diff), Topics, 0, Config)
                  end
          end).


