-module(vmq_bench_stats_collector).

-behaviour(gen_server).

%% API functions
-export([start_link/0,
         collect/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {fd}).
-define(TBL_COLLECT, bench_collect).
-define(TBL_COLLECT_LATS, bench_collect_lats).

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

collect(TS, Measurement) ->
    gen_server:cast({global, ?MODULE}, {collect, TS, Measurement}).

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
    ets:new(?TBL_COLLECT, [ordered_set, named_table]),
    ets:new(?TBL_COLLECT_LATS, [bag, public, named_table]),
    {{YY, MM, DD}, {H, M, S}} = calendar:universal_time_to_local_time(
                                  calendar:universal_time()),
    FolderName = io_lib:format("bench_~p-~p-~p_~p.~p.~p", [YY, MM, DD, H, M, S]),
    os:cmd("mkdir " ++ FolderName),
    os:cmd("rm current_bench"),
    os:cmd("ln -s " ++ FolderName ++ " current_bench"),
    FileNameStr = filename:join(FolderName, "bench.csv"),
    {ok, Fd} = file:open(FileNameStr, [append]),
    Header = io_lib:format("ts,nr_of_pub_msgs,nr_of_pub_data,nr_of_pubs,nr_of_con_msgs,nr_of_con_data,nr_of_cons,avg_latency,median_latency,variance_latency,lat_50,lat_75,lat_90,lat_95,lat_99,lat_999~n", []),
    file:write(Fd, Header),
    {ok, [[ScenarioFile]]} = init:get_argument(scenario),
    os:cmd("cp " ++ ScenarioFile ++ " current_bench/bench.scenario"),
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
handle_cast({collect, TS, Measurement}, State) ->
    {PubMsgCnt, PubByteCnt, NrOfPubs,
     ConMsgCnt, ConByteCnt, NrOfCons,
     Lats} = Measurement,
    try ets:update_counter(?TBL_COLLECT, TS,
                           [{2, PubMsgCnt},
                            {3, PubByteCnt},
                            {4, NrOfPubs},
                            {5, ConMsgCnt},
                            {6, ConByteCnt},
                            {7, NrOfCons}])
    catch error:badarg ->
              true = ets:insert_new(?TBL_COLLECT,
                                    {TS, PubMsgCnt, PubByteCnt, NrOfPubs,
                                         ConMsgCnt, ConByteCnt, NrOfCons})
    end,
    ets:insert(?TBL_COLLECT_LATS, Lats),
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
    {MegaSecs, Secs, _} = os:timestamp(),
    OldUnixTs = (MegaSecs * 1000000) + Secs - 10, %% we take 10 second old values
    {PubMsgCnt, PubByteCnt, NrOfPubs,
     ConMsgCnt, ConByteCnt, NrOfCons} = val_or_0(?TBL_COLLECT, ets:lookup(?TBL_COLLECT, OldUnixTs)),
    Lats = ets:lookup(?TBL_COLLECT_LATS, OldUnixTs),
    ets:delete(?TBL_COLLECT_LATS, OldUnixTs),
    %         Avg    Median  StdVar  50-perc 75-perc 90-perc 95-perc 99-perc 999-perc
    InitAcc = [0,       0,      0,      0,      0,      0,      0,      0,      0],
    AvgLats =
    case length(Lats) of
        0 ->
            InitAcc;
        M ->
            LatSums =
            lists:foldl(fun({_, LatVals}, AccLatVals) when size(LatVals) == length(AccLatVals) ->
                                T = lists:foldl(fun(I, Acc) ->
                                                        Sum =  element(I, LatVals)
                                                        + lists:nth(I, AccLatVals),
                                                        [Sum|Acc]
                                                end, [], lists:seq(1, size(LatVals))),
                                lists:reverse(T)
                        end, InitAcc, Lats),
            MM = M * 1000,
            lists:reverse(lists:foldl(fun(LatSum, Acc) ->
                                              [trunc(LatSum / MM)|Acc]
                                      end, [], LatSums))
    end,
    file:write(Fd, io_lib:format("~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p,~p~n",
                                 [OldUnixTs,
                                  PubMsgCnt,
                                  PubByteCnt,
                                  NrOfPubs,
                                  ConMsgCnt,
                                  ConByteCnt,
                                  NrOfCons | AvgLats])),
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
val_or_0(_, []) -> {0, 0, 0, 0, 0, 0};
val_or_0(T, [{TS, PubMsgCnt, PubByteCnt, NrOfPubs, ConMsgCnt, ConByteCnt, NrOfCons}]) ->
    ets:delete(T, TS),
    {PubMsgCnt, PubByteCnt, NrOfPubs, ConMsgCnt, ConByteCnt, NrOfCons}.
