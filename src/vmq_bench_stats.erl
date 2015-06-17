-module(vmq_bench_stats).

-behaviour(gen_server).

%% API functions
-export([start_link/0,
         init_counters/1,
         incr_counters/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).
-define(TBL_PUB, vmq_bench_pub_stats).
-define(TBL_CON, vmq_bench_con_stats).
-define(TBL_LAT, vmq_bench_lat_stats).

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
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init_counters(pub) ->
    {?TBL_PUB, os:timestamp(), 0, 0, []};
init_counters(con) ->
    {?TBL_CON, os:timestamp(), 0, 0, []}.

incr_counters(MsgIncr, ByteIncr, LatPoint, {Type, {MegaSecs, Secs, _} = TS, MsgCnt, ByteCnt, Lats}) ->
    case os:timestamp() of
        {MegaSecs, Secs, _} = Now ->
            {Type, TS, MsgCnt + MsgIncr, ByteCnt + ByteIncr,
             add_lats(Now, LatPoint, Lats)};
        Now ->
            LastUnixTs = (MegaSecs * 1000000) + Secs,
            safe_update_counter(Type, LastUnixTs, MsgCnt, ByteCnt, Lats),
            {Type, Now, MsgIncr, ByteIncr, add_lats(Now, LatPoint, [])}
    end.

add_lats(TS, {_,_,_} = TS, Lats) ->
    [0|Lats];
add_lats({MegaSecs, Secs, MicroSecs}, {MegaSecs, Secs, MMicroSecs}, Lats) ->
    %% only differ in MicroSecs
    [abs(MicroSecs - MMicroSecs)|Lats];
add_lats({MegaSecs, Secs, MicroSecs}, {MegaSecs, SSecs, MMicroSecs}, Lats) ->
    [abs(((Secs * 1000000) + MicroSecs) -
         ((SSecs * 1000000) + MMicroSecs))|Lats];
add_lats({MegaSecs, Secs, MicroSecs}, {MMegaSecs, SSecs, MMicroSecs}, Lats) ->
    [abs(((MegaSecs * 1000000000) + (Secs * 1000000) + MicroSecs) -
         ((MMegaSecs * 1000000000) + (SSecs * 1000000) + MMicroSecs))|Lats];
add_lats(_, _, Lats) -> Lats.

safe_update_counter(Type, TS, MsgCnt, ByteCnt, Lats) ->
    ets:insert(?TBL_LAT, {TS, calc_lats(Lats)}),
    safe_update_counter_(Type, TS, MsgCnt, ByteCnt).

safe_update_counter_(Type, TS, MsgCnt, ByteCnt) ->
    try ets:update_counter(Type, TS, [{2, MsgCnt}, {3, ByteCnt}])
    catch error:badarg ->
              case ets:insert_new(Type, {TS, MsgCnt, ByteCnt}) of
                  true -> ok;
                  false ->
                      safe_update_counter_(Type, TS, MsgCnt, ByteCnt)
              end
    end.

calc_lats([]) -> {0, 0, 0, 0, 0, 0, 0, 0, 0};
calc_lats(Lats) ->
    N = length(Lats),
    LatAvg = lists:sum(Lats) / N,
    LatMed = lists:nth((N + 1) div 2, lists:sort(Lats)),
    LatVar = math:sqrt(lists:foldl(fun(V, Acc) ->
                        Acc + math:pow(V - LatAvg, 2)
                end, 0, Lats) / N),
    list_to_tuple([LatAvg, LatMed, LatVar | percentiles(Lats)]).


percentiles(Lats) ->
    Len = length(Lats),
    Sorted = lists:sort(Lats),
    [percentile(Sorted, Len, Perc) ||
     Perc <- [0.50, 0.75, 0.90, 0.95, 0.99, 0.999]].

percentile(List, Size, Perc) ->
    Element = round(Perc * Size),
    lists:nth(Element, List).




%%%
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
    ets:new(?TBL_PUB, [ordered_set, public, named_table, {write_concurrency, true}]),
    ets:new(?TBL_CON, [ordered_set, public, named_table, {write_concurrency, true}]),
    ets:new(?TBL_LAT, [public, bag, named_table, {write_concurrency, true}]),
    erlang:send_after(1000, self(), dump),
    {ok, #state{}}.

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
handle_info(dump, State) ->
    {_, PubMsgCnt, PubByteCnt} = val_or_0(?TBL_PUB, ets:first(?TBL_PUB)),
    {TS, ConMsgCnt, ConByteCnt} = val_or_0(?TBL_CON, ets:first(?TBL_CON)),
    NrOfPubs = length(supervisor:which_children(vmq_bench_pub_sup)),
    NrOfCons = length(supervisor:which_children(vmq_bench_con_sup)),

    Lats = ets:lookup(?TBL_LAT, TS),
    ets:delete(?TBL_LAT, TS),

    vmq_bench_stats_collector:collect(TS,
                                      {PubMsgCnt, PubByteCnt, NrOfPubs,
                                       ConMsgCnt, ConByteCnt, NrOfCons,
                                       Lats}),

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
val_or_0(_, '$end_of_table') ->
    {MegaSecs, Secs, _} = os:timestamp(),
    Ts = (MegaSecs * 1000000) + Secs,
    {Ts, 0, 0};
val_or_0(T, TS) ->
    [{TS, MsgCnt, ByteCnt}] = ets:lookup(T, TS),
    ets:delete(T, TS),
    {TS, MsgCnt, ByteCnt}.
