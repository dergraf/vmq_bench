-module(vmq_bench_stats).

-export([init_counters/1,
         incr_counters/4,
         statistics/0]).

init_counters(pub) ->
    {publisher, os:timestamp(), 0, 0, []};
init_counters(con) ->
    {consumer, os:timestamp(), 0, 0, []}.

incr_counters(MsgIncr, ByteIncr, LatPoint, {Type, {MegaSecs, Secs, _} = TS, MsgCnt, ByteCnt, Lats}) ->
    Now = os:timestamp(),
    case {LatPoint, Type} of
        {{_, _, _}, consumer}  ->
            Lat = calc_lats(Now, LatPoint),
            folsom_metrics:notify({latency, trunc(Lat/1000)});
        _ ->
            ok
    end,
    case Now of
        {MegaSecs, Secs, _} ->
            {Type, TS, MsgCnt + MsgIncr, ByteCnt + ByteIncr, Lats};
        _ ->
            safe_update_counter(Type, MsgCnt, ByteCnt),
            {Type, Now, MsgIncr, ByteIncr, []}
    end.

safe_update_counter(Type, MsgCnt, ByteCnt) ->
    case Type of
        publisher ->
            folsom_metrics:notify({published_msgs, {inc, MsgCnt}}),
            folsom_metrics:notify({published_bytes, {inc, ByteCnt}});
        consumer ->
            folsom_metrics:notify({consumed_msgs, {inc, MsgCnt}}),
            folsom_metrics:notify({consumed_bytes, {inc, ByteCnt}})
    end.

calc_lats(TS, {_,_,_} = TS) -> 0;
calc_lats({MegaSecs, Secs, MicroSecs}, {MegaSecs, Secs, MMicroSecs}) ->
    %% only differ in MicroSecs
    abs(MicroSecs - MMicroSecs);
calc_lats({MegaSecs, Secs, MicroSecs}, {MegaSecs, SSecs, MMicroSecs}) ->
    abs(((Secs * 1000000) + MicroSecs) -
        ((SSecs * 1000000) + MMicroSecs));
calc_lats({MegaSecs, Secs, MicroSecs}, {MMegaSecs, SSecs, MMicroSecs}) ->
    abs(((MegaSecs * 1000000000) + (Secs * 1000000) + MicroSecs) -
        ((MMegaSecs * 1000000000) + (SSecs * 1000000) + MMicroSecs)).

statistics() ->
    PubCnt = folsom_metrics:get_metric_value(nr_of_publishers),
    SubCnt = folsom_metrics:get_metric_value(nr_of_consumers),
    R1 = folsom_metrics:get_metric_value(published_msgs),
    folsom_metrics:notify({published_msgs, {dec, R1}}),
    R2 = folsom_metrics:get_metric_value(published_bytes),
    folsom_metrics:notify({published_bytes, {dec, R2}}),
    R3 = folsom_metrics:get_metric_value(consumed_msgs),
    folsom_metrics:notify({consumed_msgs, {dec, R3}}),
    R4 = folsom_metrics:get_metric_value(consumed_bytes),
    folsom_metrics:notify({consumed_bytes, {dec, R4}}),
    R5 = case catch folsom_metrics:get_metric_value(latency) of
             R -> R
         end,
    {PubCnt, SubCnt, R1, R2, R3, R4, R5}.

