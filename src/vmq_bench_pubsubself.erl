-module(vmq_bench_pubsubself).
-include_lib("emqtt_commons/include/emqtt_frame.hrl").

-behaviour(gen_server).

%% API functions
-export([start_link/1,
         get_topic/1,
         change_interval/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {host,
                port,
                socket,
                client_id,
                connect_opts,
                publish_opts,
                payload_generator,
                retain,
                topic,
                qos,
                interval,
                msgs_per_step,
                next_mid=1,
                parser_state=emqtt_frame:initial_state(),
                pub_counters=vmq_bench_stats:init_counters(pub),
                sub_counters=vmq_bench_stats:init_counters(con)}).

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
    gen_server:start_link(?MODULE, [Config], []).

get_topic(Pid) ->
    gen_server:call(Pid, get_topic).

change_interval(Pid, NewInterval) ->
    gen_server:cast(Pid, {change_interval, NewInterval}).

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
init([Config]) ->
    {A, B, C} = now(),
    random:seed(A, B, C),
    StartAfter = proplists:get_value(start_after, Config, 0),
    Interval = proplists:get_value(interval, Config, 1000),
    MsgsPerStep = proplists:get_value(msgs_per_step, Config, 1),
    Hosts = proplists:get_value(hosts, Config, [{"localhost", 1883}]),
    ConnectOpts = proplists:get_value(connect_opts, Config, []),
    ClientId = proplists:get_value(client_id, ConnectOpts,
                                   "vmq-pub-" ++ integer_to_list(erlang:phash2({A,B,C, node()}))),

    {Topic, QoS} =
    case proplists:get_value(topic, Config, {"/test/topic", 0}) of
        {{Prefix, client_id}, TQoS} ->
            {Prefix ++ ClientId, TQoS};
        V -> V
    end,
    Payload = proplists:get_value(payload, Config, "test-message"),
    PublishOpts = proplists:get_value(publish_opts, Config, []),
    PayloadGenerator =
    case {lists:keyfind(max, 1, Payload),
          lists:keyfind(min, 1, Payload)} of
        {false, false} -> fun() -> list_to_binary(Payload) end;
        {{_, Max}, false} ->
            fun() ->
                    crypto:rand_bytes(random:uniform(Max))
            end;
        {false, {_, Min}} ->
            fun() ->
                    Max = random:uniform(100000),
                    crypto:rand_bytes(abs(Min - Max))
            end;
        {{_, Max}, {_, Min}} ->
            fun() -> crypto:rand_bytes(
                       abs(random:uniform(Max)
                           - random:uniform(Min))) end
    end,
    {Host, Port} = lists:nth(random:uniform(length(Hosts)), Hosts),

    case proplists:get_value(stop_after, Config, 0) of
        0 -> ok;
        StopAfter when is_integer(StopAfter) ->
            erlang:send_after(StopAfter, self(), stop_now)
    end,
    erlang:send_after(StartAfter, self(), connect),
    process_flag(trap_exit, true),
    {ok, #state{host=Host,
                port=Port,
                client_id=ClientId,
                connect_opts=ConnectOpts,
                publish_opts=PublishOpts,
                payload_generator=PayloadGenerator,
                topic=Topic,
                qos=QoS,
                interval=Interval,
                msgs_per_step=MsgsPerStep}}.

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
handle_call(get_topic, _From, #state{topic=Topic} = State) ->
    {reply, Topic, State};
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
handle_cast({change_interval, NewInterval}, #state{socket=Sock} = State)
    when Sock /= undefiend ->
    {noreply, State#state{interval=NewInterval}};
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
handle_info(connect, #state{socket=undefined, topic=Topic, qos=QoS} = State) ->
    #state{client_id=ClientId,
           host=Host,
           port=Port,
           interval=Interval,
           connect_opts=ConnectOpts} = State,
    {A,B,C} = now(),
    random:seed(A, B, C),

    Connect = packet:gen_connect(ClientId, ConnectOpts),
    Connack = packet:gen_connack(),
    case packet:do_client_connect(Connect, Connack,
                                  [{hostname, Host}, {port, Port}]) of
        {ok, Socket} ->
            folsom_metrics:notify({nr_of_publishers, {inc, 1}}),
            folsom_metrics:notify({nr_of_consumers, {inc, 1}}),
            Subscribe = packet:gen_subscribe(1, Topic, QoS),
            ok = gen_tcp:send(Socket, Subscribe),
            Suback = packet:gen_suback(1, QoS),
            ok = packet:expect_packet(Socket, "suback", Suback),
            inet:setopts(Socket, [{active, once}]),
            erlang:send_after(Interval + random:uniform(500), self(), publish),
            {noreply, State#state{socket=Socket}};
        {error, _} ->
            %% we retry in 1 second
            erlang:send_after(1000, self(), connect),
            {noreply, State}
    end;
handle_info({tcp, Socket, Data}, #state{interval=Interval} = State) ->
    NewNewState =
    case process_data(Data, false, State) of
        {NewState, true} ->
            erlang:send_after(Interval, self(), publish),
            NewState;
        {NewState, false} ->
            NewState
    end,
    inet:setopts(Socket, [{active, once}]),
    {noreply, NewNewState};

handle_info(publish, #state{socket=Socket} = State) ->
    #state{topic=Topic,
           payload_generator=Generator,
           publish_opts=PublishOpts,
           next_mid=Mid,
           qos=QoS,
           socket=Socket,
           pub_counters=PubCounters} = State,
    Payload = term_to_binary({os:timestamp(), Generator()}),
    PublishFrame = packet:gen_publish(Topic, QoS, Payload,
                                 [{mid, Mid} | PublishOpts]),

    case gen_tcp:send(Socket, PublishFrame) of
        ok ->
            L = byte_size(PublishFrame),
            NewPubCounters =
            case QoS of
                0 ->
                    vmq_bench_stats:incr_counters(1, L, nil, PubCounters);
                _ ->
                    vmq_bench_stats:incr_counters(0, L, nil, PubCounters)
            end,
            {noreply, State#state{next_mid=next_mid(Mid),
                                  pub_counters=NewPubCounters}};
        {error, closed} ->
            folsom_metrics:notify({nr_of_publishers, {dec, 1}}),
            folsom_metrics:notify({nr_of_consumers, {dec, 1}}),
            erlang:send_after(1000, self(), connect),
            {noreply, State#state{socket=undefined,
                                  next_mid=1,
                                  pub_counters=vmq_bench_stats:init_counters(pub),
                                  sub_counters=vmq_bench_stats:init_counters(con)}}
    end;

handle_info(stop_now, State) ->
    folsom_metrics:notify({nr_of_publishers, {dec, 1}}),
    {stop, normal, State}.

process_data(Data, TriggerPublish, #state{socket=Socket, parser_state=PS,
                                          pub_counters=PubCounters,
                                          sub_counters=SubCounters} = State) ->
    case emqtt_frame:parse(Data, PS) of
        {more, MorePS} ->
            {State#state{parser_state=MorePS}, TriggerPublish};
        {ok, #mqtt_frame{
                fixed=#mqtt_frame_fixed{type=?PUBLISH, qos=QoS},
                variable=#mqtt_frame_publish{message_id=MId},
                payload=Payload}, Rest} ->
            %% We received a publish frame
            L = byte_size(Payload),
            {LatPoint, _} = binary_to_term(Payload),
            {NewSubCounters, NewTriggerPublish} =
            case QoS of
                0 ->
                    {vmq_bench_stats:incr_counters(1, L, LatPoint, SubCounters), true};
                1 ->
                    Puback = packet:gen_puback(MId),
                    ok = gen_tcp:send(Socket, Puback),
                    {vmq_bench_stats:incr_counters(1, L, LatPoint, SubCounters), true};
                2 ->
                    Pubrec = packet:gen_pubrec(MId),
                    ok = gen_tcp:send(Socket, Pubrec),
                    {vmq_bench_stats:incr_counters(0, L, LatPoint, SubCounters), TriggerPublish}
            end,
            process_data(Rest, NewTriggerPublish, State#state{parser_state=emqtt_frame:initial_state(),
                                                              sub_counters=NewSubCounters});
        {ok, #mqtt_frame{
                fixed=#mqtt_frame_fixed{type=?PUBREL},
                variable=#mqtt_frame_publish{message_id=MId}}, Rest} ->
            Pubcomp = packet:gen_pubcomp(MId),
            ok = gen_tcp:send(Socket, Pubcomp),
            NewSubCounters = vmq_bench_stats:incr_counters(1, 0, nil, SubCounters),
            process_data(Rest, true, State#state{parser_state=emqtt_frame:initial_state(),
                                                 sub_counters=NewSubCounters});
        {ok, #mqtt_frame{
                fixed=#mqtt_frame_fixed{type=?PUBACK}}, Rest} ->
            NewPubCounters = vmq_bench_stats:incr_counters(1, 0, nil, PubCounters),
            process_data(Rest, TriggerPublish, State#state{parser_state=emqtt_frame:initial_state(),
                                                           pub_counters=NewPubCounters});
        {ok, #mqtt_frame{
                fixed=#mqtt_frame_fixed{type=?PUBREC},
                variable=#mqtt_frame_publish{message_id=MId}}, Rest} ->
            Pubrel = packet:gen_pubrel(MId),
            ok = gen_tcp:send(Socket, Pubrel),
            process_data(Rest, TriggerPublish, State#state{parser_state=emqtt_frame:initial_state()});
        {ok, #mqtt_frame{
                fixed=#mqtt_frame_fixed{type=?PUBCOMP}}, Rest} ->
            NewPubCounters = vmq_bench_stats:incr_counters(1, 0, nil, PubCounters),
            process_data(Rest, TriggerPublish, State#state{parser_state=emqtt_frame:initial_state(),
                                                           pub_counters=NewPubCounters})
    end.

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
terminate(shutdown, _) ->
    folsom_metrics:notify({nr_of_publishers, {dec, 1}}),
    folsom_metrics:notify({nr_of_consumers, {dec, 1}}),
    ok;

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
next_mid(65535) -> 1;
next_mid(Mid) -> Mid + 1.

