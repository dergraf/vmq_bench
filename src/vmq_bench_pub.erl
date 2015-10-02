-module(vmq_bench_pub).

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
                publish_confirm,
                wait_for_start,
                msgs_per_step,
                next_mid=1,
                counters=vmq_bench_stats:init_counters(pub)}).

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
    Payload =
    case proplists:get_value(payload, Config) of
        undefined -> "test-message";
        {fixed_size, Size} when is_integer(Size) ->
            crypto:rand_bytes(Size);
        P when is_list(P) -> P
    end,
    PublishOpts = proplists:get_value(publish_opts, Config, []),
    PayloadGenerator =
    case is_list(Payload) of
        true ->
            case {lists:keyfind(max, 1, Payload),
                  lists:keyfind(min, 1, Payload)} of
                {false, false} ->
                    Bb = list_to_binary(Payload),
                    fun() -> Bb end;
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
            end;
        false when is_binary(Payload) ->
            fun() -> Payload end
    end,
    PublishConfirm = proplists:get_value(publish_confirm, Config, false),
    WaitForPublishStart = proplists:get_value(wait_for_start, Config, false),
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
                wait_for_start=WaitForPublishStart,
                publish_confirm=PublishConfirm,
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
handle_info(connect, #state{socket=undefined} = State) ->
    #state{client_id=ClientId,
           host=Host,
           port=Port,
           interval=Interval,
           wait_for_start=WaitForPublishStart,
           connect_opts=ConnectOpts} = State,
    {A,B,C} = now(),
    random:seed(A, B, C),

    Connect = packet:gen_connect(ClientId, ConnectOpts),
    Connack = packet:gen_connack(),
    case packet:do_client_connect(Connect, Connack,
                                  [{hostname, Host}, {port, Port}]) of
        {ok, Socket} ->
            {ok, BufSizes} = inet:getopts(Socket, [sndbuf, recbuf, buffer]),
            BufSize = lists:max([Sz || {_, Sz} <- BufSizes]),
            inet:setopts(Socket, [{buffer, BufSize}]),
            folsom_metrics:notify({nr_of_publishers, {inc, 1}}),
            case WaitForPublishStart of
                true -> ignore;
                false ->
                    erlang:send_after(Interval + random:uniform(500), self(), publish)
            end,
            {noreply, State#state{socket=Socket}};
        {error, _} ->
            %% we retry in 1 second
            erlang:send_after(1000, self(), connect),
            {noreply, State}
    end;
handle_info(publish_start, State) ->
    erlang:send_after(random:uniform(1000), self(), publish),
    {noreply, State};
handle_info(publish, #state{socket=Socket} = State) ->
    #state{interval=Interval,
           topic=Topic,
           payload_generator=Generator,
           publish_opts=PublishOpts,
           msgs_per_step=MsgsPerStep,
           next_mid=Mid,
           publish_confirm=PublishConfirm,
           qos=QoS,
           socket=Socket,
           counters=Counters} = State,
    T1 = os:timestamp(),
    {NextMid, Mids, Frame} = stuff_the_frame(MsgsPerStep, PublishConfirm, Topic, QoS,
                                             Mid, Generator, PublishOpts),

    case gen_tcp:send(Socket, Frame) of
        ok ->
            L = iolist_size(Frame),
            NewCounters = await_acks(Socket, QoS, Mids, L, Counters),
            T2 = os:timestamp(),
            TimeDif = timer:now_diff(T2, T1) div 1000,
            case {PublishConfirm, TimeDif > Interval} of
                {true, _} ->
                    ignore;
                {false, true} ->
                    % broker is very saturated
                    erlang:send_after(Interval, self(), publish);
                {false, false} ->
                    % correct Interval
                    erlang:send_after(Interval - TimeDif, self(), publish)
            end,
            {noreply, State#state{next_mid=NextMid,
                                  counters=NewCounters}};
        {error, closed} ->
            folsom_metrics:notify({nr_of_publishers, {dec, 1}}),
            erlang:send_after(1000, self(), connect),
            {noreply, State#state{socket=undefined,
                                  next_mid=1,
                                  counters=vmq_bench_stats:init_counters(pub)}}
    end;
handle_info(publish_confirm, #state{interval=Interval} = State) ->
    case Interval of
        0 ->
            handle_info(publish, State);
        _ ->
            erlang:send_after(Interval, self(), publish),
            {noreply, State}
    end;

handle_info(stop_now, State) ->
    folsom_metrics:notify({nr_of_publishers, {dec, 1}}),
    {stop, normal, State}.

stuff_the_frame(MsgsPerStep, PublishConfirm, Topic, QoS, Mid, Generator, PublishOpts) ->
    stuff_the_frame(MsgsPerStep, PublishConfirm, Topic, QoS, Mid, Generator, PublishOpts, [], []).

stuff_the_frame(0, _, _, _, Mid, _, _, Mids, Buf) ->
    {Mid, lists:reverse(Mids), lists:reverse(Buf)};
stuff_the_frame(MsgsPerStep, PublishConfirm, Topic, QoS, Mid, Generator, PublishOpts, Mids, Buf) ->
    Payload = case PublishConfirm of
                  true ->
                        term_to_binary({os:timestamp(), self(), Generator()});
                  false ->
                        term_to_binary({os:timestamp(), Generator()})
              end,
    Publish = packet:gen_publish(Topic, QoS, Payload,
                                 [{mid, Mid} | PublishOpts]),
    NewBuf = [Publish|Buf],
    stuff_the_frame(MsgsPerStep - 1, PublishConfirm, Topic, QoS, next_mid(Mid), Generator,
                    PublishOpts, [Mid|Mids], NewBuf).

await_acks(_, 0, Mids, L, Counters) ->
    vmq_bench_stats:incr_counters(length(Mids), L, nil, Counters);
await_acks(Socket, 1, [Mid|Mids], L, Counters) ->
    Puback = packet:gen_puback(Mid),
    ok = packet:expect_packet(Socket, "puback", Puback),
    NewCounters = vmq_bench_stats:incr_counters(1, byte_size(Puback), nil, Counters),
    await_acks(Socket, 1, Mids, L, NewCounters);
await_acks(_, 1, [], L, Counters) ->
    vmq_bench_stats:incr_counters(0, L, nil, Counters);
await_acks(Socket, 2, [Mid|Mids], L, Counters) ->
    Pubrec = packet:gen_pubrec(Mid),
    ok = packet:expect_packet(Socket, "pubrec", Pubrec),
    Pubrel = packet:gen_pubrel(Mid),
    ok = gen_tcp:send(Socket, Pubrel),
    Pubcomp = packet:gen_pubcomp(Mid),
    ok = packet:expect_packet(Socket, "pubcomp", Pubcomp),
    NewCounters = vmq_bench_stats:incr_counters(1, byte_size(Pubrel), nil, Counters),
    await_acks(Socket, 2, Mids, L, NewCounters);
await_acks(_, 2, [], L, Counters) ->
    vmq_bench_stats:incr_counters(0, L, nil, Counters).



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
