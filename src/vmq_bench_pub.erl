-module(vmq_bench_pub).

-behaviour(gen_server).

%% API functions
-export([start_link/1]).

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
    ClientId = "vmq-pub-" ++ integer_to_list(erlang:phash2({A,B,C})),
    StartAfter = proplists:get_value(start_after, Config, 0),
    Interval = proplists:get_value(interval, Config, 1000),
    Hosts = proplists:get_value(hosts, Config, [{"localhost", 1883}]),
    ConnectOpts = proplists:get_value(connect_opts, Config, []),

    Topics = proplists:get_value(topics, Config, [{"/test/topic", 0}]),
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
    {Topic, QoS} = lists:nth(random:uniform(length(Topics)), Topics),
    {Host, Port} = lists:nth(random:uniform(length(Hosts)), Hosts),

    case proplists:get_value(stop_after, Config, 0) of
        0 -> ok;
        StopAfter when is_integer(StopAfter) ->
            erlang:send_after(StopAfter, self(), stop_now)
    end,
    {ok, #state{host=Host,
                port=Port,
                client_id=ClientId,
                connect_opts=ConnectOpts,
                publish_opts=PublishOpts,
                payload_generator=PayloadGenerator,
                topic=Topic,
                qos=QoS,
                interval=Interval}, StartAfter}.

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
handle_info(timeout, #state{socket=undefined} = State) ->
    #state{client_id=ClientId,
           host=Host,
           port=Port,
           interval=Interval,
           connect_opts=ConnectOpts} = State,

    Connect = packet:gen_connect(ClientId, ConnectOpts),
    Connack = packet:gen_connack(),
    case packet:do_client_connect(Connect, Connack,
                                  [{host, Host}, {port, Port}]) of
        {ok, Socket} ->
            {noreply, State#state{socket=Socket}, Interval};
        {error, timeout} ->
            %% we retry in 1 second
            {noreply, State, 1000}
    end;
handle_info(timeout, #state{socket=Socket} = State) ->
    #state{interval=Interval,
           topic=Topic,
           payload_generator=Generator,
           publish_opts=PublishOpts,
           next_mid=Mid,
           qos=QoS,
           socket=Socket,
           counters=Counters} = State,
    Payload = term_to_binary({os:timestamp(), Generator()}),
    Publish = packet:gen_publish(Topic, QoS, Payload,
                                 [{mid, Mid} | PublishOpts]),
    ok = gen_tcp:send(Socket, Publish),
    L = byte_size(Publish),
    NewCounters =
    case QoS of
        0 ->
            vmq_bench_stats:incr_counters(1, L, nil, Counters);
        1 ->
            Puback = packet:gen_puback(Mid),
            ok = packet:expect_packet(Socket, "puback", Puback),
            vmq_bench_stats:incr_counters(1, L + byte_size(Puback), nil, Counters);
        2 ->
            Pubrec = packet:gen_pubrec(Mid),
            ok = packet:expect_packet(Socket, "pubrec", Pubrec),
            Pubrel = packet:gen_pubrel(Mid),
            ok = gen_tcp:send(Socket, Pubrel),
            Pubcomp = packet:gen_pubcomp(Mid),
            ok = packet:expect_packet(Socket, "pubcomp", Pubcomp),
            vmq_bench_stats:incr_counters(1, L + byte_size(Pubrel), nil, Counters)
    end,
    {noreply, State#state{next_mid=next_mid(Mid),
                          counters=NewCounters}, Interval};

handle_info(stop_now, State) ->
    {stop, normal, State}.

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
next_mid(65535) -> 1;
next_mid(Mid) -> Mid + 1.