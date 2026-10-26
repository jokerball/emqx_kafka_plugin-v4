%%--------------------------------------------------------------------
%% Copyright (c) 2015-2017 Feng Lee <feng@emqtt.io>.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc Kafka 异步有界队列协调进程。
%%
%% EMQX 的 hook 都在客户端连接进程里同步执行，直接调用 brod 的 produce 接口会在
%% Kafka 抖动时把这些进程以及 brod 生产者的邮箱拖成无界增长。这里把写 Kafka 的
%% 动作收敛到一组 worker 进程（emqx_plugin_kafka_worker），hook 侧只做
%% 「熔断状态 + 队列额度」判断和 cast，永不阻塞：
%%   * 队列长度/字节数都有上限，超限直接丢弃并计数；
%%   * 按 device_id 哈希把消息分片到多个 worker，保序且吞吐可线性扩展；
%%   * 熔断由本进程统一裁决：任一 worker 连续失败达到阈值即打开，期间 hook 直接
%%     丢弃、worker 也快速丢弃已入队消息；探测成功自动恢复。
%%
%% 本进程只处理统计、熔断和探测，不参与消息转发，因此不会成为吞吐瓶颈。
-module(emqx_plugin_kafka_queue).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([ start_link/0
        , push/3
        , full/0
        , drop/0
        , stats/0
        , accepting/0
        , release/1
        , breaker_state/0
        , produce_timeout/0
        , worker_names/0
        , report_failure/2
        , report_success/0
        , note_buffered/0
        , note_acked/0
        , note_failed/0
        ]).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(TAB, emqx_plugin_kafka_queue_tab).
-define(COUNT, queue_count).
-define(BYTES, queue_bytes).
-define(BUFFERED, buffered).
-define(ACKED, acked).
-define(FAILED, failed).
-define(DROPPED, dropped).
-define(ACCEPTING, accepting).
-define(BREAKER, breaker).

-define(DEFAULT_WORKERS, 4).
-define(DEFAULT_MAX_LEN, 100000).
-define(DEFAULT_MAX_BYTES, 134217728).  %% 128MB
-define(DEFAULT_REPORT_INTERVAL, 5000).
-define(DEFAULT_PRODUCE_TIMEOUT, 2000).
-define(DEFAULT_FAIL_THRESHOLD, 3).
-define(DEFAULT_PROBE_INTERVAL, 10000).

-record(state, {report_interval = ?DEFAULT_REPORT_INTERVAL :: pos_integer(),
                last_dropped = 0 :: non_neg_integer(),
                last_failed = 0 :: non_neg_integer(),
                last_error = undefined :: term(),
                breaker = closed :: closed | probing | open,
                probe_tref :: reference() | undefined}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc 非阻塞入队：按 Key 哈希选 worker，worker 未就绪或队列满时直接丢弃。
-spec push(binary(), binary(), binary()) -> ok | {error, full | unavailable}.
push(Topic, Key, Payload) ->
    Size = byte_size(Payload),
    case reserve(Size) of
        ok ->
            %% gen_server:cast 会吞掉「目标未注册」的 badarg，额度就会永久泄漏；
            %% 这里显式 send 并捕获异常，worker 不在时立即归还额度。
            try erlang:send(worker(Key), {'$gen_cast', {produce, Topic, Key, Payload}}) of
                _ -> ok
            catch
                _:_ ->
                    release(Size),
                    drop(),
                    {error, unavailable}
            end;
        full ->
            drop(),
            {error, full};
        unavailable ->
            drop(),
            {error, unavailable}
    end.

%% @doc 供 hook 在 JSON 编码前做一次廉价预判：队列满或熔断中都不接受新消息。
-spec full() -> boolean().
full() ->
    try
        ets:lookup_element(?TAB, ?ACCEPTING, 2) =:= false
            orelse ets:lookup_element(?TAB, ?COUNT, 2) >= max_len()
            orelse ets:lookup_element(?TAB, ?BYTES, 2) >= max_bytes()
    catch
        error:badarg -> true
    end.

-spec accepting() -> boolean().
accepting() ->
    try ets:lookup_element(?TAB, ?ACCEPTING, 2)
    catch error:badarg -> false
    end.

-spec breaker_state() -> closed | probing | open | unknown.
breaker_state() ->
    try ets:lookup_element(?TAB, ?BREAKER, 2)
    catch error:badarg -> unknown
    end.

-spec drop() -> ok.
drop() ->
    incr(?DROPPED),
    ok.

-spec stats() -> map().
stats() ->
    #{queued => counter(?COUNT),
      queued_bytes => counter(?BYTES),
      buffered => counter(?BUFFERED),
      acked => counter(?ACKED),
      failed => counter(?FAILED),
      dropped => counter(?DROPPED),
      breaker => breaker_state(),
      workers => pool_size(),
      max_len => max_len(),
      max_bytes => max_bytes()}.

%%--------------------------------------------------------------------
%% Called by workers
%%--------------------------------------------------------------------

-spec release(non_neg_integer()) -> ok.
release(Size) ->
    %% Threshold/SetValue 形式保证协调进程重启、计数表重建后不会出现负数
    catch ets:update_counter(?TAB, ?COUNT, {2, -1, 0, 0}),
    catch ets:update_counter(?TAB, ?BYTES, {2, -Size, 0, 0}),
    ok.

note_buffered() ->
    incr(?BUFFERED).

note_acked() ->
    incr(?ACKED).

note_failed() ->
    incr(?FAILED).

report_failure(Streak, Reason) ->
    catch gen_server:cast(?MODULE, {produce_failed, self(), Streak, Reason}),
    ok.

report_success() ->
    catch gen_server:cast(?MODULE, {produce_ok, self()}),
    ok.

-spec produce_timeout() -> pos_integer().
produce_timeout() ->
    persistent_term:get({?MODULE, produce_timeout}, ?DEFAULT_PRODUCE_TIMEOUT).

-spec worker_names() -> tuple().
worker_names() ->
    list_to_tuple([list_to_atom("emqx_plugin_kafka_queue_" ++ integer_to_list(I))
                   || I <- lists:seq(1, pool_size())]).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    catch ets:delete(?TAB),
    ets:new(?TAB, [named_table, public, set,
                   {read_concurrency, true}, {write_concurrency, true}]),
    ets:insert(?TAB, [{?COUNT, 0}, {?BYTES, 0}, {?BUFFERED, 0}, {?ACKED, 0},
                      {?FAILED, 0}, {?DROPPED, 0}, {?ACCEPTING, true},
                      {?BREAKER, closed}]),
    persistent_term:put({?MODULE, workers}, worker_names()),
    persistent_term:put({?MODULE, max_len}, env(queue_max_len, ?DEFAULT_MAX_LEN)),
    persistent_term:put({?MODULE, max_bytes}, env(queue_max_bytes, ?DEFAULT_MAX_BYTES)),
    persistent_term:put({?MODULE, produce_timeout}, env(produce_timeout, ?DEFAULT_PRODUCE_TIMEOUT)),
    persistent_term:put({?MODULE, fail_threshold}, env(fail_threshold, ?DEFAULT_FAIL_THRESHOLD)),
    persistent_term:put({?MODULE, probe_interval}, env(probe_interval, ?DEFAULT_PROBE_INTERVAL)),
    Interval = env(queue_report_interval, ?DEFAULT_REPORT_INTERVAL),
    erlang:send_after(Interval, self(), report),
    ?LOG_NOTICE("[KAFKA PLUGIN]queue started, workers=~p, max_len=~p, max_bytes=~p, "
                "produce_timeout=~p, fail_threshold=~p, probe_interval=~p",
                [pool_size(), max_len(), max_bytes(), produce_timeout(),
                 fail_threshold(), probe_interval()]),
    {ok, #state{report_interval = Interval}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({produce_failed, _Worker, Streak, Reason}, State = #state{breaker = closed}) ->
    State1 = State#state{last_error = Reason},
    case Streak >= fail_threshold() of
        true -> {noreply, open_breaker(Reason, State1)};
        false -> {noreply, State1}
    end;
handle_cast({produce_failed, _Worker, _Streak, Reason}, State = #state{breaker = probing}) ->
    %% 探测失败立即重新打开，不再等累积到阈值
    {noreply, open_breaker(Reason, State)};
handle_cast({produce_failed, _Worker, _Streak, _Reason}, State) ->
    {noreply, State};
handle_cast({produce_ok, _Worker}, State = #state{breaker = probing}) ->
    set_breaker(closed),
    set_accepting(true),
    ?LOG_NOTICE("[KAFKA PLUGIN]kafka recovered, breaker closed"),
    {noreply, State#state{breaker = closed, probe_tref = undefined}};
handle_cast({produce_ok, _Worker}, State) ->
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(report, State = #state{report_interval = Interval,
                                   last_dropped = LastDropped,
                                   last_failed = LastFailed,
                                   last_error = LastError}) ->
    Dropped = counter(?DROPPED),
    Failed = counter(?FAILED),
    case Dropped > LastDropped of
        true ->
            ?LOG_WARNING("[KAFKA PLUGIN]queue overflow or breaker open, dropped ~p messages "
                         "in last ~pms, total dropped ~p, still queued ~p, breaker ~p",
                         [Dropped - LastDropped, Interval, Dropped, counter(?COUNT),
                          breaker_state()]);
        false ->
            ok
    end,
    case Failed > LastFailed of
        true ->
            ?LOG_WARNING("[KAFKA PLUGIN]produce to kafka failed ~p times in last ~pms, "
                         "total failed ~p, last error ~p",
                         [Failed - LastFailed, Interval, Failed, LastError]);
        false ->
            ok
    end,
    erlang:send_after(Interval, self(), report),
    {noreply, State#state{last_dropped = Dropped, last_failed = Failed}};
handle_info(probe, State = #state{breaker = open}) ->
    set_breaker(probing),
    set_accepting(true),
    ?LOG_NOTICE("[KAFKA PLUGIN]breaker half-open, probing kafka"),
    {noreply, State#state{breaker = probing, probe_tref = undefined}};
handle_info(probe, State) ->
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

worker(Key) ->
    Names = persistent_term:get({?MODULE, workers}),
    element(erlang:phash2(Key, tuple_size(Names)) + 1, Names).

open_breaker(Reason, State) ->
    cancel_timer(State#state.probe_tref),
    TRef = erlang:send_after(probe_interval(), self(), probe),
    set_breaker(open),
    set_accepting(false),
    ?LOG_ERROR("[KAFKA PLUGIN]kafka unavailable, breaker open for ~pms, last error: ~p",
               [probe_interval(), Reason]),
    State#state{breaker = open, probe_tref = TRef, last_error = Reason}.

cancel_timer(undefined) ->
    ok;
cancel_timer(TRef) ->
    _ = erlang:cancel_timer(TRef),
    ok.

set_accepting(Bool) ->
    catch ets:insert(?TAB, {?ACCEPTING, Bool}),
    ok.

set_breaker(StateName) ->
    catch ets:insert(?TAB, {?BREAKER, StateName}),
    ok.

%% 先占额度再入队，保证 worker 邮箱总量不会超过 queue_max_len。
reserve(Size) ->
    try
        N = ets:update_counter(?TAB, ?COUNT, 1),
        B = ets:update_counter(?TAB, ?BYTES, Size),
        case N =< max_len() andalso B =< max_bytes() of
            true ->
                ok;
            false ->
                ets:update_counter(?TAB, ?COUNT, -1),
                ets:update_counter(?TAB, ?BYTES, -Size),
                full
        end
    catch
        error:badarg -> unavailable
    end.

counter(Key) ->
    try ets:lookup_element(?TAB, Key, 2)
    catch error:badarg -> 0
    end.

incr(Key) ->
    catch ets:update_counter(?TAB, Key, 1),
    ok.

max_len() ->
    persistent_term:get({?MODULE, max_len}, ?DEFAULT_MAX_LEN).

max_bytes() ->
    persistent_term:get({?MODULE, max_bytes}, ?DEFAULT_MAX_BYTES).

fail_threshold() ->
    persistent_term:get({?MODULE, fail_threshold}, ?DEFAULT_FAIL_THRESHOLD).

probe_interval() ->
    persistent_term:get({?MODULE, probe_interval}, ?DEFAULT_PROBE_INTERVAL).

pool_size() ->
    max(1, env(queue_workers, ?DEFAULT_WORKERS)).

env(Key, Default) ->
    application:get_env(emqx_plugin_kafka, Key, Default).
