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
%% 「容量判断 + 入队」，永不阻塞：
%%   * 队列长度/字节数都有上限，超限直接丢弃并按原因计数；
%%   * 按 device_id 哈希把消息分片到多个 worker，保序且吞吐可线性扩展；
%%   * 熔断只决定「发不发 Kafka」，不决定「收不收消息」：熔断期间消息在 worker
%%     本地 park，恢复后按 FIFO 补发；
%%   * 探测改为每个 worker 每次只放行 probe_per_worker 条，避免探测期全量灌入。
%%
%% 本进程只处理统计、熔断和广播，不参与消息转发，因此不会成为吞吐瓶颈。
-module(emqx_plugin_kafka_queue).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([ start_link/0
        , push/3
        , full_reason/0
        , full/0
        , drop/1
        , stats/0
        , accepting/0
        , release/2
        , release_batch/3
        , breaker_state/0
        , breaker_action/0
        , produce_timeout/0
        , fail_threshold/0
        , probe_per_worker/0
        , batch_max_count/0
        , batch_max_bytes/0
        , worker_names/0
        , report_failure/2
        , report_success/0
        , note_buffered/0
        , note_buffered/1
        , note_acked/0
        , note_acked/1
        , note_late_buffered/1
        , note_failed/1
        , note_failed/2
        , note_producer_down/0
        ]).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(TAB, emqx_plugin_kafka_queue_tab).

%% 容量计数合并成一行 {queue_usage, Count, Bytes}，准入判断只需一次 lookup
-define(QUEUE, queue_usage).
%% 每个 worker 名下已预占但尚未释放的额度，worker 重启时用于回收
-define(OUTSTANDING, outstanding).

-define(BUFFERED, buffered).
-define(ACKED, acked).
%% 超时后才收到 buffered 回执的消息数：额度已提前归还，但对账口径需要单列
-define(LATE_BUFFERED, late_buffered).
-define(FAILED, failed).
-define(FAILED_TIMEOUT, failed_timeout).
-define(FAILED_PRODUCER_DOWN, failed_producer_down).
-define(FAILED_GET_PRODUCER, failed_get_producer).
-define(FAILED_OTHER, failed_other).

-define(DROPPED, dropped_total).
-define(DROP_LEN, dropped_full_len).
-define(DROP_BYTES, dropped_full_bytes).
-define(DROP_UNAVAIL, dropped_unavailable).
-define(DROP_PROBE, dropped_probe).
-define(DROP_BREAKER, dropped_breaker_open).

-define(BREAKER_CNT, breaker_open_count).
-define(BREAKER_MS, breaker_open_ms).
-define(PRODUCER_DOWN, producer_down).

-define(DEFAULT_WORKERS, 4).
-define(DEFAULT_MAX_LEN, 100000).
-define(DEFAULT_MAX_BYTES, 134217728).  %% 128MB
-define(DEFAULT_REPORT_INTERVAL, 5000).
-define(DEFAULT_PRODUCE_TIMEOUT, 2000).
-define(DEFAULT_FAIL_THRESHOLD, 3).
-define(DEFAULT_PROBE_INTERVAL, 10000).
-define(DEFAULT_PROBE_PER_WORKER, 1).
-define(DEFAULT_BATCH_MAX_COUNT, 200).
%% 单批字节上限必须明显小于 default_producer_config 的 max_batch_size，
%% 否则「值字节数」贴着上限时，加上批次头/key/offset 开销就会超限并触发
%% RecordTooLargeException，producer 会直接崩溃重启。
-define(DEFAULT_BATCH_MAX_BYTES, 131072).
-define(DEFAULT_BREAKER_ACTION, drop).

-record(state, {report_interval = ?DEFAULT_REPORT_INTERVAL :: pos_integer(),
                last_dropped = 0 :: non_neg_integer(),
                last_failed = 0 :: non_neg_integer(),
                last_error = undefined :: term(),
                breaker = closed :: closed | probing | open,
                open_at = undefined :: integer() | undefined,
                probe_tref :: reference() | undefined,
                probe_timeout_tref :: reference() | undefined}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc 非阻塞入队：按 Key 哈希选 worker，先预占额度再投递。
%% worker 不在时立即归还额度，避免额度永久泄漏。
-spec push(binary(), binary(), binary()) -> ok | {error, term()}.
push(Topic, Key, Payload) ->
    Size = byte_size(Payload),
    Worker = worker(Key),
    case reserve(Size, Worker) of
        ok ->
            %% gen_server:cast 会吞掉「目标未注册」的 badarg，额度就会永久泄漏；
            %% 这里显式 send 并捕获异常，worker 不在时立即归还额度。
            try erlang:send(Worker, {'$gen_cast', {produce, Topic, Key, Payload}}) of
                _ -> ok
            catch
                _:_ ->
                    release(Size, Worker),
                    drop(unavailable),
                    {error, unavailable}
            end;
        {drop, Reason} ->
            drop(Reason),
            {error, Reason}
    end.

%% @doc 准入判断：容量 + 熔断动作。
%% 熔断打开时是否接收新消息由 breaker_action 决定：
%%   drop（默认）：直接丢弃，避免恢复后补发一批已经过期的心跳/事件；
%%   park        ：照常入队，由 worker 本地 park，恢复后按序补发。
%% 熔断状态与容量计数放在同一行 ETS，准入只做一次 lookup。
-spec full_reason() -> ok | {drop, full_len | full_bytes | breaker_open | unavailable}.
full_reason() ->
    try ets:lookup(?TAB, ?QUEUE) of
        [{?QUEUE, N, B, Breaker}] -> check_limits(N, B, Breaker)
    catch
        error:badarg -> {drop, unavailable}
    end.

check_limits(N, B, Breaker) ->
    MaxLen = max_len(),
    MaxBytes = max_bytes(),
    if
        N >= MaxLen   -> {drop, full_len};
        B >= MaxBytes -> {drop, full_bytes};
        true          -> check_breaker(Breaker)
    end.

check_breaker(closed) ->
    ok;
check_breaker(_Breaker) ->
    case breaker_action() of
        drop -> {drop, breaker_open};
        park -> ok
    end.

-spec full() -> boolean().
full() ->
    full_reason() =/= ok.

%% 仅用于统计/兼容；准入已不再依赖熔断状态
-spec accepting() -> boolean().
accepting() ->
    breaker_state() =:= closed.

-spec breaker_state() -> closed | probing | open | unknown.
breaker_state() ->
    try ets:lookup_element(?TAB, ?QUEUE, 4)
    catch error:badarg -> unknown
    end.

-spec drop(full_len | full_bytes | unavailable | probe | breaker_open | term()) -> ok.
drop(Reason) ->
    incr(?DROPPED),
    incr(drop_counter(Reason)),
    ok.

-spec stats() -> map().
stats() ->
    {Queued, QueuedBytes} =
        try ets:lookup(?TAB, ?QUEUE) of
            [{?QUEUE, N, B, _Breaker}] -> {N, B};
            [] -> {0, 0}
        catch
            error:badarg -> {0, 0}
        end,
    Buffered = counter(?BUFFERED),
    Acked = counter(?ACKED),
    Failed = counter(?FAILED),
    LateBuffered = counter(?LATE_BUFFERED),
    #{queued => Queued,
      queued_bytes => QueuedBytes,
      avg_payload_bytes => QueuedBytes div max(1, Queued),
      buffered => Buffered,
      acked => Acked,
      late_buffered => LateBuffered,
      inflight => max(0, Buffered - Acked),
      failed => Failed,
      %% 提交失败且迟到回执始终没到 = 真实丢失。对账口径：
      %% 入队总量 = queued + buffered + lost，入队总量 + dropped = 上游总量
      lost => max(0, Failed - LateBuffered),
      failed_timeout => counter(?FAILED_TIMEOUT),
      failed_producer_down => counter(?FAILED_PRODUCER_DOWN),
      failed_get_producer => counter(?FAILED_GET_PRODUCER),
      failed_other => counter(?FAILED_OTHER),
      producer_down => counter(?PRODUCER_DOWN),
      dropped => counter(?DROPPED),
      dropped_full_len => counter(?DROP_LEN),
      dropped_full_bytes => counter(?DROP_BYTES),
      dropped_unavailable => counter(?DROP_UNAVAIL),
      dropped_probe => counter(?DROP_PROBE),
      dropped_breaker_open => counter(?DROP_BREAKER),
      breaker => breaker_state(),
      breaker_action => breaker_action(),
      breaker_open_count => counter(?BREAKER_CNT),
      breaker_open_ms => counter(?BREAKER_MS),
      workers => pool_size(),
      max_len => max_len(),
      max_bytes => max_bytes(),
      batch_max_count => batch_max_count(),
      batch_max_bytes => batch_max_bytes(),
      required_acks => required_acks()}.

%%--------------------------------------------------------------------
%% Called by workers / hooks
%%--------------------------------------------------------------------

%% 同时归还全局额度和该 worker 名下的在途额度
-spec release(non_neg_integer(), atom()) -> ok.
release(Size, Worker) ->
    release_batch(1, Size, Worker).

%% 批量提交时一组只做一次额度归还，把每条的 ETS 更新摊薄到每组一次
-spec release_batch(non_neg_integer(), non_neg_integer(), atom()) -> ok.
release_batch(Count, Bytes, Worker) when Count > 0 ->
    %% Threshold/SetValue 形式保证协调进程重启、计数表重建后不会出现负数
    catch ets:update_counter(?TAB, ?QUEUE, [{2, -Count, 0, 0}, {3, -Bytes, 0, 0}]),
    catch ets:update_counter(?TAB, {?OUTSTANDING, Worker}, [{2, -Count, 0, 0}, {3, -Bytes, 0, 0}]),
    ok;
release_batch(_Count, _Bytes, _Worker) ->
    ok.

note_buffered() ->
    note_buffered(1).

note_buffered(N) ->
    incr(?BUFFERED, N).

note_acked() ->
    note_acked(1).

%% 一个 produce req 只回一次 acked，批量模式下按组内条数累加
note_acked(N) ->
    incr(?ACKED, N).

note_late_buffered(N) ->
    incr(?LATE_BUFFERED, N).

note_failed(Kind) ->
    note_failed(Kind, 1).

note_failed(Kind, N) ->
    incr(?FAILED, N),
    incr(failed_counter(Kind), N).

note_producer_down() ->
    incr(?PRODUCER_DOWN).

report_failure(Streak, Reason) ->
    catch gen_server:cast(?MODULE, {produce_failed, self(), Streak, Reason}),
    ok.

report_success() ->
    catch gen_server:cast(?MODULE, {produce_ok, self()}),
    ok.

-spec produce_timeout() -> pos_integer().
produce_timeout() ->
    persistent_term:get({?MODULE, produce_timeout}, ?DEFAULT_PRODUCE_TIMEOUT).

-spec fail_threshold() -> pos_integer().
fail_threshold() ->
    persistent_term:get({?MODULE, fail_threshold}, ?DEFAULT_FAIL_THRESHOLD).

-spec probe_per_worker() -> pos_integer().
probe_per_worker() ->
    persistent_term:get({?MODULE, probe_per_worker}, ?DEFAULT_PROBE_PER_WORKER).

%% 单次提交到 Kafka 的最大条数/字节数，worker 按 {Topic, Partition} 分组后一组一次提交
-spec batch_max_count() -> pos_integer().
batch_max_count() ->
    persistent_term:get({?MODULE, batch_max_count}, ?DEFAULT_BATCH_MAX_COUNT).

-spec batch_max_bytes() -> pos_integer().
batch_max_bytes() ->
    persistent_term:get({?MODULE, batch_max_bytes}, ?DEFAULT_BATCH_MAX_BYTES).

-spec breaker_action() -> drop | park.
breaker_action() ->
    persistent_term:get({?MODULE, breaker_action}, ?DEFAULT_BREAKER_ACTION).

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
    Names = worker_names(),
    persistent_term:put({?MODULE, workers}, Names),
    persistent_term:put({?MODULE, max_len}, env(queue_max_len, ?DEFAULT_MAX_LEN)),
    persistent_term:put({?MODULE, max_bytes}, env(queue_max_bytes, ?DEFAULT_MAX_BYTES)),
    persistent_term:put({?MODULE, produce_timeout}, env(produce_timeout, ?DEFAULT_PRODUCE_TIMEOUT)),
    persistent_term:put({?MODULE, fail_threshold}, env(fail_threshold, ?DEFAULT_FAIL_THRESHOLD)),
    persistent_term:put({?MODULE, probe_interval}, env(probe_interval, ?DEFAULT_PROBE_INTERVAL)),
    persistent_term:put({?MODULE, probe_per_worker},
                        env(probe_per_worker, ?DEFAULT_PROBE_PER_WORKER)),
    persistent_term:put({?MODULE, batch_max_count},
                        env(batch_max_count, ?DEFAULT_BATCH_MAX_COUNT)),
    persistent_term:put({?MODULE, batch_max_bytes},
                        max(1, env(batch_max_bytes, ?DEFAULT_BATCH_MAX_BYTES))),
    persistent_term:put({?MODULE, breaker_action},
                        normalize_breaker_action(env(breaker_action, ?DEFAULT_BREAKER_ACTION))),
    ets:insert(?TAB, [{?QUEUE, 0, 0, closed},
                      {?BUFFERED, 0}, {?ACKED, 0}, {?LATE_BUFFERED, 0}, {?FAILED, 0},
                      {?FAILED_TIMEOUT, 0}, {?FAILED_PRODUCER_DOWN, 0},
                      {?FAILED_GET_PRODUCER, 0}, {?FAILED_OTHER, 0},
                      {?DROPPED, 0}, {?DROP_LEN, 0}, {?DROP_BYTES, 0},
                      {?DROP_UNAVAIL, 0}, {?DROP_PROBE, 0}, {?DROP_BREAKER, 0},
                      {?BREAKER_CNT, 0}, {?BREAKER_MS, 0},
                      {?PRODUCER_DOWN, 0}
                      | [{{?OUTSTANDING, N}, 0, 0} || N <- tuple_to_list(Names)]]),
    %% 本进程重启会重建 ETS、计数归零，worker 的 pending 却仍占真实内存，
    %% 让它们清空 pending 且不做 release（计数已归零）。
    broadcast(reset),
    Interval = env(queue_report_interval, ?DEFAULT_REPORT_INTERVAL),
    erlang:send_after(Interval, self(), report),
    ?LOG_NOTICE("[KAFKA PLUGIN]queue started, workers=~p, max_len=~p, max_bytes=~p, "
                "produce_timeout=~p, fail_threshold=~p, probe_interval=~p, "
                "probe_per_worker=~p, breaker_action=~p, batch_max_count=~p, "
                "batch_max_bytes=~p",
                [pool_size(), max_len(), max_bytes(), produce_timeout(),
                 fail_threshold(), probe_interval(), probe_per_worker(),
                 breaker_action(), batch_max_count(), batch_max_bytes()]),
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
    {noreply, close_breaker(State)};
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
            ?LOG_WARNING("[KAFKA PLUGIN]dropped ~p messages in last ~pms "
                         "(total ~p: full_len ~p, full_bytes ~p, unavailable ~p, "
                         "probe ~p, breaker_open ~p), "
                         "still queued ~p msgs / ~p bytes, breaker ~p, inflight ~p",
                         [Dropped - LastDropped, Interval, Dropped,
                          counter(?DROP_LEN), counter(?DROP_BYTES),
                          counter(?DROP_UNAVAIL), counter(?DROP_PROBE),
                          counter(?DROP_BREAKER),
                          queued(), queued_bytes(), breaker_state(), inflight()]);
        false ->
            ok
    end,
    case Failed > LastFailed of
        true ->
            ?LOG_WARNING("[KAFKA PLUGIN]produce to kafka failed ~p times in last ~pms "
                         "(total ~p: timeout ~p, producer_down ~p, get_producer ~p), "
                         "last error ~p",
                         [Failed - LastFailed, Interval, Failed,
                          counter(?FAILED_TIMEOUT), counter(?FAILED_PRODUCER_DOWN),
                          counter(?FAILED_GET_PRODUCER), LastError]);
        false ->
            ok
    end,
    erlang:send_after(Interval, self(), report),
    {noreply, State#state{last_dropped = Dropped, last_failed = Failed}};
handle_info(probe, State = #state{breaker = open}) ->
    set_breaker(probing),
    %% 只让每个 worker 从自己 pending 头部放行少量真实消息做探测，
    %% 避免「熔断恢复瞬间全量放开」把积压一次性灌给 broker。
    broadcast({probe, probe_per_worker()}),
    %% 若探测窗口内没有任何 worker 上报结果（典型情况：pending 为空），
    %% 必须有保底超时把状态回退到 open 并重调度，否则会永久停在 probing。
    TimeoutTRef = erlang:send_after(probe_interval(), self(), probe_timeout),
    ?LOG_DEBUG("[KAFKA PLUGIN]breaker half-open, probe ~p msg per worker",
               [probe_per_worker()]),
    {noreply, State#state{breaker = probing, probe_tref = undefined,
                          probe_timeout_tref = TimeoutTRef}};
handle_info(probe, State) ->
    {noreply, State};
handle_info(probe_timeout, State = #state{breaker = probing}) ->
    set_breaker(open),
    TRef = erlang:send_after(probe_interval(), self(), probe),
    ?LOG_DEBUG("[KAFKA PLUGIN]probe window expired without result, retry probe"),
    {noreply, State#state{breaker = open, probe_tref = TRef,
                          probe_timeout_tref = undefined}};
handle_info(probe_timeout, State) ->
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
    cancel_timer(State#state.probe_timeout_tref),
    TRef = erlang:send_after(probe_interval(), self(), probe),
    WasClosed = State#state.breaker =:= closed,
    set_breaker(open),
    %% 只有 closed -> open 才算一次新的熔断；探测失败重开是同一轮故障
    case WasClosed of
        true -> incr(?BREAKER_CNT);
        false -> ok
    end,
    OpenAt = case State#state.open_at of
                 undefined -> now_ms();
                 Ts -> Ts
             end,
    case WasClosed of
        true ->
            ?LOG_ERROR("[KAFKA PLUGIN]kafka unavailable, breaker open for ~pms, "
                       "last error: ~p", [probe_interval(), Reason]);
        false ->
            ?LOG_DEBUG("[KAFKA PLUGIN]probe failed, breaker reopen, last error: ~p",
                       [Reason])
    end,
    State#state{breaker = open, probe_tref = TRef, probe_timeout_tref = undefined,
                open_at = OpenAt, last_error = Reason}.

close_breaker(State = #state{open_at = OpenAt}) ->
    cancel_timer(State#state.probe_tref),
    cancel_timer(State#state.probe_timeout_tref),
    case OpenAt of
        undefined -> ok;
        _ -> incr(?BREAKER_MS, max(0, now_ms() - OpenAt))
    end,
    set_breaker(closed),
    broadcast(drain),
    ?LOG_NOTICE("[KAFKA PLUGIN]kafka recovered, breaker closed, draining parked messages"),
    State#state{breaker = closed, probe_tref = undefined, probe_timeout_tref = undefined,
                open_at = undefined}.

cancel_timer(undefined) ->
    ok;
cancel_timer(TRef) ->
    _ = erlang:cancel_timer(TRef),
    ok.

set_breaker(StateName) ->
    %% 只改第 4 个字段，不能用 insert 覆盖整行，否则会丢掉并发中的容量计数
    catch ets:update_element(?TAB, ?QUEUE, {4, StateName}),
    ok.

broadcast(Msg) ->
    Names = persistent_term:get({?MODULE, workers}, {}),
    lists:foreach(fun(N) -> catch erlang:send(N, {'$gen_cast', Msg}) end,
                  tuple_to_list(Names)).

%% 先占额度再入队，保证 worker 邮箱总量不会超过 queue_max_len。
reserve(Size, Worker) ->
    try
        [N, B] = ets:update_counter(?TAB, ?QUEUE, [{2, 1}, {3, Size}]),
        case N =< max_len() andalso B =< max_bytes() of
            true ->
                catch ets:update_counter(?TAB, {?OUTSTANDING, Worker}, [{2, 1}, {3, Size}]),
                ok;
            false ->
                ets:update_counter(?TAB, ?QUEUE, [{2, -1}, {3, -Size}]),
                {drop, drop_reason(N, B)}
        end
    catch
        error:badarg -> {drop, unavailable}
    end.

drop_reason(N, B) ->
    MaxLen = max_len(),
    MaxBytes = max_bytes(),
    if
        N > MaxLen   -> full_len;
        B > MaxBytes -> full_bytes;
        true         -> full_bytes
    end.

drop_counter(full_len)   -> ?DROP_LEN;
drop_counter(full_bytes) -> ?DROP_BYTES;
drop_counter(probe)      -> ?DROP_PROBE;
drop_counter(breaker_open) -> ?DROP_BREAKER;
drop_counter(_)          -> ?DROP_UNAVAIL.

failed_counter(timeout)       -> ?FAILED_TIMEOUT;
failed_counter(producer_down) -> ?FAILED_PRODUCER_DOWN;
failed_counter(get_producer)  -> ?FAILED_GET_PRODUCER;
failed_counter(_)             -> ?FAILED_OTHER.

queued() ->
    try ets:lookup_element(?TAB, ?QUEUE, 2)
    catch error:badarg -> 0
    end.

queued_bytes() ->
    try ets:lookup_element(?TAB, ?QUEUE, 3)
    catch error:badarg -> 0
    end.

inflight() ->
    max(0, counter(?BUFFERED) - counter(?ACKED)).

counter(Key) ->
    try ets:lookup_element(?TAB, Key, 2)
    catch error:badarg -> 0
    end.

incr(Key) ->
    incr(Key, 1).

incr(Key, N) ->
    catch ets:update_counter(?TAB, Key, N),
    ok.

max_len() ->
    persistent_term:get({?MODULE, max_len}, ?DEFAULT_MAX_LEN).

max_bytes() ->
    persistent_term:get({?MODULE, max_bytes}, ?DEFAULT_MAX_BYTES).

probe_interval() ->
    persistent_term:get({?MODULE, probe_interval}, ?DEFAULT_PROBE_INTERVAL).

pool_size() ->
    max(1, env(queue_workers, ?DEFAULT_WORKERS)).

required_acks() ->
    try
        {ok, Config} = application:get_env(emqx_plugin_kafka, kafka_config),
        case lists:keyfind(default_producer_config, 1, Config) of
            {default_producer_config, ProducerCfg} ->
                proplists:get_value(required_acks, ProducerCfg, 1);
            false ->
                1
        end
    catch
        _:_ -> 1
    end.

now_ms() ->
    erlang:monotonic_time(millisecond).

%% 配置写错时不能静默变成 park：未知取值直接按默认 drop 处理并告警
normalize_breaker_action(drop) -> drop;
normalize_breaker_action(park) -> park;
normalize_breaker_action(Other) ->
    ?LOG_WARNING("[KAFKA PLUGIN]invalid breaker_action ~p, fallback to ~p",
                 [Other, ?DEFAULT_BREAKER_ACTION]),
    ?DEFAULT_BREAKER_ACTION.

env(Key, Default) ->
    application:get_env(emqx_plugin_kafka, Key, Default).
