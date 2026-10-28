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
        , report_producer_down/1
        , recovery_state/0
        , recovery_info/0
        , rebuild_client/0
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

-define(RECOVERY_DEGRADE, recovery_degrade_count).
-define(REBUILD_CNT, client_rebuild_count).
-define(RECOVERY_EXHAUSTED_CNT, recovery_exhausted_count).

%% 必须与 worker 侧的 ?KAFKA_CLIENT 一致
-define(CLIENT, emqx_repost_worker).

%% client 级兜底恢复：三层健康信号 -> 五状态机 -> 退避重建。
%% 状态：healthy -> degraded -> rebuilding_client -> verifying -> healthy
%%       （重建次数在窗口内超限则进入 exhausted，只保留低频探测，靠手动 rebuild 或
%%         探测成功自动解除）
-define(DEFAULT_ACK_STALL_MS, 30000).
%% 熔断持续打开且探测始终失败时，升级为 client 重建。
%% 这是「produce 快速失败」场景唯一的触发信号：broker/client 立刻返回错误时
%% inflight 恒为 0，ack 停滞检测永远不会命中，producer 也不会 DOWN，
%% 只会看到熔断一直开合，正是压测里 27.115 无法自愈的形态。0 表示关闭该升级。
-define(DEFAULT_BREAKER_ESCALATE_MS, 120000).
-define(DEFAULT_PRODUCER_DOWN_WINDOW_MS, 60000).
-define(DEFAULT_PRODUCER_DOWN_THRESHOLD, 3).
-define(DEFAULT_REBUILD_BACKOFF_BASE_MS, 1000).
-define(DEFAULT_REBUILD_BACKOFF_MAX_MS, 30000).
-define(DEFAULT_REBUILD_WINDOW_MS, 3600000).
-define(DEFAULT_MAX_REBUILDS, 6).
-define(DEFAULT_VERIFY_MS, 10000).
-define(DEFAULT_REBUILD_TIMEOUT_MS, 15000).
%% stop_client 返回后立刻 start_client 可能拿到 already_started，留一点沉降时间
-define(REBUILD_SETTLE_MS, 200).

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
                probe_timeout_tref :: reference() | undefined,
                %% ---- client 级兜底恢复状态机 ----
                recovery = healthy :: healthy | degraded | rebuilding_client
                                     | verifying | exhausted,
                recovery_reason = undefined :: term(),
                recovery_since = undefined :: integer() | undefined,
                rebuild_attempt = 0 :: non_neg_integer(),
                rebuild_times = [] :: [integer()],
                rebuild_tref :: reference() | undefined,
                rebuild_timeout_tref :: reference() | undefined,
                rebuild_epoch = 0 :: non_neg_integer(),
                verify_tref :: reference() | undefined,
                %% ack 停滞检测：inflight > 0 且 acked 连续不增长即判定 client 半死
                last_acked = 0 :: non_neg_integer(),
                ack_stall_since = undefined :: integer() | undefined,
                producer_down_times = [] :: [integer()]}).

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
      recovery => recovery_state(),
      recovery_degrade_count => counter(?RECOVERY_DEGRADE),
      client_rebuild_count => counter(?REBUILD_CNT),
      recovery_exhausted_count => counter(?RECOVERY_EXHAUSTED_CNT),
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

%% worker 侧 producer 退出时上报。单个 producer 崩掉 brod 会自动重启，只有窗口内
%% 连续多次才说明 client 层已不可用，因此这里只做计数，由协调进程裁决是否重建。
-spec report_producer_down(term()) -> ok.
report_producer_down(Reason) ->
    catch gen_server:cast(?MODULE, {producer_down, Reason}),
    ok.

-spec recovery_state() -> healthy | degraded | rebuilding_client | verifying | exhausted | unknown.
recovery_state() ->
    maps:get(state, recovery_info(), unknown).

-spec recovery_info() -> map().
recovery_info() ->
    try gen_server:call(?MODULE, recovery_info, 2000)
    catch
        _:_ -> #{state => unknown}
    end.

%% 手动重建：绕过退避立即执行，并清空窗口内的重建预算（运维显式介入）
-spec rebuild_client() -> ok.
rebuild_client() ->
    catch gen_server:cast(?MODULE, manual_rebuild),
    ok.

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
    persistent_term:put({?MODULE, ack_stall_ms},
                        max(1, env(ack_stall_ms, ?DEFAULT_ACK_STALL_MS))),
    persistent_term:put({?MODULE, breaker_escalate_ms},
                        max(0, env(breaker_escalate_ms, ?DEFAULT_BREAKER_ESCALATE_MS))),
    persistent_term:put({?MODULE, producer_down_window_ms},
                        max(1, env(producer_down_window_ms, ?DEFAULT_PRODUCER_DOWN_WINDOW_MS))),
    persistent_term:put({?MODULE, producer_down_threshold},
                        max(1, env(producer_down_threshold, ?DEFAULT_PRODUCER_DOWN_THRESHOLD))),
    persistent_term:put({?MODULE, rebuild_backoff_base_ms},
                        max(1, env(rebuild_backoff_base_ms, ?DEFAULT_REBUILD_BACKOFF_BASE_MS))),
    persistent_term:put({?MODULE, rebuild_backoff_max_ms},
                        max(1, env(rebuild_backoff_max_ms, ?DEFAULT_REBUILD_BACKOFF_MAX_MS))),
    persistent_term:put({?MODULE, rebuild_window_ms},
                        max(1, env(rebuild_window_ms, ?DEFAULT_REBUILD_WINDOW_MS))),
    persistent_term:put({?MODULE, max_rebuilds},
                        max(1, env(max_rebuilds, ?DEFAULT_MAX_REBUILDS))),
    persistent_term:put({?MODULE, verify_ms}, max(1, env(verify_ms, ?DEFAULT_VERIFY_MS))),
    persistent_term:put({?MODULE, rebuild_timeout_ms},
                        max(1, env(rebuild_timeout_ms, ?DEFAULT_REBUILD_TIMEOUT_MS))),
    ets:insert(?TAB, [{?QUEUE, 0, 0, closed},
                      {?BUFFERED, 0}, {?ACKED, 0}, {?LATE_BUFFERED, 0}, {?FAILED, 0},
                      {?FAILED_TIMEOUT, 0}, {?FAILED_PRODUCER_DOWN, 0},
                      {?FAILED_GET_PRODUCER, 0}, {?FAILED_OTHER, 0},
                      {?DROPPED, 0}, {?DROP_LEN, 0}, {?DROP_BYTES, 0},
                      {?DROP_UNAVAIL, 0}, {?DROP_PROBE, 0}, {?DROP_BREAKER, 0},
                      {?BREAKER_CNT, 0}, {?BREAKER_MS, 0},
                      {?PRODUCER_DOWN, 0}, {?RECOVERY_DEGRADE, 0},
                      {?REBUILD_CNT, 0}, {?RECOVERY_EXHAUSTED_CNT, 0}
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

handle_call(recovery_info, _From, State) ->
    {reply, recovery_snapshot(State), State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({produce_failed, _Worker, Streak, Reason}, State) ->
    State1 = State#state{last_error = Reason},
    case State#state.recovery of
        verifying ->
            %% 重建后的探测仍然失败：说明 client 层没救回来，进入下一轮退避
            {noreply, verify_failed(Reason, State1)};
        _ ->
            handle_produce_failed(Streak, Reason, State1)
    end;
handle_cast({produce_ok, _Worker}, State) ->
    case State#state.recovery of
        verifying ->
            {noreply, verify_ok(State)};
        exhausted ->
            %% 放弃态下探测成功说明链路已恢复，自动解除放弃标记
            {noreply, recover_from_exhausted(State)};
        _ ->
            case State#state.breaker of
                probing -> {noreply, close_breaker(State)};
                _ -> {noreply, State}
            end
    end;
handle_cast({producer_down, Reason}, State) ->
    Now = now_ms(),
    Times = prune_times([Now | State#state.producer_down_times],
                        Now, producer_down_window_ms()),
    State1 = State#state{producer_down_times = Times},
    case length(Times) >= producer_down_threshold() of
        true ->
            ?LOG_ERROR("[KAFKA PLUGIN]producer down ~p times within ~pms "
                       "(threshold ~p), last reason: ~p",
                       [length(Times), producer_down_window_ms(),
                        producer_down_threshold(), Reason]),
            {noreply, enter_degraded({producer_down, Reason}, State1)};
        false ->
            {noreply, State1}
    end;
handle_cast(manual_rebuild, State) ->
    {noreply, manual_rebuild_now(State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(report, State0 = #state{report_interval = Interval,
                                    last_dropped = LastDropped,
                                    last_failed = LastFailed,
                                    last_error = LastError}) ->
    Dropped = counter(?DROPPED),
    Failed = counter(?FAILED),
    Acked = counter(?ACKED),
    Inflight = max(0, counter(?BUFFERED) - Acked),
    State = check_breaker_escalation(
              check_ack_stall(Acked, Inflight, State0#state.last_acked,
                              State0#state{last_acked = Acked})),
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
handle_info(rebuild, State = #state{recovery = degraded, rebuild_epoch = Epoch}) ->
    Epoch1 = Epoch + 1,
    cancel_timer(State#state.rebuild_tref),
    incr(?REBUILD_CNT),
    ?LOG_WARNING("[KAFKA PLUGIN]rebuilding kafka client (epoch ~p, attempt ~p, reason ~p)",
                 [Epoch1, State#state.rebuild_attempt + 1, State#state.recovery_reason]),
    Self = self(),
    %% stop_client/start_client 都是同步调用，放到独立进程里执行，
    %% 避免重建期间协调进程错过 report tick 和探测超时。
    spawn(fun() -> Self ! {rebuild_done, Epoch1, do_rebuild_client()} end),
    TRef = erlang:send_after(rebuild_timeout_ms(), self(), {rebuild_timeout, Epoch1}),
    {noreply, State#state{recovery = rebuilding_client,
                          rebuild_epoch = Epoch1,
                          rebuild_tref = undefined,
                          rebuild_timeout_tref = TRef,
                          rebuild_attempt = State#state.rebuild_attempt + 1}};
handle_info(rebuild, State) ->
    {noreply, State};
handle_info({rebuild_done, Epoch, Result}, State = #state{rebuild_epoch = Epoch}) ->
    cancel_timer(State#state.rebuild_timeout_tref),
    case Result of
        ok ->
            {noreply, enter_verifying(State)};
        {error, Reason} ->
            ?LOG_ERROR("[KAFKA PLUGIN]client rebuild failed: ~p", [Reason]),
            {noreply, enter_degraded({rebuild_failed, Reason}, State)}
    end;
handle_info({rebuild_done, _StaleEpoch, _Result}, State) ->
    %% 重建超时后才返回的结果：状态机已经走到下一轮，直接丢弃
    {noreply, State};
handle_info({rebuild_timeout, Epoch}, State = #state{rebuild_epoch = Epoch,
                                                    recovery = rebuilding_client}) ->
    ?LOG_ERROR("[KAFKA PLUGIN]client rebuild timed out after ~pms",
               [rebuild_timeout_ms()]),
    {noreply, enter_degraded(rebuild_timeout,
                             State#state{rebuild_timeout_tref = undefined})};
handle_info({rebuild_timeout, _Epoch}, State) ->
    {noreply, State};
handle_info(verify_timeout, State = #state{recovery = verifying}) ->
    cancel_timer(State#state.probe_timeout_tref),
    case queued() of
        0 ->
            %% 队列已空（故障期间消息全被丢弃或业务本就空闲），没有样本可判定。
            %% 不能因此再排一轮重建，否则空闲节点会一路烧到 exhausted。
            {noreply, verify_no_traffic(State)};
        _ ->
            ?LOG_ERROR("[KAFKA PLUGIN]client verification timed out after ~pms without success",
                       [verify_ms()]),
            {noreply, enter_degraded(verify_timeout, State#state{verify_tref = undefined})}
    end;
handle_info(verify_timeout, State) ->
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

handle_produce_failed(Streak, Reason, State = #state{breaker = closed}) ->
    case Streak >= fail_threshold() of
        true -> {noreply, open_breaker(Reason, State)};
        false -> {noreply, State}
    end;
handle_produce_failed(_Streak, Reason, State = #state{breaker = probing}) ->
    %% 探测失败立即重新打开，不再等累积到阈值
    {noreply, open_breaker(Reason, State)};
handle_produce_failed(_Streak, _Reason, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% client 级兜底恢复状态机
%%--------------------------------------------------------------------

%% ack 停滞检测只在熔断关闭时生效：熔断打开说明已经确认 Kafka 不可达，
%% 此时 inflight 不动是预期行为，不应算作 client 故障。
check_ack_stall(_Acked, _Inflight, _LastAcked, State = #state{recovery = Recovery})
  when Recovery =/= healthy ->
    State;
check_ack_stall(_Acked, _Inflight, _LastAcked, State = #state{breaker = Breaker})
  when Breaker =/= closed ->
    State;
check_ack_stall(_Acked, Inflight, _LastAcked, State) when Inflight =< 0 ->
    State#state{ack_stall_since = undefined};
check_ack_stall(Acked, _Inflight, LastAcked, State) when Acked > LastAcked ->
    State#state{ack_stall_since = undefined};
check_ack_stall(_Acked, _Inflight, _LastAcked,
                State = #state{ack_stall_since = undefined}) ->
    State#state{ack_stall_since = now_ms()};
check_ack_stall(_Acked, Inflight, _LastAcked,
                State = #state{ack_stall_since = Since}) ->
    Now = now_ms(),
    case Now - Since >= ack_stall_ms() of
        true ->
            ?LOG_ERROR("[KAFKA PLUGIN]ack stalled: inflight ~p, no ack for ~pms",
                       [Inflight, Now - Since]),
            enter_degraded({ack_stalled, Inflight},
                           State#state{ack_stall_since = undefined});
        false ->
            State
    end.

%% 熔断长期开着却一直探测不回来：只有重建 client 能打破这个循环。
%% 仅 healthy 态升级；已降级/重建中/放弃态各自有既定节奏，不重复介入。
check_breaker_escalation(State = #state{recovery = Recovery}) when Recovery =/= healthy ->
    State;
check_breaker_escalation(State = #state{breaker = closed}) ->
    State;
check_breaker_escalation(State = #state{open_at = undefined}) ->
    State;
check_breaker_escalation(State = #state{open_at = OpenAt}) ->
    case breaker_escalate_ms() of
        0 ->
            State;
        Limit ->
            Now = now_ms(),
            case Now - OpenAt >= Limit of
                true ->
                    ?LOG_ERROR("[KAFKA PLUGIN]breaker open for ~pms without any successful "
                               "probe, escalate to client rebuild", [Now - OpenAt]),
                    enter_degraded({breaker_open, Now - OpenAt}, State);
                false ->
                    State
            end
    end.

%% 进入降级：统一关掉探测、打开熔断、按退避调度一次 client 重建。
%% exhausted 与「已在重建中」不重复处理；已排好重建时只更新原因，不重排定时器。
enter_degraded(_Reason, State = #state{recovery = exhausted}) ->
    State;
enter_degraded(_Reason, State = #state{recovery = rebuilding_client}) ->
    State;
enter_degraded(Reason, State = #state{recovery = degraded, rebuild_tref = TRef})
  when TRef =/= undefined ->
    State#state{recovery_reason = Reason};
enter_degraded(Reason, State) ->
    schedule_rebuild(Reason, State).

schedule_rebuild(Reason, State) ->
    cancel_timer(State#state.probe_tref),
    cancel_timer(State#state.probe_timeout_tref),
    cancel_timer(State#state.verify_tref),
    cancel_timer(State#state.rebuild_tref),
    cancel_timer(State#state.rebuild_timeout_tref),
    set_breaker(open),
    incr(?RECOVERY_DEGRADE),
    Now = now_ms(),
    Times = prune_times([Now | State#state.rebuild_times], Now, rebuild_window_ms()),
    case length(Times) > max_rebuilds() of
        true ->
            exhausted_state(Reason, Times, State);
        false ->
            Delay = rebuild_delay(State#state.rebuild_attempt),
            TRef = erlang:send_after(Delay, self(), rebuild),
            ?LOG_ERROR("[KAFKA PLUGIN]client fault detected (~p), rebuild in ~pms "
                       "(attempt ~p, ~p/~p rebuilds in last ~pms)",
                       [Reason, Delay, State#state.rebuild_attempt + 1,
                        length(Times), max_rebuilds(), rebuild_window_ms()]),
            State#state{recovery = degraded,
                        recovery_reason = Reason,
                        recovery_since = Now,
                        rebuild_times = Times,
                        rebuild_tref = TRef,
                        rebuild_timeout_tref = undefined,
                        verify_tref = undefined,
                        ack_stall_since = undefined,
                        producer_down_times = []}
    end.

exhausted_state(Reason, Times, State) ->
    cancel_timer(State#state.probe_tref),
    cancel_timer(State#state.probe_timeout_tref),
    cancel_timer(State#state.verify_tref),
    cancel_timer(State#state.rebuild_tref),
    cancel_timer(State#state.rebuild_timeout_tref),
    set_breaker(open),
    incr(?RECOVERY_EXHAUSTED_CNT),
    %% 保留低频探测：链路若自行恢复，探测成功会自动解除放弃态；
    %% 否则只能靠 `emqx_ctl kafka rebuild` 手动介入。
    TRef = erlang:send_after(probe_interval(), self(), probe),
    ?LOG_ERROR("[KAFKA PLUGIN]client recovery exhausted after ~p rebuilds in ~pms "
               "(last reason ~p), drop/park continues, manual rebuild required",
               [length(Times), rebuild_window_ms(), Reason]),
    State#state{recovery = exhausted,
                recovery_reason = {exhausted, Reason},
                recovery_since = now_ms(),
                rebuild_times = Times,
                probe_tref = TRef,
                rebuild_tref = undefined,
                rebuild_timeout_tref = undefined,
                verify_tref = undefined,
                ack_stall_since = undefined}.

%% 重建完成：清空 worker 侧的旧 producer 监控与元数据缓存，用一次探测窗口验证
enter_verifying(State = #state{rebuild_epoch = Epoch}) ->
    cancel_timer(State#state.rebuild_tref),
    cancel_timer(State#state.rebuild_timeout_tref),
    cancel_timer(State#state.probe_tref),
    cancel_timer(State#state.probe_timeout_tref),
    cancel_timer(State#state.verify_tref),
    catch ets:match_delete(?TAB, {{part_cnt, '_'}, '_'}),
    broadcast({client_rebuilt, Epoch}),
    set_breaker(probing),
    broadcast({probe, probe_per_worker()}),
    TRef = erlang:send_after(verify_ms(), self(), verify_timeout),
    ?LOG_NOTICE("[KAFKA PLUGIN]client rebuilt (epoch ~p), verifying with ~p probe msg/worker",
                [Epoch, probe_per_worker()]),
    State#state{recovery = verifying,
                recovery_since = now_ms(),
                rebuild_tref = undefined,
                rebuild_timeout_tref = undefined,
                verify_tref = TRef,
                probe_tref = undefined,
                probe_timeout_tref = undefined,
                ack_stall_since = undefined}.

verify_ok(State) ->
    cancel_timer(State#state.verify_tref),
    cancel_timer(State#state.probe_timeout_tref),
    ?LOG_NOTICE("[KAFKA PLUGIN]client verified after ~p rebuild attempt(s), recovery healthy",
                [State#state.rebuild_attempt]),
    mark_healthy(State#state{verify_tref = undefined}).

verify_no_traffic(State) ->
    ?LOG_NOTICE("[KAFKA PLUGIN]client rebuilt (epoch ~p) with empty queue, nothing to verify, "
                "mark healthy", [State#state.rebuild_epoch]),
    mark_healthy(State#state{verify_tref = undefined}).

verify_failed(Reason, State) ->
    cancel_timer(State#state.verify_tref),
    cancel_timer(State#state.probe_timeout_tref),
    ?LOG_ERROR("[KAFKA PLUGIN]client verification failed: ~p", [Reason]),
    set_breaker(open),
    enter_degraded({verify_failed, Reason},
                   State#state{recovery = degraded,
                               verify_tref = undefined,
                               rebuild_tref = undefined}).

recover_from_exhausted(State) ->
    ?LOG_NOTICE("[KAFKA PLUGIN]probe succeeded while exhausted, clear exhausted flag"),
    mark_healthy(State).

%% 唯一的「恢复健康」收口：关熔断、广播 drain、清空重建预算与所有故障信号
mark_healthy(State) ->
    State1 = close_breaker(State),
    State1#state{recovery = healthy,
                 recovery_reason = undefined,
                 recovery_since = undefined,
                 rebuild_attempt = 0,
                 rebuild_times = [],
                 rebuild_tref = undefined,
                 verify_tref = undefined,
                 ack_stall_since = undefined,
                 producer_down_times = []}.

%% 手动重建：不受退避与窗口预算限制，运维显式介入时清空历史重建次数
manual_rebuild_now(State = #state{recovery = rebuilding_client}) ->
    ?LOG_WARNING("[KAFKA PLUGIN]manual rebuild ignored, rebuild already in progress"),
    State;
manual_rebuild_now(State) ->
    cancel_timer(State#state.probe_tref),
    cancel_timer(State#state.probe_timeout_tref),
    cancel_timer(State#state.verify_tref),
    cancel_timer(State#state.rebuild_tref),
    cancel_timer(State#state.rebuild_timeout_tref),
    set_breaker(open),
    ?LOG_WARNING("[KAFKA PLUGIN]manual rebuild requested, recovery state reset"),
    TRef = erlang:send_after(0, self(), rebuild),
    State#state{recovery = degraded,
                recovery_reason = manual,
                recovery_since = now_ms(),
                rebuild_attempt = 0,
                rebuild_times = [],
                rebuild_tref = TRef,
                rebuild_timeout_tref = undefined,
                verify_tref = undefined,
                ack_stall_since = undefined,
                producer_down_times = []}.

%% 在独立进程里执行，stop 失败（client 本就不在）不应阻断后续 start
do_rebuild_client() ->
    catch brod:stop_client(?CLIENT),
    timer:sleep(?REBUILD_SETTLE_MS),
    case persistent_term:get({emqx_plugin_kafka, kafka_client_config}, undefined) of
        {Addresses, Config} when is_list(Addresses), is_list(Config) ->
            try brod:start_client(Addresses, ?CLIENT, Config) of
                ok -> ok;
                {ok, _} -> ok;
                Other -> {error, Other}
            catch
                Class:Reason -> {error, {Class, Reason}}
            end;
        Other ->
            {error, {no_client_config, Other}}
    end.

recovery_snapshot(State) ->
    Now = now_ms(),
    #{state => State#state.recovery,
      reason => State#state.recovery_reason,
      since_ms => elapsed(State#state.recovery_since, Now),
      rebuild_attempt => State#state.rebuild_attempt,
      rebuild_epoch => State#state.rebuild_epoch,
      rebuilds_in_window => length(State#state.rebuild_times),
      max_rebuilds => max_rebuilds(),
      rebuild_window_ms => rebuild_window_ms(),
      next_rebuild_in_ms => read_timer_ms(State#state.rebuild_tref),
      verify_in_ms => read_timer_ms(State#state.verify_tref),
      ack_stall_elapsed_ms => elapsed(State#state.ack_stall_since, Now),
      ack_stall_timeout_ms => ack_stall_ms(),
      breaker_open_ms => elapsed(State#state.open_at, Now),
      breaker_escalate_ms => breaker_escalate_ms(),
      last_acked => State#state.last_acked,
      inflight => inflight(),
      producer_down_in_window => length(State#state.producer_down_times),
      producer_down_threshold => producer_down_threshold(),
      degrade_total => counter(?RECOVERY_DEGRADE),
      rebuild_total => counter(?REBUILD_CNT),
      exhausted_total => counter(?RECOVERY_EXHAUSTED_CNT)}.

elapsed(undefined, _Now) -> 0;
elapsed(Since, Now) -> max(0, Now - Since).

read_timer_ms(undefined) ->
    0;
read_timer_ms(TRef) ->
    case erlang:read_timer(TRef) of
        false -> 0;
        Ms -> Ms
    end.

prune_times(Times, Now, Window) ->
    [T || T <- Times, Now - T < Window].

rebuild_delay(Attempt) ->
    Base = rebuild_backoff_base_ms(),
    Max = rebuild_backoff_max_ms(),
    min(Max, Base bsl min(Attempt, 10)).

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

ack_stall_ms() ->
    persistent_term:get({?MODULE, ack_stall_ms}, ?DEFAULT_ACK_STALL_MS).

breaker_escalate_ms() ->
    persistent_term:get({?MODULE, breaker_escalate_ms}, ?DEFAULT_BREAKER_ESCALATE_MS).

producer_down_window_ms() ->
    persistent_term:get({?MODULE, producer_down_window_ms}, ?DEFAULT_PRODUCER_DOWN_WINDOW_MS).

producer_down_threshold() ->
    persistent_term:get({?MODULE, producer_down_threshold}, ?DEFAULT_PRODUCER_DOWN_THRESHOLD).

rebuild_backoff_base_ms() ->
    persistent_term:get({?MODULE, rebuild_backoff_base_ms}, ?DEFAULT_REBUILD_BACKOFF_BASE_MS).

rebuild_backoff_max_ms() ->
    persistent_term:get({?MODULE, rebuild_backoff_max_ms}, ?DEFAULT_REBUILD_BACKOFF_MAX_MS).

rebuild_window_ms() ->
    persistent_term:get({?MODULE, rebuild_window_ms}, ?DEFAULT_REBUILD_WINDOW_MS).

max_rebuilds() ->
    persistent_term:get({?MODULE, max_rebuilds}, ?DEFAULT_MAX_REBUILDS).

verify_ms() ->
    persistent_term:get({?MODULE, verify_ms}, ?DEFAULT_VERIFY_MS).

rebuild_timeout_ms() ->
    persistent_term:get({?MODULE, rebuild_timeout_ms}, ?DEFAULT_REBUILD_TIMEOUT_MS).

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
