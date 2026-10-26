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

%% @doc Kafka 异步有界队列 + worker 侧熔断。
%%
%% EMQX 的 hook 都在客户端连接进程里同步执行，直接调用 brod 的 produce 接口会在
%% Kafka 抖动时把这些进程以及 brod 生产者的邮箱拖成无界增长。这里把写 Kafka 的
%% 动作收敛到单个 worker 进程：
%%   * hook 侧只做「熔断状态 + 队列额度」判断和 cast，永不阻塞；
%%   * 队列长度/字节数都有上限，超限直接丢弃并计数，内存边界可配；
%%   * worker 侧给单次 produce 加超时，连续失败达到阈值后熔断；熔断期间 hook 直接
%%     丢弃，worker 也把已入队的消息快速丢完，避免 Kafka 长时间不可用时把陈旧数据
%%     一直挂在内存里、恢复后又一股脑灌给 Kafka。
-module(emqx_plugin_kafka_queue).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([ start_link/0
  , push/3
  , full/0
  , drop/0
  , stats/0
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

-define(DEFAULT_MAX_LEN, 10000).
-define(DEFAULT_MAX_BYTES, 33554432).  %% 32MB
-define(DEFAULT_REPORT_INTERVAL, 5000).
-define(DEFAULT_PRODUCE_TIMEOUT, 5000).
-define(DEFAULT_FAIL_THRESHOLD, 5).
-define(DEFAULT_PROBE_INTERVAL, 10000).

-define(KAFKA_CLIENT, emqx_repost_worker).

%% brod 3.16.8 的 #brod_call_ref{} / #brod_produce_reply{} 以及
%% brod_producer:produce_cb/4 往 producer 进程发的 {produce, CallRef, Batch, AckCb}
%% 协议。插件单独编译时没有 brod 依赖，无法 include brod.hrl，因此这里按字段顺序
%% 本地声明；升级 brod 时需要同步核对 brod.hrl。
-record(brod_call_ref, {caller, callee, ref}).
-record(brod_produce_reply, {call_ref, base_offset, result}).

-define(BROD_PRODUCE_REQ_BUFFERED, brod_produce_req_buffered).

-record(state, {report_interval = ?DEFAULT_REPORT_INTERVAL :: pos_integer(),
  last_dropped = 0 :: non_neg_integer(),
  last_failed = 0 :: non_neg_integer(),
  fail_cnt = 0 :: non_neg_integer(),
  breaker = closed :: closed | probing | open,
  probe_tref :: reference() | undefined,
  last_error = undefined :: term()}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc 非阻塞入队。队列满、熔断中或队列未就绪时直接丢弃，返回错误给调用方计数。
-spec push(binary(), binary(), binary()) -> ok | {error, full | unavailable}.
push(Topic, Key, Payload) ->
  case reserve(byte_size(Payload)) of
    ok ->
      gen_server:cast(?MODULE, {produce, Topic, Key, Payload}),
      ok;
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

-spec drop() -> ok.
drop() ->
  catch ets:update_counter(?TAB, ?DROPPED, 1),
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
    max_len => max_len(),
    max_bytes => max_bytes()}.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
  catch ets:delete(?TAB),
  ets:new(?TAB, [named_table, public, set, {write_concurrency, true}]),
  ets:insert(?TAB, [{?COUNT, 0}, {?BYTES, 0}, {?BUFFERED, 0}, {?ACKED, 0},
    {?FAILED, 0}, {?DROPPED, 0}, {?ACCEPTING, true},
    {?BREAKER, closed}]),
  persistent_term:put({?MODULE, max_len}, env(queue_max_len, ?DEFAULT_MAX_LEN)),
  persistent_term:put({?MODULE, max_bytes}, env(queue_max_bytes, ?DEFAULT_MAX_BYTES)),
  persistent_term:put({?MODULE, produce_timeout}, env(produce_timeout, ?DEFAULT_PRODUCE_TIMEOUT)),
  persistent_term:put({?MODULE, fail_threshold}, env(fail_threshold, ?DEFAULT_FAIL_THRESHOLD)),
  persistent_term:put({?MODULE, probe_interval}, env(probe_interval, ?DEFAULT_PROBE_INTERVAL)),
  Interval = env(queue_report_interval, ?DEFAULT_REPORT_INTERVAL),
  erlang:send_after(Interval, self(), report),
  ?LOG_NOTICE("[KAFKA PLUGIN]queue started, max_len=~p, max_bytes=~p, "
  "produce_timeout=~p, fail_threshold=~p, probe_interval=~p",
    [max_len(), max_bytes(), produce_timeout(), fail_threshold(),
      probe_interval()]),
  {ok, #state{report_interval = Interval}}.

handle_call(_Request, _From, State) ->
  {reply, {error, unknown_call}, State}.

handle_cast({produce, Topic, Key, Payload}, State) ->
  State1 = handle_produce(Topic, Key, Payload, State),
  %% 发送完成后再释放额度，这样 worker 卡在 Kafka 上时额度保持占用，
  %% 新消息会被拒绝而不是堆进 worker 邮箱。
  release(byte_size(Payload)),
  {noreply, State1};
handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(report, State = #state{report_interval = Interval,
  last_dropped = LastDropped,
  last_failed = LastFailed}) ->
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
        [Failed - LastFailed, Interval, Failed, State#state.last_error]);
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
%% produce 超时后 brod 仍可能补发 buffered 回执，这里直接丢弃，避免堆积在邮箱里
handle_info(#brod_produce_reply{}, State) ->
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

%% 熔断打开期间，hook 侧已经不会再 push，这里只清理残留的已入队消息。
handle_produce(_Topic, _Key, _Payload, State = #state{breaker = open}) ->
  bump(?DROPPED),
  State;
handle_produce(Topic, Key, Payload, State) ->
  Result = try produce(Topic, Key, Payload)
           catch Class:CatchReason -> {error, {Class, CatchReason}}
           end,
  case Result of
    ok ->
      on_success(State);
    {error, Reason} ->
      on_failure(Reason, State)
  end.

produce(Topic, Key, Payload) ->
  case partitions_count(Topic) of
    {ok, Cnt} ->
      Part = erlang:phash2(Key, Cnt),
      case brod:get_producer(?KAFKA_CLIENT, Topic, Part) of
        {ok, Pid} ->
          produce_to(Pid, Key, Payload);
        {error, Reason} ->
          %% 分区数可能已变化，下次重新拉 metadata
          catch ets:delete(?TAB, {part_cnt, Topic}),
          {error, {get_producer, Reason}}
      end;
    {error, Reason} ->
      {error, {get_partitions_count, Reason}}
  end.

%% 与 brod_producer:produce_cb/4 等价，区别是 receive 带超时：
%% buffer 满时 brod 不会回 buffered 回执，调用方会无限期挂住，超时是唯一的感知手段。
produce_to(Pid, Key, Payload) ->
  Mref = erlang:monitor(process, Pid),
  CallRef = #brod_call_ref{caller = self(), callee = Pid, ref = Mref},
  %% AckCb 必须是 fun/2：传 undefined 时 acked 回执会被 send 到本进程邮箱
  AckCb = fun(_Partition, _BaseOffset) -> bump(?ACKED) end,
  Pid ! {produce, CallRef, [{Key, Payload}], AckCb},
  receive
    #brod_produce_reply{call_ref = #brod_call_ref{ref = Mref},
      result = ?BROD_PRODUCE_REQ_BUFFERED} ->
      erlang:demonitor(Mref, [flush]),
      bump(?BUFFERED),
      ok;
    {'DOWN', Mref, process, _, Reason} ->
      {error, {producer_down, Reason}}
  after produce_timeout() ->
    erlang:demonitor(Mref, [flush]),
    {error, timeout}
  end.

partitions_count(Topic) ->
  try ets:lookup(?TAB, {part_cnt, Topic}) of
    [{_, Cnt}] ->
      {ok, Cnt};
    [] ->
      case brod:get_partitions_count(?KAFKA_CLIENT, Topic) of
        {ok, Cnt} ->
          ets:insert(?TAB, {{part_cnt, Topic}, Cnt}),
          {ok, Cnt};
        {error, Reason} ->
          {error, Reason}
      end
  catch
    error:badarg ->
      {error, unavailable}
  end.

on_success(State = #state{breaker = probing}) ->
  set_breaker(closed),
  set_accepting(true),
  ?LOG_NOTICE("[KAFKA PLUGIN]kafka recovered, breaker closed"),
  State#state{breaker = closed, fail_cnt = 0};
on_success(State) ->
  State#state{fail_cnt = 0}.

on_failure(Reason, State = #state{breaker = probing}) ->
  %% 探测失败立即重新打开，不再等累积到阈值
  bump(?FAILED),
  open_breaker(Reason, State);
on_failure(Reason, State = #state{fail_cnt = Cnt}) ->
  bump(?FAILED),
  Cnt1 = Cnt + 1,
  State1 = State#state{fail_cnt = Cnt1, last_error = Reason},
  case Cnt1 >= fail_threshold() of
    true -> open_breaker(Reason, State1);
    false -> State1
  end.

open_breaker(Reason, State) ->
  cancel_timer(State#state.probe_tref),
  TRef = erlang:send_after(probe_interval(), self(), probe),
  set_breaker(open),
  set_accepting(false),
  ?LOG_ERROR("[KAFKA PLUGIN]kafka unavailable, breaker open for ~pms, last error: ~p",
    [probe_interval(), Reason]),
  State#state{breaker = open, fail_cnt = 0, probe_tref = TRef, last_error = Reason}.

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

%% 先占额度再入队，保证 worker 邮箱不会超过 queue_max_len。
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

release(Size) ->
  catch ets:update_counter(?TAB, ?COUNT, -1),
  catch ets:update_counter(?TAB, ?BYTES, -Size),
  ok.

counter(Key) ->
  try ets:lookup_element(?TAB, Key, 2)
  catch error:badarg -> 0
  end.

breaker_state() ->
  try ets:lookup_element(?TAB, ?BREAKER, 2)
  catch error:badarg -> unknown
  end.

bump(Key) ->
  catch ets:update_counter(?TAB, Key, 1),
  ok.

max_len() ->
  persistent_term:get({?MODULE, max_len}, ?DEFAULT_MAX_LEN).

max_bytes() ->
  persistent_term:get({?MODULE, max_bytes}, ?DEFAULT_MAX_BYTES).

produce_timeout() ->
  persistent_term:get({?MODULE, produce_timeout}, ?DEFAULT_PRODUCE_TIMEOUT).

fail_threshold() ->
  persistent_term:get({?MODULE, fail_threshold}, ?DEFAULT_FAIL_THRESHOLD).

probe_interval() ->
  persistent_term:get({?MODULE, probe_interval}, ?DEFAULT_PROBE_INTERVAL).

env(Key, Default) ->
  application:get_env(emqx_plugin_kafka, Key, Default).
