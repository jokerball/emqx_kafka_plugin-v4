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

%% @doc Kafka 异步有界队列。
%%
%% EMQX 的 hook 都在客户端连接进程里同步执行，直接调用 brod:produce_cb/6 会在
%% Kafka 抖动时把这些进程以及 brod 生产者的邮箱拖成无界增长。这里把写 Kafka
%% 的动作收敛到单个 worker 进程，hook 侧只做「计数准入 + cast」：
%%   * 队列长度/字节数都有上限，超限直接丢弃并计数，绝不阻塞 hook；
%%   * Kafka 卡住时只阻塞 worker，内存始终被 queue_max_len/queue_max_bytes 约束。
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
-define(SENT, sent).
-define(FAILED, failed).
-define(DROPPED, dropped).

-define(DEFAULT_MAX_LEN, 10000).
-define(DEFAULT_MAX_BYTES, 33554432).  %% 32MB
-define(DEFAULT_REPORT_INTERVAL, 5000).

-define(KAFKA_CLIENT, emqx_repost_worker).

-record(state, {report_interval = ?DEFAULT_REPORT_INTERVAL :: pos_integer(),
  last_dropped = 0 :: non_neg_integer(),
  last_failed = 0 :: non_neg_integer(),
  last_error = undefined :: term()}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc 非阻塞入队。队列满或队列未就绪时直接丢弃，返回错误给调用方计数。
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

%% @doc 供 hook 在 JSON 编码前做一次廉价预判，避免满队列时白白编码。
-spec full() -> boolean().
full() ->
  try
    ets:lookup_element(?TAB, ?COUNT, 2) >= max_len()
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
  try
    #{queued => ets:lookup_element(?TAB, ?COUNT, 2),
      queued_bytes => ets:lookup_element(?TAB, ?BYTES, 2),
      sent => ets:lookup_element(?TAB, ?SENT, 2),
      failed => ets:lookup_element(?TAB, ?FAILED, 2),
      dropped => ets:lookup_element(?TAB, ?DROPPED, 2),
      max_len => max_len(),
      max_bytes => max_bytes()}
  catch
    error:badarg ->
      #{queued => 0, queued_bytes => 0, sent => 0, failed => 0, dropped => 0,
        max_len => max_len(), max_bytes => max_bytes()}
  end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
  catch ets:delete(?TAB),
  ets:new(?TAB, [named_table, public, set, {write_concurrency, true}]),
  ets:insert(?TAB, [{?COUNT, 0}, {?BYTES, 0}, {?SENT, 0}, {?FAILED, 0}, {?DROPPED, 0}]),
  persistent_term:put({?MODULE, max_len}, env(queue_max_len, ?DEFAULT_MAX_LEN)),
  persistent_term:put({?MODULE, max_bytes}, env(queue_max_bytes, ?DEFAULT_MAX_BYTES)),
  Interval = env(queue_report_interval, ?DEFAULT_REPORT_INTERVAL),
  erlang:send_after(Interval, self(), report),
  ?LOG_NOTICE("[KAFKA PLUGIN]queue started, max_len=~p, max_bytes=~p",
    [max_len(), max_bytes()]),
  {ok, #state{report_interval = Interval}}.

handle_call(_Request, _From, State) ->
  {reply, {error, unknown_call}, State}.

handle_cast({produce, Topic, Key, Payload}, State) ->
  Size = byte_size(Payload),
  State1 = do_produce(Topic, Key, Payload, State),
  %% 发送完成后再释放额度，这样 worker 卡在 Kafka 上时额度保持占用，
  %% 新消息会被拒绝而不是堆进 worker 邮箱。
  release(Size),
  {noreply, State1};
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
      ?LOG_WARNING("[KAFKA PLUGIN]queue overflow, dropped ~p messages in last ~pms, "
      "total dropped ~p, still queued ~p",
        [Dropped - LastDropped, Interval, Dropped, counter(?COUNT)]);
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
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

do_produce(Topic, Key, Payload, State) ->
  Result = try
             brod:produce_cb(?KAFKA_CLIENT, Topic, hash, Key, Payload, fun(_, _) -> ok end)
           catch
             Class:CatchReason -> {error, {Class, CatchReason}}
           end,
  case Result of
    ok ->
      bump(?SENT),
      State;
    {ok, _} ->
      bump(?SENT),
      State;
    {error, Reason} ->
      bump(?FAILED),
      State#state{last_error = Reason};
    Other ->
      bump(?FAILED),
      State#state{last_error = Other}
  end.

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

bump(Key) ->
  catch ets:update_counter(?TAB, Key, 1),
  ok.

max_len() ->
  persistent_term:get({?MODULE, max_len}, ?DEFAULT_MAX_LEN).

max_bytes() ->
  persistent_term:get({?MODULE, max_bytes}, ?DEFAULT_MAX_BYTES).

env(Key, Default) ->
  application:get_env(emqx_plugin_kafka, Key, Default).
