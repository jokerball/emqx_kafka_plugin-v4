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

%% @doc Kafka 写入 worker。
%%
%% 每个 worker 只负责自己分片（按 device_id 哈希）的消息，串行写 Kafka；
%% 单条 produce 带超时，超时或 producer 退出都算一次失败并把连续失败次数
%% 上报给 emqx_plugin_kafka_queue 裁决是否熔断。
-module(emqx_plugin_kafka_worker).

-behaviour(gen_server).

-export([start_link/1]).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(TAB, emqx_plugin_kafka_queue_tab).
-define(KAFKA_CLIENT, emqx_repost_worker).

%% brod 3.16.8 的 #brod_call_ref{} / #brod_produce_reply{} 以及
%% brod_producer:produce_cb/4 往 producer 进程发的 {produce, CallRef, Batch, AckCb}
%% 协议。插件单独编译时没有 brod 依赖，无法 include brod.hrl，因此这里按字段顺序
%% 本地声明；升级 brod 时需要同步核对 brod.hrl。
-record(brod_call_ref, {caller, callee, ref}).
-record(brod_produce_reply, {call_ref, base_offset, result}).

-define(BROD_PRODUCE_REQ_BUFFERED, brod_produce_req_buffered).

-record(state, {streak = 0 :: non_neg_integer()}).

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

init([]) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({produce, Topic, Key, Payload}, State) ->
    Size = byte_size(Payload),
    State1 =
        case emqx_plugin_kafka_queue:accepting() of
            false ->
                %% 熔断期间快速丢弃，避免 worker 把已入队消息一条条写超时
                emqx_plugin_kafka_queue:drop(),
                State;
            true ->
                do_produce(Topic, Key, Payload, State)
        end,
    %% 发送完成后再释放额度，worker 卡在 Kafka 上时额度保持占用
    emqx_plugin_kafka_queue:release(Size),
    {noreply, State1};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(#brod_produce_reply{}, State) ->
    %% produce 超时后 brod 仍可能补发 buffered 回执，直接丢弃
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

do_produce(Topic, Key, Payload, State) ->
    Result = try produce(Topic, Key, Payload)
             catch Class:CatchReason -> {error, {Class, CatchReason}}
             end,
    case Result of
        ok -> on_success(State);
        {error, Reason} -> on_failure(Reason, State)
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
    AckCb = fun(_Partition, _BaseOffset) -> emqx_plugin_kafka_queue:note_acked() end,
    %% brod 3.16 的批次单元是 map（#{ts, key, value}），不是 {Key, Value} 元组，
    %% 直接传元组会让 brod_producer 在 brod_utils:bytes/1 处 function_clause 崩溃。
    %% 用 brod 自己的构造函数，避免与内部格式耦合。
    Batch = brod_utils:make_batch_input(Key, Payload),
    Pid ! {produce, CallRef, Batch, AckCb},
    receive
        #brod_produce_reply{call_ref = #brod_call_ref{ref = Mref},
                            result = ?BROD_PRODUCE_REQ_BUFFERED} ->
            erlang:demonitor(Mref, [flush]),
            emqx_plugin_kafka_queue:note_buffered(),
            ok;
        {'DOWN', Mref, process, _, Reason} ->
            {error, {producer_down, Reason}}
    after emqx_plugin_kafka_queue:produce_timeout() ->
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

on_success(State = #state{streak = Streak}) ->
    case Streak > 0 orelse emqx_plugin_kafka_queue:breaker_state() =:= probing of
        true -> emqx_plugin_kafka_queue:report_success();
        false -> ok
    end,
    State#state{streak = 0}.

on_failure(Reason, State = #state{streak = Streak}) ->
    Streak1 = Streak + 1,
    emqx_plugin_kafka_queue:note_failed(),
    emqx_plugin_kafka_queue:report_failure(Streak1, Reason),
    State#state{streak = Streak1}.
