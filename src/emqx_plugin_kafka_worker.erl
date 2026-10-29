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
%% 每个 worker 只负责自己分片（按 device_id 哈希）的消息，串行写 Kafka。
%%
%% 消息先进本地 FIFO，再从邮箱里尽量多捞一批，按 {Topic, Partition}
%% 分组后每组只做一次 monitor/send/receive，把「每条一次」的调度开销摊薄到
%% 「每组一次」；邮箱为空时 after 0 立即返回，单条消息不会因此多等。
%%
%% 熔断只决定「发不发 Kafka」：熔断期间已入队消息 park 在本地 FIFO，恢复后由
%% drain 按序补发；探测时每个 worker 只放行 probe_per_worker 条。
%% 一组提交共用一个批级超时预算，超时或 producer 退出都算失败并把连续失败次数
%% 上报给 emqx_plugin_kafka_queue 裁决是否熔断。
-module(emqx_plugin_kafka_worker).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

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
%% 必须与 emqx_plugin_kafka_queue.erl 的 ?QUEUE 保持一致
-define(QUEUE, queue_usage).

%% brod 3.16.8 的 #brod_call_ref{} / #brod_produce_reply{} 以及
%% brod_producer:produce_cb/4 往 producer 进程发的 {produce, CallRef, Batch, AckCb}
%% 协议。插件单独编译时没有 brod 依赖，无法 include brod.hrl，因此这里按字段顺序
%% 本地声明；升级 brod 时需要同步核对 brod.hrl。
-record(brod_call_ref, {caller, callee, ref}).
-record(brod_produce_reply, {call_ref, base_offset, result}).

-define(BROD_PRODUCE_REQ_BUFFERED, brod_produce_req_buffered).

%% 分区数缓存的失效时间，过期后重新向 brod 查询，避免 Topic 动态扩分区后按旧分区数哈希
-define(PART_CNT_TTL_MS, 60000).

%% pending 里的元素：{Size, Topic, Key, Payload}，Size 预先算好，批量切分时
%% 不需要反复 byte_size/1。
-record(state, {name :: atom(),
                streak = 0 :: non_neg_integer(),
                pending = queue:new() :: queue:queue(),
                pending_len = 0 :: non_neg_integer(),
                pending_bytes = 0 :: non_neg_integer(),
                producers = #{} :: #{pid() => reference()}}).

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, Name, []).

init(Name) ->
    set_message_queue_data(),
    reclaim_outstanding(Name),
    {ok, #state{name = Name}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({produce, Topic, Key, Payload}, State) ->
    Item = {byte_size(Payload), Topic, Key, Payload},
    %% 熔断状态通过 ETS 传播有延迟，Kafka 快速失败时 worker 可能把整个邮箱
    %% 逐条烧完。drain 里用本地连续失败数做同阈值的前置熔断。
    drain(collect(park_msg(Item, State)));
handle_cast({probe, N}, State) ->
    probe(N, State);
handle_cast(drain, State) ->
    %% drain 只在协调进程确认 Kafka 恢复后广播，此时本地失败计数一并清零
    drain(State#state{streak = 0});
handle_cast({client_rebuilt, _Epoch}, State = #state{producers = Mon}) ->
    %% 旧 client 名下的 producer 已全部作废：先解除监控并清表，再清零本地失败计数。
    %% 否则这些 producer 的 DOWN 会被当成新一轮故障上报，刚重建完就再次降级。
    maps:foreach(fun(_Pid, MRef) -> catch erlang:demonitor(MRef, [flush]) end, Mon),
    {noreply, State#state{producers = #{}, streak = 0}};
handle_cast(reset, State) ->
    %% 协调进程重启后计数已归零，清空 pending 但不要 release；
    %% 本地熔断计数也要清零，否则 worker 会一直 park 而等不到 probe。
    {noreply, State#state{pending = queue:new(), pending_len = 0, pending_bytes = 0,
                          streak = 0}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(#brod_produce_reply{result = ?BROD_PRODUCE_REQ_BUFFERED,
                                call_ref = #brod_call_ref{ref = {_Mref, Count}}},
            State) when is_integer(Count) ->
    %% 超时后才到的回执：消息实际已进 brod buffer，按组内条数补记 buffered。
    %% 额度在超时时已归还，这条消息的 queued/inflight 口径必然不守恒，
    %% 单列 late_buffered 让对账时能解释差额。
    emqx_plugin_kafka_queue:note_buffered(Count),
    emqx_plugin_kafka_queue:note_late_buffered(Count),
    {noreply, State};
handle_info(#brod_produce_reply{}, State) ->
    {noreply, State};
handle_info({'DOWN', MRef, process, Pid, Reason}, State) ->
    handle_producer_down(MRef, Pid, Reason, State);
handle_info(continue_drain, State) ->
    drain(State);
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

%% 从邮箱里把已经到达的 produce 消息尽量并进当前批：
%% 邮箱为空时 after 0 立刻返回，不会给单条消息增加等待；
%% 有积压时这一批的上限由 batch_max_count / batch_max_bytes 决定。
collect(State) ->
    collect(State, emqx_plugin_kafka_queue:batch_max_count() - 1,
            emqx_plugin_kafka_queue:batch_max_bytes()).

collect(State, N, Bytes) when N =< 0; Bytes =< 0 ->
    State;
collect(State, N, Bytes) ->
    receive
        {'$gen_cast', {produce, Topic, Key, Payload}} ->
            Size = byte_size(Payload),
            collect(park_msg({Size, Topic, Key, Payload}, State), N - 1, Bytes - Size)
    after 0 ->
        State
    end.

park_msg(Item, State = #state{pending = Q, pending_len = L, pending_bytes = B}) ->
    {Size, _, _, _} = Item,
    State#state{pending = queue:in(Item, Q), pending_len = L + 1, pending_bytes = B + Size}.

%% 放回队首：未发送的消息不能排到新消息后面，否则同一分片的顺序会被打乱
park_front(Items, State) ->
    park_msg_front(Items, State).

park_msg_front([], State) ->
    State;
park_msg_front(Items, State = #state{pending = Q, pending_len = L, pending_bytes = B}) ->
    {Q1, L1, B1} =
        lists:foldl(fun({Size, _, _, _} = Item, {Qa, La, Ba}) ->
                            {queue:in_r(Item, Qa), La + 1, Ba + Size}
                    end, {Q, L, B}, lists:reverse(Items)),
    State#state{pending = Q1, pending_len = L1, pending_bytes = B1}.

drain(State = #state{pending_len = 0}) ->
    {noreply, State};
drain(State = #state{streak = Streak}) ->
    case can_send(Streak) of
        false ->
            {noreply, State};
        true ->
            maybe_continue(drain_msg(State))
    end.

drain_msg(State) ->
    {Batch, State1} = take_batch(State),
    case Batch of
        [] -> State1;
        _  -> send_batch(Batch, State1)
    end.

maybe_continue(State = #state{streak = Streak}) ->
    HasWork = State#state.pending_len > 0,
    case HasWork andalso can_send(Streak) of
        true ->
            %% 让出进程：既避免单次 drain 长时间占住调度器，也给
            %% 邮箱里的新消息一个被合并进下一批的机会。
            erlang:send_after(0, self(), continue_drain),
            {noreply, State};
        false ->
            {noreply, State}
    end.

can_send(Streak) ->
    Streak < emqx_plugin_kafka_queue:fail_threshold()
        andalso emqx_plugin_kafka_queue:breaker_state() =:= closed.

probe(_N, State = #state{pending_len = 0}) ->
    {noreply, State};
probe(N, State) ->
    {Batch, State1} = take_batch(State, max(1, N),
                                 emqx_plugin_kafka_queue:batch_max_bytes()),
    probe_result(Batch, State1).

probe_result([], State) ->
    {noreply, State};
probe_result(Batch, State) ->
    State1 = send_batch(Batch, State),
    case emqx_plugin_kafka_queue:breaker_state() of
        closed -> drain(State1);       %% 已恢复，剩余积压交给 drain
        _ -> {noreply, State1}
    end.

take_batch(State) ->
    take_batch(State, emqx_plugin_kafka_queue:batch_max_count(),
               emqx_plugin_kafka_queue:batch_max_bytes()).

take_batch(State, N, Bytes) ->
    take_batch(State, N, Bytes, []).

take_batch(State, N, _Bytes, Acc) when N =< 0 ->
    {lists:reverse(Acc), State};
take_batch(State, _N, Bytes, Acc) when Bytes =< 0 ->
    {lists:reverse(Acc), State};
take_batch(State = #state{pending = Q, pending_len = L, pending_bytes = B}, N, Bytes, Acc) ->
    case queue:out(Q) of
        {empty, _} ->
            {lists:reverse(Acc), State#state{pending = Q, pending_len = 0, pending_bytes = 0}};
        {{value, {Size, _, _, _} = Item}, Q1} ->
            take_batch(State#state{pending = Q1, pending_len = L - 1,
                                   pending_bytes = B - Size},
                       N - 1, Bytes - Size, [Item | Acc])
    end.

%% 一批消息共用一个超时预算：批内任何一组超时都不会把整批的等待放大成
%% 组数 × produce_timeout。
send_batch(Batch, State) ->
    Deadline = now_ms() + emqx_plugin_kafka_queue:produce_timeout(),
    case group_batch(Batch) of
        {ok, Groups} ->
            send_groups(Groups, Deadline, State);
        {error, Reason} ->
            %% 分区数都拉不到，整批一条都没发出去：放回队首等下一轮，额度不归还
            on_failure(Reason, length(Batch), park_front(Batch, State))
    end.

group_batch(Batch) ->
    Topics = lists:usort([Topic || {_Size, Topic, _Key, _Payload} <- Batch]),
    case resolve_topics(Topics, #{}) of
        {ok, Counts} -> {ok, group_items(Batch, Counts, [], #{})};
        {error, Reason} -> {error, Reason}
    end.

resolve_topics([], Acc) ->
    {ok, Acc};
resolve_topics([Topic | Rest], Acc) ->
    case partitions_count(Topic) of
        {ok, Cnt} -> resolve_topics(Rest, Acc#{Topic => Cnt});
        {error, Reason} -> {error, {get_partitions_count, Reason}}
    end.

%% 按 {Topic, Partition} 分组，组内保持原顺序
group_items([], _Counts, Order, Map) ->
    [{GroupKey, lists:reverse(maps:get(GroupKey, Map))}
     || GroupKey <- lists:reverse(Order)];
group_items([{_Size, Topic, Key, _Payload} = Item | Rest], Counts, Order, Map) ->
    GroupKey = {Topic, erlang:phash2(Key, maps:get(Topic, Counts))},
    case maps:is_key(GroupKey, Map) of
        true ->
            group_items(Rest, Counts, Order, Map#{GroupKey => [Item | maps:get(GroupKey, Map)]});
        false ->
            group_items(Rest, Counts, [GroupKey | Order], Map#{GroupKey => [Item]})
    end.

send_groups([], _Deadline, State) ->
    State;
send_groups([{GroupKey, Items} | Rest], Deadline, State) ->
    case remaining(Deadline) of
        0 ->
            %% 批级预算已耗尽：剩下的消息还没发出去，按序放回队首，不丢
            park_front(flatten(Rest), State);
        Timeout ->
            {Topic, Partition} = GroupKey,
            case send_group(Topic, Partition, Items, Timeout, State) of
                {ok, State1} ->
                    send_groups(Rest, Deadline, State1);
                {unsent, Reason, State1} ->
                    %% 这一组根本没发出去（拿不到 producer），和剩余组一起放回队首
                    on_failure(Reason, length(Items),
                               park_front(Items ++ flatten(Rest), State1));
                {failed, Reason, State1} ->
                    on_failure(Reason, length(Items),
                               park_front(flatten(Rest), State1))
            end
    end.

send_group(Topic, Partition, Items, Timeout, State = #state{name = Name}) ->
    case brod:get_producer(?KAFKA_CLIENT, Topic, Partition) of
        {ok, Pid} ->
            State1 = ensure_monitor(Pid, State),
            Count = length(Items),
            Bytes = items_bytes(Items),
            case produce_group(Pid, Items, Timeout) of
                ok ->
                    %% 一组只归还一次额度，把每条一次的 ETS 更新摊薄到每组一次
                    emqx_plugin_kafka_queue:release_batch(Count, Bytes, Name),
                    {ok, on_success(State1)};
                {error, Reason} ->
                    %% 已经交给 brod 但没拿到 buffered 回执：按丢弃处理，不重试，
                    %% 否则回执迟到时会造成重复写入。
                    emqx_plugin_kafka_queue:release_batch(Count, Bytes, Name),
                    {failed, Reason, State1}
            end;
        {error, Reason} ->
            %% 分区数可能已变化，下次重新拉 metadata
            catch ets:delete(?TAB, {part_cnt, Topic}),
            {unsent, {get_producer, Reason}, State}
    end.

%% 与 brod_producer:produce_cb/4 等价，区别是：
%%   * 一组多条消息合成一个 req，只做一次 monitor/send/receive；
%%   * receive 带超时：buffer 满时 brod 不会回 buffered 回执，超时是唯一感知手段；
%%   * CallRef 的 ref 里带上实际写入条数，超时后迟到的回执才能按条数补记 buffered。
produce_group(Pid, Items, Timeout) ->
    Mref = erlang:monitor(process, Pid),
    %% brod 3.16 的批次单元是 map（#{ts, key, value}），一个 req 可携带多条，
    %% 用 brod 自己的构造函数拼接，避免与内部格式耦合。
    Batch = lists:append([brod_utils:make_batch_input(Key, Payload)
                          || {_Size, _Topic, Key, Payload} <- Items]),
    %% 对账以 brod 侧 msg_cnt（= length(Batch)）为准，而非外层 length(Items)：
    %% 若某条 payload 是 batch，make_batch_input 会从单条展开成多条，两者不再相等，
    %% note_buffered/note_acked 必须与 brod 实际口径保持一致。
    BatchLen = length(Batch),
    CallRef = #brod_call_ref{caller = self(), callee = Pid, ref = {Mref, BatchLen}},
    %% AckCb 必须是 fun/2：传 undefined 时 acked 回执会被 send 到本进程邮箱
    AckCb = fun(_Partition, _BaseOffset) -> emqx_plugin_kafka_queue:note_acked(BatchLen) end,
    Pid ! {produce, CallRef, Batch, AckCb},
    receive
        #brod_produce_reply{call_ref = #brod_call_ref{ref = {Mref, BatchLen}},
                            result = ?BROD_PRODUCE_REQ_BUFFERED} ->
            erlang:demonitor(Mref, [flush]),
            emqx_plugin_kafka_queue:note_buffered(BatchLen),
            ok;
        {'DOWN', Mref, process, _, Reason} ->
            {error, {producer_down, Reason}}
    after Timeout ->
        erlang:demonitor(Mref, [flush]),
        {error, timeout}
    end.

partitions_count(Topic) ->
    try ets:lookup(?TAB, {part_cnt, Topic}) of
        [{_, {Cnt, Ts}}] ->
            case Ts + ?PART_CNT_TTL_MS > now_ms() of
                true -> {ok, Cnt};
                false -> refresh_partitions_count(Topic)
            end;
        _ ->
            refresh_partitions_count(Topic)
    catch
        error:badarg ->
            {error, unavailable}
    end.

refresh_partitions_count(Topic) ->
    case brod:get_partitions_count(?KAFKA_CLIENT, Topic) of
        {ok, Cnt} ->
            ets:insert(?TAB, {{part_cnt, Topic}, {Cnt, now_ms()}}),
            {ok, Cnt};
        {error, Reason} ->
            {error, Reason}
    end.

%% 长期监控 producer：它崩溃时 brod 内部 buffer 会静默消失，必须能被观测到
ensure_monitor(Pid, State = #state{producers = Mon}) ->
    case maps:is_key(Pid, Mon) of
        true -> State;
        false -> State#state{producers = Mon#{Pid => erlang:monitor(process, Pid)}}
    end.

handle_producer_down(MRef, Pid, Reason, State = #state{producers = Mon}) ->
    case maps:take(Pid, Mon) of
        {MRef, Mon1} ->
            emqx_plugin_kafka_queue:note_producer_down(),
            emqx_plugin_kafka_queue:note_failed(producer_down),
            %% 直接按阈值上报，强制熔断：producer 崩溃意味着 buffer 已丢
            emqx_plugin_kafka_queue:report_failure(
              emqx_plugin_kafka_queue:fail_threshold(), {producer_down, Reason}),
            %% 单次崩溃由 brod 自动重启 producer 即可，只有窗口内反复崩溃才由
            %% 协调进程判定为 client 层故障并触发重建
            emqx_plugin_kafka_queue:report_producer_down(Reason),
            ?LOG_ERROR("[KAFKA PLUGIN]producer ~p down: ~p", [Pid, Reason]),
            %% buffer 已随 producer 消失，本地也立即挂起，等恢复广播后再 drain
            {noreply, State#state{producers = Mon1,
                                  streak = emqx_plugin_kafka_queue:fail_threshold()}};
        _ ->
            {noreply, State}
    end.

%% worker 被 kill 时 mailbox 中已预占的额度会永久占用，重启时一次性归还
reclaim_outstanding(Name) ->
    try ets:lookup(?TAB, {outstanding, Name}) of
        [{_, C, B}] ->
            catch ets:update_counter(?TAB, ?QUEUE, [{2, -C, 0, 0}, {3, -B, 0, 0}]),
            ets:insert(?TAB, {{outstanding, Name}, 0, 0}),
            case C > 0 of
                true ->
                    ?LOG_NOTICE("[KAFKA PLUGIN]worker ~p restarted, reclaimed ~p msgs / ~p bytes",
                                [Name, C, B]);
                false ->
                    ok
            end;
        [] ->
            ok
    catch
        error:badarg -> ok
    end.

set_message_queue_data() ->
    case application:get_env(emqx_plugin_kafka, worker_message_queue_data, on_heap) of
        off_heap -> process_flag(message_queue_data, off_heap);
        _ -> ok
    end.

on_success(State = #state{streak = Streak}) ->
    case Streak > 0 orelse emqx_plugin_kafka_queue:breaker_state() =:= probing of
        true -> emqx_plugin_kafka_queue:report_success();
        false -> ok
    end,
    State#state{streak = 0}.

on_failure(Reason, Count, State = #state{streak = Streak}) ->
    %% streak 记的是「连续失败的提交次数」，不是失败消息条数。批量提交下一次失败
    %% 可能涉及上百条，若按条累加，fail_threshold=3 会被单次超时直接打满，
    %% 熔断立刻打开；配合 breaker_action=drop，一次 2s 超时就会把整个探测周期
    %% 的新消息全部丢掉，把偶发抖动放大成脉冲式丢弃。
    Streak1 = Streak + 1,
    emqx_plugin_kafka_queue:note_failed(failure_kind(Reason), Count),
    emqx_plugin_kafka_queue:report_failure(Streak1, Reason),
    State#state{streak = Streak1}.

failure_kind(timeout) -> timeout;
failure_kind({producer_down, _}) -> producer_down;
failure_kind({get_producer, _}) -> get_producer;
failure_kind(_) -> other.

flatten(Groups) ->
    lists:append([Items || {_GroupKey, Items} <- Groups]).

items_bytes(Items) ->
    lists:sum([Size || {Size, _, _, _} <- Items]).

remaining(Deadline) ->
    max(0, Deadline - now_ms()).

now_ms() ->
    erlang:monotonic_time(millisecond).