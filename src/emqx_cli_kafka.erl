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

%% @doc 插件 CLI：`emqx_ctl kafka stats` / `emqx_ctl kafka workers`。
%%
%% 压测采样需要每轮反复读取队列与熔断计数，远程 shell 手工调用无法脚本化，
%% 这里把两个最常用的采样入口注册成 emqx_ctl 命令。
%%   * stats  ：全量计数器，一行一个 key=value，便于 awk/grep 解析；
%%   * workers：各 worker 邮箱长度，用于判断分片倾斜（max/min ≤ 3）。
%%
%% 注意：worker 在 produce 等待期间不回消息，所以这里只读 process_info，
%% 不向 worker 发 gen_server:call，避免采样本身被阻塞的 worker 拖住。
-module(emqx_cli_kafka).

-export([cmd/1]).

cmd(usage) ->
    usage();
cmd([]) ->
    usage();
cmd(["stats"]) ->
    print_stats();
cmd(["workers"]) ->
    print_workers();
cmd(["recovery"]) ->
    print_recovery();
cmd(["rebuild"]) ->
    rebuild_client();
cmd(_) ->
    usage().

usage() ->
    emqx_ctl:usage([{"kafka stats", "print queue / breaker / drop / loss counters"},
                    {"kafka workers", "print per-worker mailbox length (skew check)"},
                    {"kafka recovery", "print client-level recovery state machine"},
                    {"kafka rebuild", "force rebuild kafka client and reset rebuild budget"}]).

print_stats() ->
    Stats = emqx_plugin_kafka_queue:stats(),
    lists:foreach(fun(Key) ->
                          emqx_ctl:print("~-24s = ~p~n", [Key, maps:get(Key, Stats)])
                  end, lists:sort(maps:keys(Stats))),
    ok.

print_workers() ->
    Names = tuple_to_list(emqx_plugin_kafka_queue:worker_names()),
    Mailboxes = [{Name, mailbox_len(Name)} || Name <- Names],
    lists:foreach(fun({Name, undefined}) ->
                          emqx_ctl:print("~-28s = down~n", [Name]);
                     ({Name, Len}) ->
                          emqx_ctl:print("~-28s = mailbox ~p~n", [Name, Len])
                  end, Mailboxes),
    case [Len || {_, Len} <- Mailboxes, is_integer(Len)] of
        [] ->
            emqx_ctl:print("workers=~p alive=0~n", [length(Names)]);
        Alive ->
            emqx_ctl:print("workers=~p alive=~p mailbox max/min/avg = ~p/~p/~.1f~n",
                           [length(Names), length(Alive), lists:max(Alive),
                            lists:min(Alive), lists:sum(Alive) / length(Alive)])
    end,
    ok.

print_recovery() ->
    Info = emqx_plugin_kafka_queue:recovery_info(),
    lists:foreach(fun(Key) ->
                          emqx_ctl:print("~-28s = ~p~n", [Key, maps:get(Key, Info, undefined)])
                  end, lists:sort(maps:keys(Info))),
    ok.

rebuild_client() ->
    emqx_ctl:print("requesting kafka client rebuild, check `kafka recovery` in a few seconds~n", []),
    ok = emqx_plugin_kafka_queue:rebuild_client().

mailbox_len(Name) ->
    case whereis(Name) of
        undefined ->
            undefined;
        Pid ->
            case process_info(Pid, message_queue_len) of
                {message_queue_len, Len} -> Len;
                undefined -> undefined
            end
    end.
