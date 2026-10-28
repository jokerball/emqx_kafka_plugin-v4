%%--------------------------------------------------------------------
%% Copyright (c) 2015-2017 Feng Lee <feng@emqtt.io>.
%%
%% Modified by Ramez Hanna <rhanna@iotblue.net>
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

-module(emqx_plugin_kafka).

% -include("emqx_plugin_kafka.hrl").

% -include_lib("emqx/include/emqx.hrl").

-include("emqx.hrl").
-include_lib("kernel/include/logger.hrl").

-export([load/1, unload/0]).

%% brod 只认 default_producer_config 里的生产者参数，放在 client 配置顶层会被静默忽略
-define(PRODUCER_CONFIG_KEYS,
  [required_acks, ack_timeout, max_retries, retry_backoff_ms,
   partition_buffer_limit, partition_onwire_limit, max_batch_size,
   max_linger_ms, max_linger_count, compression]).

%% 配置缺失时的兜底值：绝不能是 acks=0 / max_retries=0。
%% acks=0 时既观测不到 broker 侧错误，acked 统计也永远为 0；
%% max_retries=0 会让任何一次抖动都直接判失败，且 brod 缓冲里的消息会静默丢失。
-define(DEFAULT_PRODUCER_CONFIG,
  [{partition_buffer_limit, 1024},
   {partition_onwire_limit, 8},
   {retry_backoff_ms, 500},
   {max_retries, 100},
   {required_acks, 1},
   {ack_timeout, 1500},
   {max_linger_ms, 30},
   {max_linger_count, 1000},
   {max_batch_size, 262144}]).

%% 支持按事件单独配置 Kafka topic 的 hook
-define(CONFIGURED_EVENTS,
  [on_client_heartbeat, on_message_publish, on_message_delivered,
   on_message_acked, on_session_subscribed, on_session_unsubscribed]).

%% Client Lifecircle Hooks
-export([on_client_connect/3
  , on_client_connack/4
  , on_client_connected/3
  , on_client_disconnected/4
%%  , on_client_authenticate/3
%%  , on_client_check_acl/5
  , on_client_subscribe/4
  , on_client_unsubscribe/4

  , on_client_heartbeat/3
]).

%% Session Lifecircle Hooks
-export([on_session_created/3
  , on_session_subscribed/4
  , on_session_unsubscribed/4
  , on_session_resumed/3
  , on_session_discarded/3
  , on_session_takeovered/3
  , on_session_terminated/4
]).

%% Message Pubsub Hooks
-export([on_message_publish/2
  , on_message_delivered/3
  , on_message_acked/3
  , on_message_dropped/4
]).


%% Called when the plugin application start
load(Env) ->
  kafka_init([Env]),
  init_forward_switches(),
  init_config_cache(),
  emqx_ctl:register_command(kafka, {emqx_cli_kafka, cmd}, []),
  emqx:hook('client.heartbeat', {?MODULE, on_client_heartbeat, [Env]}),

  emqx:hook('client.connect', {?MODULE, on_client_connect, [Env]}),
  emqx:hook('client.connack', {?MODULE, on_client_connack, [Env]}),
  emqx:hook('client.connected', {?MODULE, on_client_connected, [Env]}),
  emqx:hook('client.disconnected', {?MODULE, on_client_disconnected, [Env]}),
%%  emqx:hook('client.authenticate', {?MODULE, on_client_authenticate, [Env]}),
%%  emqx:hook('client.check_acl', {?MODULE, on_client_check_acl, [Env]}),
  emqx:hook('client.subscribe', {?MODULE, on_client_subscribe, [Env]}),
  emqx:hook('client.unsubscribe', {?MODULE, on_client_unsubscribe, [Env]}),
  emqx:hook('session.created', {?MODULE, on_session_created, [Env]}),
  emqx:hook('session.subscribed', {?MODULE, on_session_subscribed, [Env]}),
  emqx:hook('session.unsubscribed', {?MODULE, on_session_unsubscribed, [Env]}),
  emqx:hook('session.resumed', {?MODULE, on_session_resumed, [Env]}),
  emqx:hook('session.discarded', {?MODULE, on_session_discarded, [Env]}),
  emqx:hook('session.takeovered', {?MODULE, on_session_takeovered, [Env]}),
  emqx:hook('session.terminated', {?MODULE, on_session_terminated, [Env]}),
  emqx:hook('message.publish', {?MODULE, on_message_publish, [Env]}),
  emqx:hook('message.delivered', {?MODULE, on_message_delivered, [Env]}),
  emqx:hook('message.acked', {?MODULE, on_message_acked, [Env]}),
  emqx:hook('message.dropped', {?MODULE, on_message_dropped, [Env]}).

on_client_heartbeat(_ClientInfo = #{clientid := ClientId, protocol := Protocol},
    _ConnInfo = #{keepalive := Keepalive, socktype := SockType}, _Env) ->
  ?LOG_DEBUG("[KAFKA PLUGIN]Client(~s) heartbeat", [ClientId]),
  Action = <<"heartbeat">>,
  Now = now_mill_secs(),
  Payload = [
    {action, Action},
    {protocol, Protocol},
    {conn_type, SockType},
    {device_id, ClientId},
    {keepalive, Keepalive},
    {ts, Now},
    {cluster_node, node()}
  ],
  produce_kafka_payload(ClientId, Payload, on_client_heartbeat),
  ok.

on_client_connect(ConnInfo = #{clientid := ClientId}, Props, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) connect, ConnInfo: ~p, Props: ~p~n",
    [ClientId, ConnInfo, Props]),
  ok.

on_client_connack(ConnInfo = #{clientid := ClientId}, Rc, Props, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) connack, ConnInfo: ~p, Rc: ~p, Props: ~p~n",
    [ClientId, ConnInfo, Rc, Props]),
  ok.

on_client_connected(ClientInfo = #{clientid := ClientId, protocol := Protocol},
    ConnInfo = #{keepalive := Keepalive, socktype := SockType}, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) connected, ClientInfo:~n~p~n, ConnInfo:~n~p~n",
    [ClientId, ClientInfo, ConnInfo]),
%%  上线时改为发送心跳
  Action = <<"heartbeat">>,
  Now = now_mill_secs(),
  Payload = [
    {action, Action},
    {protocol, Protocol},
    {conn_type, SockType},
    {device_id, ClientId},
    {keepalive, Keepalive},
    {ts, Now},
    {cluster_node, node()}
  ],
  produce_kafka_payload(ClientId, Payload, on_client_heartbeat),
  ok.
%%  {IpAddr, _Port} = maps:get(peername, ConnInfo),
%%  Action = <<"connected">>,
%%  Now = now_mill_secs(),
%%  Online = 1,
%%  Payload = [
%%    {action, Action},
%%    {device_id, ClientId},
%%    {username, maps:get(username, ClientInfo)},
%%    {keepalive, maps:get(keepalive, ConnInfo)},
%%    {ipaddress, iolist_to_binary(ntoa(IpAddr))},
%%    {proto_name, maps:get(proto_name, ConnInfo)},
%%    {proto_ver, maps:get(proto_ver, ConnInfo)},
%%    {ts, Now},
%%    {online, Online}
%%  ],
%%  produce_kafka_payload(ClientId, Payload),
%%  ok.

on_client_disconnected(ClientInfo = #{clientid := ClientId, protocol := Protocol}, ReasonCode,
    ConnInfo = #{keepalive := Keepalive, socktype := SockType}, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) disconnected due to ~p, ClientInfo:~n~p~n, ConnInfo:~n~p~n",
    [ClientId, ReasonCode, ClientInfo, ConnInfo]),
  %% 离线时改为发送带有离线标识的心跳
  Action = <<"heartbeat">>,
  Now = now_mill_secs(),
  Payload = [
    {action, Action},
    {protocol, Protocol},
    {conn_type, SockType},
    {device_id, ClientId},
    {keepalive, Keepalive},
    {ts, Now},
    {off, 1},
    {cluster_node, node()}
  ],
  produce_kafka_payload(ClientId, Payload, on_client_heartbeat),
  ok.
%%  Action = <<"disconnected">>,
%%  Now = now_mill_secs(),
%%  Online = 0,
%%  Payload = [
%%    {action, Action},
%%    {device_id, ClientId},
%%    {username, maps:get(username, ClientInfo)},
%%    {reason, ReasonCode},
%%    {ts, Now},
%%    {online, Online}
%%  ],
%%  produce_kafka_payload(ClientId, Payload),
%%  ok.

%%on_client_authenticate(_ClientInfo = #{clientid := ClientId}, Result, _Env) ->
%%  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) authenticate, Result:~n~p~n", [ClientId, Result]),
%%  ok.

%%on_client_check_acl(_ClientInfo = #{clientid := ClientId}, Topic, PubSub, Result, _Env) ->
%%  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) check_acl, PubSub:~p, Topic:~p, Result:~p~n",
%%    [ClientId, PubSub, Topic, Result]),
%%  ok.

%%---------------------------client subscribe start--------------------------%%
on_client_subscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) will subscribe: ~p~n", [ClientId, TopicFilters]),
%%  Topic = erlang:element(1, erlang:hd(TopicFilters)),
%%  Qos = erlang:element(2, lists:last(TopicFilters)),
%%  Action = <<"subscribe">>,
%%  Now = now_mill_secs(),
%%  Payload = [
%%    {device_id, ClientId},
%%    {action, Action},
%%    {topic, Topic},
%%    {qos, maps:get(qos, Qos)},
%%    {ts, Now}
%%  ],
%%  produce_kafka_payload(ClientId, Payload, on_client_subscribe),
  ok.

%%---------------------client subscribe stop----------------------%%
on_client_unsubscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Client(~s) will unsubscribe ~p~n", [ClientId, TopicFilters]),
%%  Topic = erlang:element(1, erlang:hd(TopicFilters)),
%%  Action = <<"unsubscribe">>,
%%  Now = now_mill_secs(),
%%  Payload = [
%%    {device_id, ClientId},
%%    {action, Action},
%%    {topic, Topic},
%%    {ts, Now}
%%  ],
%%  produce_kafka_payload(ClientId, Payload, on_client_unsubscribe),
  ok.

on_message_dropped(_Message = #message{topic = <<"$SYS/", _/binary>>}, _By, _Reason, _Env) ->
  ok;
on_message_dropped(Message, _By = #{node := Node}, Reason, _Env) ->
%%  减少非info级别下emqx_message:format的开销
  case logger:allow(info, ?MODULE) of
    true ->
      ?LOG_INFO("[KAFKA PLUGIN]Message dropped by node ~s due to ~s: ~s~n",
        [Node, Reason, emqx_message:format(Message)]);
    false ->
      ok
  end,
  ok.




%% ------------------------------
%% 核心路由逻辑（前缀→精确→默认）
%% ------------------------------
%% 总路由函数：实现「前缀匹配 → 精确匹配 → 默认兜底」
-spec get_target_kafka_topic(binary()) -> binary().
get_target_kafka_topic(MqttTopic) ->
  %% 步骤1：尝试前缀匹配
  case get_kafka_topic_by_prefix(MqttTopic) of
    KafkaTopic when is_binary(KafkaTopic) ->
      KafkaTopic;
    _ ->  %% 匹配所有非二进制情况（包括undefined）
      %% 步骤2：前缀匹配失败，尝试精确匹配
      case get_kafka_topic_by_mqtt_topic(MqttTopic) of
        KafkaTopic2 when is_binary(KafkaTopic2) ->
          KafkaTopic2;
        _ ->  %% 匹配所有非二进制情况
          %% 步骤3：均失败，使用默认Topic
          get_kafka_topic()
      end
  end.

%% 前缀匹配：遍历配置的前缀映射表，二进制前缀匹配
-spec get_kafka_topic_by_prefix(binary()) -> binary() | undefined.
get_kafka_topic_by_prefix(MqttTopic) ->
  PrefixMappings = persistent_term:get({?MODULE, topic_prefix_mapping}, []),
  match_prefix(MqttTopic, PrefixMappings).

%% 内部递归辅助函数：二进制前缀匹配
-spec match_prefix(binary(), list({binary(), binary()})) -> binary() | undefined.
match_prefix(_MqttTopic, []) -> undefined;  % 无匹配返回undefined
match_prefix(MqttTopic, [{Prefix, KafkaTopic} | Rest]) ->
  PrefixLen = byte_size(Prefix),
  case MqttTopic of
    <<Prefix:PrefixLen/binary, _/binary>> -> KafkaTopic;  % 前缀匹配成功
    _ -> match_prefix(MqttTopic, Rest)  % 继续匹配下一个前缀
  end.


%%---------------------------message publish start--------------------------%%
on_message_publish(_Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
  ok;
on_message_publish(Message, _Env) ->
  Topic = Message#message.topic,
  ClientId = Message#message.from,
  case topic_in_ignored_list(Topic) of
    true ->
      ?LOG_DEBUG("[KAFKA PLUGIN]on_message_publish by client(~s), topic: ~s is in ignored list, unnecessary to repost to kafka~n",
        [ClientId, Topic]);
    _ ->
      Headers = Message#message.headers,
      %% 打印所有 header 内容（用于调试）
      ?LOG_DEBUG("[KAFKA PLUGIN] Full headers: ~p", [Headers]),
      Username = emqx_message:get_header(username, Message),
      IpAddr = emqx_message:get_header(peerhost, Message),
      %% 获取Content-Type，如果不存在则使用默认值 application/text
      Properties = maps:get(properties, Headers, #{}),
      ContentType = maps:get('Content-Type', Properties, <<"application/text">>),
      %% 打印获取到的 ContentType 值
      ?LOG_DEBUG("[KAFKA PLUGIN] ContentType: ~s", [ContentType]),
      % ?LOG_INFO("[KAFKA PLUGIN]Tail= ~s , RawType= ~s~n",[Tail,RawType]),
      Content = transform_payload(Message#message.payload),
      Payload = [{action, message_publish},
        {device_id, ClientId},
        {ipaddress, iolist_to_binary(ntoa(IpAddr))},
        {username, Username},
        {topic, Topic},
        {payload, Content},
        {content_type, ContentType},
        {cluster_node, node()},
        {ts, Message#message.timestamp}],
      %% 根据 MQTT Topic 获取对应的 Kafka Topic
      KafkaTopic = get_target_kafka_topic(Topic),
      %% 发送消息到 Kafka
      produce_kafka_payload_by_kafka_topic(ClientId, Payload, KafkaTopic)
  end,
  ok.
%%---------------------message publish stop----------------------%%

on_message_delivered(_ClientInfo, _Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
  ok;
on_message_delivered(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
  case forward_enabled(on_message_delivered) of
    true -> do_message_delivered(ClientId, Message);
    false -> ok
  end.

do_message_delivered(ClientId, Message) ->
  %%  减少非info级别下emqx_message:format的开销
  case logger:allow(info, ?MODULE) of
    true ->
      ?LOG_INFO("[KAFKA PLUGIN]Message delivered to client(~s): ~s~n",
        [ClientId, emqx_message:format(Message)]);
    false ->
      ok
  end,

  Topic = Message#message.topic,
  case topic_in_ignored_list(Topic) of
    true ->
      ?LOG_DEBUG("[KAFKA PLUGIN]on_message_delivered by client(~s), topic: ~s is in ignored list, unnecessary to repost to kafka~n",
        [ClientId, Topic]);
    _ ->
      Payload = transform_payload(Message#message.payload),
      Qos = Message#message.qos,
      From = Message#message.from,
      Timestamp = Message#message.timestamp,
      Content = [
        {action, <<"message_delivered">>},
        {from, From},
        {to, ClientId},
        {topic, Topic},
        {payload, Payload},
        {qos, Qos},
        {cluster_node, node()},
        {ts, Timestamp}
      ],
      produce_kafka_payload(ClientId, Content, on_message_delivered)
  end,
  ok.

on_message_acked(_ClientInfo, _Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
  ok;
on_message_acked(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
  case forward_enabled(on_message_acked) of
    true -> do_message_acked(ClientId, Message);
    false -> ok
  end.

do_message_acked(ClientId, Message) ->
  %%  减少非info级别下emqx_message:format的开销
  case logger:allow(info, ?MODULE) of
    true ->
      ?LOG_INFO("[KAFKA PLUGIN]Message acked by client(~s): ~s~n",
        [ClientId, emqx_message:format(Message)]);
    false ->
      ok
  end,

  Topic = Message#message.topic,
  case topic_in_ignored_list(Topic) of
    true ->
      ?LOG_DEBUG("[KAFKA PLUGIN]on_message_acked by client(~s), topic: ~s is in ignored list, unnecessary to repost to kafka~n",
        [ClientId, Topic]);
    _ ->
      Payload = transform_payload(Message#message.payload),
      Qos = Message#message.qos,
      From = Message#message.from,
      Timestamp = Message#message.timestamp,
      Content = [
        {action, <<"message_acked">>},
        {from, From},
        {to, ClientId},
        {topic, Topic},
        {payload, Payload},
        {qos, Qos},
        {cluster_node, node()},
        {ts, Timestamp}
      ],
      produce_kafka_payload(ClientId, Content, on_message_acked)
  end,
  ok.

topic_in_ignored_list(Topic) ->
  IgnoreList = persistent_term:get({?MODULE, ignore_list}, []),
  lists:any(fun(Pattern) ->
    topic_pattern_match(Topic, Pattern)
            end,
    IgnoreList).

topic_pattern_match(Topic, Pattern) ->
  ByteSize = byte_size(Pattern),
  case Topic of
    <<Pattern:ByteSize/binary, _/binary>> -> true;
    _ -> false
  end.

%% 根据 MQTT Topic 获取对应的 Kafka Topic
get_kafka_topic_by_mqtt_topic(MqttTopic) ->
  case maps:find(MqttTopic, persistent_term:get({?MODULE, topic_mapping_map}, #{})) of
    {ok, KafkaTopic} -> KafkaTopic;
    error -> get_kafka_topic()
  end.


%%--------------------------------------------------------------------
%% Session Life circle Hooks
%%--------------------------------------------------------------------

on_session_created(#{clientid := ClientId}, SessInfo, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Session(~s) created, Session Info:~n~p~n", [ClientId, SessInfo]),
  ok.


on_session_subscribed(#{clientid := ClientId}, Topic, SubOpts, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Session(~s) subscribed ~s with subopts: ~p~n", [ClientId, Topic, SubOpts]),
  case event_configured(on_session_subscribed) of
    true ->
      Qos = maps:get(qos, SubOpts),
      Payload = [
        {device_id, ClientId},
        {action, <<"subscribed">>},
        {topic, Topic},
        {qos, Qos},
        {ts, now_mill_secs()},
        {cluster_node, node()}
      ],
      produce_kafka_payload(ClientId, Payload, on_session_subscribed),
      ok;
    _ -> ok
  end.

on_session_unsubscribed(#{clientid := ClientId}, Topic, Opts, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Session(~s) unsubscribed ~s with opts: ~p~n", [ClientId, Topic, Opts]),
  case event_configured(on_session_unsubscribed) of
    true ->
      Qos = maps:get(qos, Opts),
      Payload = [
        {device_id, ClientId},
        {action, <<"unsubscribed">>},
        {topic, Topic},
        {qos, Qos},
        {ts, now_mill_secs()},
        {cluster_node, node()}
      ],
      produce_kafka_payload(ClientId, Payload, on_session_unsubscribed),
      ok;
    _ -> ok
  end.

on_session_resumed(#{clientid := ClientId}, SessInfo, _Env) ->
  ?LOG_NOTICE("[KAFKA PLUGIN]Session(~s) resumed, Session Info:~n~p~n", [ClientId, SessInfo]),
  ok.

on_session_discarded(_ClientInfo = #{clientid := ClientId}, SessInfo, _Env) ->
  ?LOG_NOTICE("[KAFKA PLUGIN]Session(~s) is discarded. Session Info: ~p~n", [ClientId, SessInfo]),
  ok.

on_session_takeovered(_ClientInfo = #{clientid := ClientId}, SessInfo, _Env) ->
  ?LOG_NOTICE("[KAFKA PLUGIN]Session(~s) is takeovered. Session Info: ~p~n", [ClientId, SessInfo]),
  ok.

on_session_terminated(_ClientInfo = #{clientid := ClientId}, Reason, SessInfo, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Session(~s) is terminated due to ~p~nSession Info: ~p~n",
    [ClientId, Reason, SessInfo]),
  ok.

kafka_init(_Env) ->
  ?LOG_NOTICE("Start to init emqx plugin kafka..... ~n"),
  {ok, AddressList} = application:get_env(emqx_plugin_kafka, kafka_address_list),
  ?LOG_NOTICE("[KAFKA PLUGIN]KafkaAddressList = ~p~n", [AddressList]),
  {ok, OriKafkaConfig} = application:get_env(emqx_plugin_kafka, kafka_config),
  KafkaConfig = normalize_kafka_config(OriKafkaConfig) ++ [{auto_start_producers, true}],
  ?LOG_NOTICE("[KAFKA PLUGIN]KafkaConfig = ~p~n", [KafkaConfig]),
  {ok, KafkaTopic} = application:get_env(emqx_plugin_kafka, topic),
  ?LOG_NOTICE("[KAFKA PLUGIN]KafkaTopic = ~s~n", [KafkaTopic]),
  {ok, TopicMapping} = application:get_env(emqx_plugin_kafka, topic_mapping),
  ?LOG_NOTICE("[KAFKA PLUGIN]TopicMapping = ~p~n", [TopicMapping]),
  Prop = application:get_all_env(emqx_plugin_kafka),
  ?LOG_NOTICE("[KAFKA PLUGIN]All config = ~p~n", [Prop]),
  {ok, _} = application:ensure_all_started(brod),
  %% client 级兜底恢复需要在运行时重建 client，把启动参数留在 persistent_term
  persistent_term:put({emqx_plugin_kafka, kafka_client_config},
                      {AddressList, KafkaConfig}),
  ok = brod:start_client(AddressList, emqx_repost_worker, KafkaConfig),
  ?LOG_NOTICE("Init emqx plugin kafka successfully.....~n"),
  ok.

get_kafka_topic() ->
  %% 默认的kafka topic
  persistent_term:get({?MODULE, topic}, <<"emqx-topic">>).

get_kafka_topic(Event) ->
  %% 不同事件特定的topic
  maps:get(Event, persistent_term:get({?MODULE, event_topics}, #{}), get_kafka_topic()).

event_configured(Event) ->
  maps:is_key(Event, persistent_term:get({?MODULE, event_topics}, #{})).

need_base64() ->
  persistent_term:get({?MODULE, publish_base64}, false).

transform_payload(Payload) ->
  case need_base64() of
    true -> base64:encode(Payload);
    false -> Payload
  end.


%% Called when the plugin application stop
unload() ->
  emqx_ctl:unregister_command(kafka),
  emqx:unhook('client.heartbeat', {?MODULE, on_client_heartbeat}),

  emqx:unhook('client.connect', {?MODULE, on_client_connect}),
  emqx:unhook('client.connack', {?MODULE, on_client_connack}),
  emqx:unhook('client.connected', {?MODULE, on_client_connected}),
  emqx:unhook('client.disconnected', {?MODULE, on_client_disconnected}),
%%  emqx:unhook('client.authenticate', {?MODULE, on_client_authenticate}),
%%  emqx:unhook('client.check_acl', {?MODULE, on_client_check_acl}),
  emqx:unhook('client.subscribe', {?MODULE, on_client_subscribe}),
  emqx:unhook('client.unsubscribe', {?MODULE, on_client_unsubscribe}),
  emqx:unhook('session.created', {?MODULE, on_session_created}),
  emqx:unhook('session.subscribed', {?MODULE, on_session_subscribed}),
  emqx:unhook('session.unsubscribed', {?MODULE, on_session_unsubscribed}),
  emqx:unhook('session.resumed', {?MODULE, on_session_resumed}),
  emqx:unhook('session.discarded', {?MODULE, on_session_discarded}),
  emqx:unhook('session.takeovered', {?MODULE, on_session_takeovered}),
  emqx:unhook('session.terminated', {?MODULE, on_session_terminated}),
  emqx:unhook('message.publish', {?MODULE, on_message_publish}),
  emqx:unhook('message.delivered', {?MODULE, on_message_delivered}),
  emqx:unhook('message.acked', {?MODULE, on_message_acked}),
  emqx:unhook('message.dropped', {?MODULE, on_message_dropped}),
  brod:stop_client(emqx_repost_worker),
  persistent_term:erase({emqx_plugin_kafka, kafka_client_config}),
  erase_config_cache().

%% delivered / acked 默认不转发：一条 MQTT 消息被 N 个订阅者消费时，这两个钩子
%% 会把 Kafka 写入量放大到 1 + 2N 倍，大连接量场景下按需开启。
init_forward_switches() ->
  persistent_term:put({?MODULE, on_message_delivered},
    application:get_env(emqx_plugin_kafka, forward_delivered, false)),
  persistent_term:put({?MODULE, on_message_acked},
    application:get_env(emqx_plugin_kafka, forward_acked, false)),
  ok.

%% 路由/过滤/base64 配置在热路径上每条消息都要读，加载时缓存到 persistent_term
init_config_cache() ->
  {ok, Topic} = application:get_env(emqx_plugin_kafka, topic),
  TopicMapping = application:get_env(emqx_plugin_kafka, topic_mapping, []),
  PrefixMappings = application:get_env(emqx_plugin_kafka, topic_prefix_mapping, []),
  IgnoreList = application:get_env(emqx_plugin_kafka, mqtt_topic_pattern_ignored_list, []),
  EventTopics = maps:from_list(
    [{Event, Topic0} || Event <- ?CONFIGURED_EVENTS,
                        {ok, Topic0} <- [application:get_env(emqx_plugin_kafka, Event)]]),
  persistent_term:put({?MODULE, topic}, Topic),
  persistent_term:put({?MODULE, topic_mapping_map}, maps:from_list(TopicMapping)),
  persistent_term:put({?MODULE, topic_prefix_mapping}, PrefixMappings),
  persistent_term:put({?MODULE, ignore_list}, IgnoreList),
  persistent_term:put({?MODULE, publish_base64},
    application:get_env(emqx_plugin_kafka, publish_base64, false)),
  persistent_term:put({?MODULE, event_topics}, EventTopics),
  ok.

erase_config_cache() ->
  lists:foreach(fun(Key) -> persistent_term:erase({?MODULE, Key}) end,
    [topic, topic_mapping_map, topic_prefix_mapping, ignore_list,
     publish_base64, event_topics, on_message_delivered, on_message_acked]).

forward_enabled(Event) ->
  persistent_term:get({?MODULE, Event}, false).

produce_kafka_payload(Key, Message, Event) ->
  enqueue_kafka(get_kafka_topic(Event), Key, Message).

produce_kafka_payload_by_kafka_topic(Key, Message, KafkaTopic) ->
  enqueue_kafka(KafkaTopic, Key, Message).

%% hook 里只做「容量判断 + JSON 编码 + 入队」，绝不直接调用 brod
enqueue_kafka(Topic, Key, Message) ->
  case emqx_plugin_kafka_queue:full_reason() of
    ok ->
      case emqx_json:safe_encode(Message) of
        {ok, MessageBody} ->
          _ = emqx_plugin_kafka_queue:push(Topic, Key, iolist_to_binary(MessageBody)),
          ok;
        {error, Reason} ->
          ?LOG_WARNING("[KAFKA PLUGIN]failed to encode payload for topic ~s: ~p",
            [Topic, Reason]),
          ok
      end;
    {drop, Reason} ->
      emqx_plugin_kafka_queue:drop(Reason),
      ok
  end.

normalize_kafka_config(Config) ->
  {ProducerCfg, ClientCfg} =
    lists:partition(fun({Key, _}) -> lists:member(Key, ?PRODUCER_CONFIG_KEYS) end, Config),
  case lists:keymember(default_producer_config, 1, ClientCfg) of
    true ->
      case ProducerCfg of
        [] -> ClientCfg;
        _ ->
          ?LOG_WARNING("[KAFKA PLUGIN]top level producer options ~p are ignored, "
            "please move them into default_producer_config",
            [[K || {K, _} <- ProducerCfg]]),
          ClientCfg
      end;
    false ->
      case ProducerCfg of
        [] ->
          ClientCfg ++ [{default_producer_config, ?DEFAULT_PRODUCER_CONFIG}];
        _ ->
          ?LOG_NOTICE("[KAFKA PLUGIN]producer options ~p moved into default_producer_config",
            [[K || {K, _} <- ProducerCfg]]),
          ClientCfg ++ [{default_producer_config, ProducerCfg ++ ?DEFAULT_PRODUCER_CONFIG}]
      end
  end.

ntoa({0, 0, 0, 0, 0, 16#ffff, AB, CD}) ->
  inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256});
ntoa(IP) ->
  Res = inet_parse:ntoa(IP),
  if
    Res == {error, einval} -> "";
    true -> Res
  end.
now_mill_secs() ->
  erlang:system_time(millisecond).
