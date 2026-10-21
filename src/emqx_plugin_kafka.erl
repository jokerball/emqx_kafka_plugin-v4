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
      Username = emqx_message:get_header(username, Message),
      IpAddr = emqx_message:get_header(peerhost, Message),
      % ?LOG_INFO("[KAFKA PLUGIN]Tail= ~s , RawType= ~s~n",[Tail,RawType]),
      Content = transform_payload(Message#message.payload),
      Payload = [{action, message_publish},
        {device_id, ClientId},
        {ipaddress, iolist_to_binary(ntoa(IpAddr))},
        {username, Username},
        {topic, Topic},
        {payload, Content},
        {cluster_node, node()},
        {ts, Message#message.timestamp}],
      %% 根据 MQTT Topic 获取对应的 Kafka Topic
      KafkaTopic = get_kafka_topic_by_mqtt_topic(Topic),
      %% 发送消息到 Kafka
      produce_kafka_payload_by_kafka_topic(ClientId, Payload, KafkaTopic)
  end,
  ok.
%%---------------------message publish stop----------------------%%

on_message_delivered(_ClientInfo, _Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
  ok;
on_message_delivered(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
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
  IgnoreList = application:get_env(emqx_plugin_kafka, mqtt_topic_pattern_ignored_list, []),
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
  %% 获取配置文件中的 topic_mapping
  TopicMapping = application:get_env(emqx_plugin_kafka, topic_mapping, []),
  DefaultTopic = get_kafka_topic(),
  %% 查找匹配的 Kafka Topic
  case lists:keyfind(MqttTopic, 1, TopicMapping) of
    {MqttTopic, KafkaTopic} -> KafkaTopic;
    false -> DefaultTopic
  end.


%%--------------------------------------------------------------------
%% Session Life circle Hooks
%%--------------------------------------------------------------------

on_session_created(#{clientid := ClientId}, SessInfo, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Session(~s) created, Session Info:~n~p~n", [ClientId, SessInfo]),
  ok.


on_session_subscribed(#{clientid := ClientId}, Topic, SubOpts, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Session(~s) subscribed ~s with subopts: ~p~n", [ClientId, Topic, SubOpts]),
  ok.

on_session_unsubscribed(#{clientid := ClientId}, Topic, Opts, _Env) ->
  ?LOG_INFO("[KAFKA PLUGIN]Session(~s) unsubscribed ~s with opts: ~p~n", [ClientId, Topic, Opts]),
  ok.

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
  KafkaConfig = OriKafkaConfig ++ [{auto_start_producers, true}],
  ?LOG_NOTICE("[KAFKA PLUGIN]KafkaConfig = ~p~n", [KafkaConfig]),
  {ok, KafkaTopic} = application:get_env(emqx_plugin_kafka, topic),
  ?LOG_NOTICE("[KAFKA PLUGIN]KafkaTopic = ~s~n", [KafkaTopic]),
  {ok, TopicMapping} = application:get_env(emqx_plugin_kafka, topic_mapping),
  ?LOG_NOTICE("[KAFKA PLUGIN]TopicMapping = ~p~n", [TopicMapping]),
  Prop = application:get_all_env(emqx_plugin_kafka),
  ?LOG_NOTICE("[KAFKA PLUGIN]All config = ~p~n", [Prop]),
  {ok, _} = application:ensure_all_started(brod),
  ok = brod:start_client(AddressList, emqx_repost_worker, KafkaConfig),
  ?LOG_NOTICE("Init emqx plugin kafka successfully.....~n"),
  ok.

get_kafka_topic() ->
  %% 默认的kafka topic
  {ok, Topic} = application:get_env(emqx_plugin_kafka, topic),
  Topic.

get_kafka_topic(Event) ->
  %% 不同事件特定的topic
  case application:get_env(emqx_plugin_kafka, Event) of
    {ok, Topic} -> Topic;
    _ -> get_kafka_topic()
  end.

need_base64() ->
  {ok, NeedBase64} = application:get_env(emqx_plugin_kafka, publish_base64),
  NeedBase64.

transform_payload(Payload) ->
  NeedBase64 = need_base64(),
  if
    NeedBase64 == true ->
      %% base64:encode/1 直接返回 binary，避免 encode_to_string/1 生成的中间
      %% Erlang 字符串（list，每个字符约 8 字节开销）再经 list_to_binary 转换
      base64:encode(Payload);
    NeedBase64 == false ->
      Payload
  end.


%% Called when the plugin application stop
unload() ->
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
  brod:stop_client(emqx_repost_worker).

produce_kafka_payload(Key, Message, Event) ->
  Topic = get_kafka_topic(Event),
  {ok, MessageBody} = emqx_json:safe_encode(Message),
  % ?LOG_INFO("[KAFKA PLUGIN]Message = ~s~n",[MessageBody]),
  Payload = iolist_to_binary(MessageBody),
  brod:produce_cb(emqx_repost_worker, Topic, hash, Key, Payload, fun(_,_) -> ok end),
  ok.

produce_kafka_payload_by_kafka_topic(Key, Message, KafkaTopic) ->
  {ok, MessageBody} = emqx_json:safe_encode(Message),
  % ?LOG_INFO("[KAFKA PLUGIN]Message = ~s~n",[MessageBody]),
  Payload = iolist_to_binary(MessageBody),
  brod:produce_cb(emqx_repost_worker, KafkaTopic, hash, Key, Payload, fun(_,_) -> ok end),
  ok.

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
