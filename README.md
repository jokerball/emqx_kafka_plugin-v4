# 适配emqx v4.3版本

## 更新说明

*v4.3.12.7*: [Release emqx-4.3.12.7-with-kafka · ULTRAKID/emqx_plugin_kafka (github.com)](https://github.com/ULTRAKID/emqx_plugin_kafka/releases/tag/emqx-4.3.12.7-with-kafka)

`2022.08.02` 新增了是否对`publish`消息进行`base64`编码后再`kafka`转发的配置项`publish_base64`，发送数据为字节流的时候建议启用该配置。

*v4.3.12.6*:  [Release emqx-4.3.12.6-with-kafka · ULTRAKID/emqx_plugin_kafka (github.com)](https://github.com/ULTRAKID/emqx_plugin_kafka/releases/tag/emqx-4.3.12.6-with-kafka)

`2022.06.16` 将原来的`kafka`依赖从`ekaf`替换成[kafka4beam](https://github.com/kafka4beam)/**[brod](https://github.com/kafka4beam/brod)**。

emqx-plugin-template
====================

[emqx/emqx-plugin-template at emqx-v4 (github.com)](https://github.com/emqx/emqx-plugin-template/tree/emqx-v4) This is a template plugin for the EMQ X broker. 

Plugin Config
-------------

配置文件模板及说明见`etc/emqx_plugin_kafka.config`

### Kafka 写入与内存保护

所有 hook 只把消息写入本地有界队列（`emqx_plugin_kafka_queue`），由独立 worker 进程异步写 Kafka，避免 Kafka 抖动时阻塞 MQTT 连接进程：

- `queue_max_len` / `queue_max_bytes`：队列条数和字节数上限，超过任一上限直接丢弃并计数，hook 永不阻塞；
- `queue_report_interval`：丢弃/发送失败统计的告警间隔（ms）。日志中出现 `queue overflow` 或 `produce to kafka failed` 即说明 Kafka 侧异常；
- `produce_timeout` / `fail_threshold` / `probe_interval`：worker 侧熔断参数。单条消息写入等待超过 `produce_timeout` 计一次失败，连续失败 `fail_threshold` 次后熔断；熔断期间 hook 直接丢弃、已入队消息快速丢完，每隔 `probe_interval` 放行一次探测，探测成功即自动恢复。日志中出现 `breaker open` / `breaker half-open` / `breaker closed` 可跟踪熔断状态；
- 生产者参数（`required_acks`、`max_retries`、`partition_buffer_limit`、`retry_backoff_ms` 等）必须写在 `kafka_config` 的 `default_producer_config` 里；写在顶层 brod 会静默忽略。插件启动时会把旧配置里的顶层生产者参数自动迁移进去，并打印 notice 日志；
- 队列统计（`emqx_plugin_kafka_queue:stats/0`）中的 `buffered` 表示已进入 brod 缓冲，`acked` 表示已收到 Kafka 确认；`required_acks = 0` 时 Kafka 不回确认，`acked` 恒为 0。


Authentication and ACL
----------------------

```
emqx:hook('client.authenticate', fun ?MODULE:on_client_authenticate/3, [Env]).
emqx:hook('client.check_acl', fun ?MODULE:on_client_check_acl/5, [Env]).
```

Build the EMQX broker
-----------------
###### 1. 基于CentOS7.5环境下编译，先安装相关插件

```
  yum -y install make gcc gcc-c++ glibc-devel glibc-headers kernel-devel kernel-headers m4 ncurses ncurses-devel openssl openssl-devel openssl-libs zlib zlib-devel libselinux-devel xmlto perl git wget
  
  注意：openssl的版本不是1.1.1k，则需要通过源码openssl-1.1.1k.tar.gz来安装openssl
```
###### 2. 准备Erlang/OTP 22及以上环境

参照[Erlang and Elixir Packages Download - Erlang Solutions (erlang-solutions.com)](https://www.erlang-solutions.com/downloads/) 官网安装方式。

###### 3. 下载EMQX源码

官方源码仓库地址为[emqx/emqx: An Open-Source, Cloud-Native, Distributed MQTT Message Broker for IoT. (github.com)](https://github.com/emqx/emqx) ，分支为`main-v4.3`

本人修改了官方的编译脚本，并且在插件目录里添加了该kafka插件的信息，仓库地址为[ULTRAKID/emqx at main-v4.3 (github.com)](https://github.com/ULTRAKID/emqx/tree/main-v4.3) ，分支为`main-v4.3`。

###### 4. 修改EMQX文件，增加kafka插件
参照[emqx/README.md at main-v4.3 · ULTRAKID/emqx (github.com)](https://github.com/ULTRAKID/emqx/blob/main-v4.3/lib-extra/README.md) 。

注：[ULTRAKID/emqx at main-v4.3 (github.com)](https://github.com/ULTRAKID/emqx/tree/main-v4.3) 仓库内已进行此项修改。

###### 5. 编译EMQX，并且启动EMQX

进入emqx目录，执行make命令，需要保持外网通畅，有条件建议科学上网。

二进制编译命令：`make`

docker镜像打包：`make emqx-docker`


License
-------

Apache License Version 2.0

Author
------

Ultrakid.

EMQ X Team.
