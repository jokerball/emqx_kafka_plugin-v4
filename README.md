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

所有 hook 只把消息写入本地有界队列（`emqx_plugin_kafka_queue`），再由一组 worker 进程（`emqx_plugin_kafka_worker`）异步写 Kafka，避免 Kafka 抖动时阻塞 MQTT 连接进程：

- `queue_workers`：worker 进程数，按 `device_id` 哈希分片，同一设备的消息固定落在同一个 worker，保证单设备顺序；吞吐随 worker 数线性提升；
- `queue_max_len` / `queue_max_bytes`：队列条数和字节数上限，超过任一上限直接丢弃并按原因计数，hook 永不阻塞；
- `queue_report_interval`：丢弃/发送失败统计的告警间隔（ms）。日志中会输出 `full_len` / `full_bytes` / `unavailable` / `probe` / `breaker_open` 五类丢弃归因；
- `batch_max_count` / `batch_max_bytes`：worker 单次提交到 Kafka 的条数与字节上限。worker 会先从本地 FIFO 取一批、再从邮箱里把已到达的消息并进同一批，按 `{Topic, Partition}` 分组后**每组只做一次 monitor/send/receive**，把「每条一次」的调度开销摊薄到「每组一次」，这是降低 CPU 的主要手段。邮箱为空时不会等待，单条消息的延迟不变；`batch_max_bytes` 必须明显小于 `default_producer_config` 的 `max_batch_size`，否则加上批次头开销会触发 `RecordTooLargeException`；
- `produce_timeout` / `fail_threshold` / `probe_interval` / `probe_per_worker`：worker 侧熔断参数。**同一批内的所有分组共用一个 `produce_timeout` 预算**，不会出现「组数 × 超时」的放大；单个 worker 连续失败 `fail_threshold` 次后熔断，worker 本地也会按同阈值前置挂起，避免 Kafka 快速失败时把邮箱里的消息逐条烧完。**熔断只决定「发不发 Kafka」，不决定「收不收已入队消息」**：已入队消息 park 在 worker 本地 FIFO，恢复后按序补发；每隔 `probe_interval` 广播一次探测，每个 worker 只放行 `probe_per_worker` 条真实消息，探测成功即关闭熔断并 drain；探测窗口内若没有任何 worker 上报结果（例如 pending 为空），保底超时会自动回退到 open 并重新调度，不会卡在 probing。日志中出现 `breaker open` / `breaker closed` 可跟踪状态；
- `breaker_action`：熔断打开时**新到达**消息的处理方式，默认 `drop`（直接丢弃，计入 `dropped_breaker_open`），避免恢复后一次性补发一批已经过期的心跳/事件；设为 `park` 则照常入队，恢复后按序补发。已入队的消息不受该项影响，永远走 park/drain；
- `worker_message_queue_data`：`on_heap`（默认）或 `off_heap`，积压场景可切 `off_heap` 做 A/B；
- `forward_delivered` / `forward_acked`：是否转发 `message.delivered` / `message.acked` 事件，默认关闭。一条 MQTT 消息被 N 个订阅者消费时，这两个钩子会把 Kafka 写入量放大到 `1 + 2N` 倍；
- 生产者参数（`required_acks`、`max_retries`、`partition_buffer_limit`、`retry_backoff_ms`、`max_linger_ms`、`max_batch_size` 等）必须写在 `kafka_config` 的 `default_producer_config` 里；写在顶层 brod 会静默忽略。插件启动时会把旧配置里的顶层生产者参数自动迁移进去，并打印 notice 日志；
- 队列统计（`emqx_plugin_kafka_queue:stats/0`）：`queued` / `queued_bytes` 为当前积压，`buffered` 表示已进入 brod 缓冲，`acked` 表示已收到 Kafka 确认，`inflight = buffered - acked` 用于观测静默丢失；`failed` 细分为 `failed_timeout` / `failed_producer_down` / `failed_get_producer`，`dropped` 细分为 `dropped_full_len` / `dropped_full_bytes` / `dropped_unavailable` / `dropped_probe` / `dropped_breaker_open`，另有 `producer_down`、`breaker_open_count`、`breaker_open_ms`、`breaker_action`、`batch_max_count`、`batch_max_bytes`。`required_acks = 0` 时 Kafka 不回确认，`acked` 恒为 0。`queued` 是额度口径（提交超时即归还），`buffered`/`acked` 是 brod 口径；超时后才收到 buffered 回执的消息（批量模式下按组内条数）单列在 `late_buffered`。`lost = failed - late_buffered` 是提交失败且回执始终没到的条数，也就是真实丢失量，端到端对账用两个等式：`入队总量 = queued + buffered + lost`、`上游总量 = 入队总量 + dropped`。注意 `fail_threshold` 统计的是**连续失败的提交次数**，不是失败消息条数——批量提交一次失败可能涉及上百条，按条累加会让阈值失去意义。
- client 级兜底恢复参数（`producer_down_threshold` / `producer_down_window_ms` / `ack_stall_ms` / `breaker_escalate_ms` / `rebuild_backoff_base_ms` / `rebuild_backoff_max_ms` / `rebuild_window_ms` / `max_rebuilds` / `verify_ms` / `rebuild_timeout_ms`）：见下方「client 级兜底恢复」。



### client 级兜底恢复

熔断只能解决「Kafka 不可达」，解决不了「brod client 本身僵死」：producer 崩溃后 brod 内部 buffer 会静默消失，client 进程若已损坏，光重启 producer 也恢复不了。这一层由协调进程 `emqx_plugin_kafka_queue` 做单一决策，worker 只上报信号，避免惊群。

**状态机**

```
healthy ──信号──> degraded ──退避到点──> rebuilding_client ──stop+start ok──> verifying
   ^                  ^                        │                                  │
   │                  └────────────失败/超时─────┘                                  │
   └────────────────────────────探测成功───────────────────────────────────────────┘
                                                                   窗口内重建超限 ↓
                                                              exhausted（仅低频探测 + 手动 rebuild）
```

**触发信号（只在 `healthy` 态生效；已降级、重建中、放弃态各自有既定节奏，不重复介入）**

- producer DOWN：worker 侧长期监控 producer，`producer_down_window_ms` 窗口内累计达到 `producer_down_threshold` 次才判定为 client 故障。单次崩溃由 brod 自动重启 producer 即可，不触发重建；
- ack 停滞：协调进程每个 `queue_report_interval` 采样一次 `acked`，`inflight > 0` 且连续 `ack_stall_ms` 没有新增 acked，判定为 client 半死（消息进了 buffer 但永远等不到确认）。熔断打开期间不算停滞——那是 Kafka 不可达的预期表现；
- 熔断长期打开：熔断持续非 `closed` 超过 `breaker_escalate_ms`（默认 120s，0 关闭）仍探测不回来，升级为重建。这条覆盖「produce 快速失败」场景：broker/client 立刻返回错误时 `inflight` 恒为 0、producer 也不会 DOWN，前两条信号永远不会命中，只剩熔断一直开合，正是压测里节点无法自愈的形态。

**重建与验证**

- 退避：`rebuild_backoff_base_ms` 起按 2 的幂递增，封顶 `rebuild_backoff_max_ms`，默认 1s→2s→4s→8s→16s→30s；
- 重建在独立进程里执行 `brod:stop_client/1` + `brod:start_client/3`，启动参数取自插件启动时写入 `persistent_term` 的 `kafka_client_config`，协调进程本身不会被阻塞；单次重建超过 `rebuild_timeout_ms` 视为失败，进入下一轮退避；
- 重建成功后广播 `client_rebuilt`：worker 解除对旧 producer 的监控并清表（否则旧 producer 的 DOWN 会被当成新故障，刚重建完就再次降级），协调进程清空 `{part_cnt, Topic}` 元数据缓存，然后把熔断切到 `probing` 放行 `probe_per_worker` 条真实消息做验证；
- `verify_ms` 内收到任一成功回执即回到 `healthy`，同时清零退避档位与窗口内重建计数；验证失败或超时则进入下一轮退避；
- 验证超时但队列已空（故障期间消息全被丢弃，或业务本就空闲）时直接回到 `healthy`：没有样本可判定，不能因此再排一轮重建，否则空闲节点会一路烧到 `exhausted`。真正的链路问题会在下一条消息失败时由熔断 + 熔断升级信号重新捕获。

**放弃态**

`rebuild_window_ms` 内重建次数超过 `max_rebuilds` 进入 `exhausted`：停止重建，按 `breaker_action` 继续丢弃/暂停补发，只保留 `probe_interval` 的低频探测。链路自行恢复时探测成功会自动解除；否则需要人工介入：

```
emqx_ctl kafka recovery   # 打印状态机：state / reason / 退避进度 / 重建预算 / ack 停滞时长
emqx_ctl kafka rebuild    # 强制重建（绕过退避，并清空窗口内重建预算）
```

新增统计字段：`recovery`、`recovery_degrade_count`、`client_rebuild_count`、`recovery_exhausted_count`。

### 采样命令（emqx_ctl）

插件加载时注册 `kafka` 命令，压测采样不需要再进远程 shell：

```
emqx_ctl kafka stats     # 一行一个 key=value，覆盖队列/熔断/丢弃/丢失全部计数
emqx_ctl kafka workers   # 各 worker 邮箱长度 + max/min/avg，用于判断分片倾斜
emqx_ctl kafka recovery  # client 级兜底恢复状态机（state / reason / 退避进度 / 重建预算）
emqx_ctl kafka rebuild   # 强制重建 brod client（绕过退避，清空窗口内重建预算）
```

`stats` 的输出可直接用 `grep`/`awk` 解析做时序采样，例如每 10 秒采一次 `queued`、`dropped`、`lost`、`breaker`：

```
while true; do emqx_ctl kafka stats | grep -E '^(queued|dropped|lost|breaker) '; sleep 10; done
```

`workers` 只读 `process_info/2`，不会向正在等 produce 回执的 worker 发 `gen_server:call`，因此采样本身不会被阻塞的 worker 拖住。

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
