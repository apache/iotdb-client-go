<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Go subscription 公共接口

当前已实现 `SubscriptionSession` 的默认配置、构造、普通 Thrift 连接与认证，以及 Topic/Subscription 管理；Consumer 默认配置、内部控制协议和单节点 Provider、Poll 响应与记录解析、消息及记录访问实现也已补齐。公共 Consumer 和处理器仍没有可运行的消费实现。下文对应表同时列出已实现能力和后续消费实现需要遵守的契约。

模块路径为 `github.com/apache/iotdb-client-go/subscription`，使用 Go 1.25，与现有 `github.com/apache/iotdb-client-go/v2` Session 模块分别维护依赖和版本。当前仅依赖现有模块，以复用 `TSDataType` 和 `ColumnCategory`；没有 TsFile、cgo 或 TLS 依赖，也没有改动现有 Session 接口。

Java 对照基线为 IoTDB `d9122d837610` 的 `iotdb-client/subscription`。本次支持范围是记录消息：不定义 TsFile handler、下载、文件操作或 Reader API。Topic 管理和 `Subscribe` 不预先拒绝 TsFile Topic；后续解析分派到文件载荷时必须返回 `ErrUnsupportedTsFile`，不能交付或确认该载荷。

## 配置与模型选择

Go 使用 `Dialect` 区分树、表模型，管理和消费接口各自共用一份契约，避免复制 Java 的树、表类型及 Builder 层次。

Java 的 `ISubscriptionTreeSession/ISubscriptionTableSession`、树/表 Pull 接口、树/表 Push 接口各自具有相同的方法签名。因此 Go 分别用 `SubscriptionSession`、`SubscriptionPullConsumer`、`SubscriptionPushConsumer` 表达这些共同操作，由配置中的 `Dialect` 选择模型；每个实例对应一个模型。

这里共用的是对外方法契约。管理 Session 已在连接时传递模型；后续消费握手和记录解析仍需分别处理树模型的设备路径与表模型的数据库、表和 TAG 身份。相同接口不要求树、表使用同一套内部实现。

| Go 公共类型 | Java 对应 | Go 中的表达方式 |
| --- | --- | --- |
| `Dialect`、`TreeDialect`、`TableDialect` | 树、表 Session/Consumer 类型及 SQL dialect | 在 `SubscriptionSessionConfig` 或 `SubscriptionConsumerConfig` 中选择模型 |
| `ConnectionConfig` | Session/Consumer Builder 的节点、认证、连接超时、最大帧配置 | `NodeURLs` 同时表示单节点和集群种子地址；时间使用 `time.Duration`；明文与加密密码互斥 |
| `SubscriptionSessionConfig` | `SubscriptionTreeSessionBuilder`、`SubscriptionTableSessionBuilder` | 通过 `DefaultSubscriptionSessionConfig` 获取默认值，再交给 `NewSubscriptionSession`；构造不建立连接 |
| `SubscriptionConsumerConfig` | `AbstractSubscriptionConsumerBuilder` | 公共消费配置，包含消费身份、owner、心跳、端点同步及并行度 |
| `SubscriptionPullConsumerConfig` | `AbstractSubscriptionPullConsumerBuilder` | `AutoCommit == nil` 表示沿用 Java 默认值 `true`，指向 `false` 表示显式手动提交 |
| `SubscriptionPushConsumerConfig` | `AbstractSubscriptionPushConsumerBuilder` | 组合 Handler、ACK 策略及拉取间隔、超时 |
| `AckStrategy`、`AckAfterConsume`、`AckBeforeConsume` | `AckStrategy.AFTER_CONSUME`、`BEFORE_CONSUME` | 零值为消费后确认；消费前确认下，回调失败不导致重新投递 |

`SubscriptionConsumerConfig.OwnerEpoch == nil` 表示未提供 epoch，指向零表示显式 epoch 0。已提供 `DefaultSubscriptionConsumerConfig`、`DefaultSubscriptionPullConsumerConfig`、`DefaultSubscriptionPushConsumerConfig`。Push 默认配置的 Handler 保持 nil，调用方需设置实际处理函数。内部归一化会复制节点列表及可选指针；时长零值使用默认值，负值报错，低于 Java 正数下限的配置提升到对应下限。Consumer 构造函数尚未提供。

管理 Session 的默认配置为 `localhost:6667`、树模型、root/root、64 MiB 帧和十秒连接及认证超时。Java 管理 Session 默认不限制连接时长，Go 入口在这里采用有界超时。构造函数补齐空节点、用户名、模型以及为零的超时和帧大小，校验地址与互斥认证，并复制节点列表。密码始终保留调用方提供的值，包括空字符串；需要 root/root 默认值时应从默认配置函数开始。使用加密密码时清空 `Password` 后设置 `EncryptedPassword`。

```go
cfg := subscription.DefaultSubscriptionSessionConfig()
cfg.Dialect = subscription.TableDialect
s, err := subscription.NewSubscriptionSession(cfg)
if err != nil {
    return err
}
if err := s.Open(ctx); err != nil {
    return err
}
defer s.Close()
topics, err := s.Topics(ctx)
```

管理连接自行处理查询分页与关闭，复用现有 TsBlock 解码器；不使用会以 `context.Background()` 分页的现有 SessionDataSet。取消会中断原始连接，后续操作重建连接时继续使用原模型和认证配置。已发送的管理语句不会因连接失败被自动重放；失败后可由调用方查询服务端状态再决定是否重试。管理 Session 关闭后可以再次打开，这与 Consumer 成功关闭后不可重新打开的契约分开处理。

## 内部订阅协议与 Provider

当前内部实现覆盖握手、心跳、订阅/退订、关闭、提交、seek、Poll 请求与 Tablet 续取请求，以及超大请求分片。它们尚未接入公共 Consumer，不能仅凭这一层直接消费记录。

- 基础编解码采用 Java 的 big-endian 布局。提交上下文使用版本 2，保留旧式提交字段及可选 Writer 身份和进度；Topic/Region/Writer 进度按原协议结构编码。
- 订阅和 Poll 的 Topic 集合使用 Java ObjectSet 字符串标记，不与心跳退订列表的普通 StringList 混用。集合编码稳定排序；null 字符串在现有 Go string 字段中映射为空串。
- 每个 Provider 自持普通 Thrift 连接和串行 RPC 通道，打开阶段包含认证、帧协商和订阅握手，均受打开超时约束。服务端分配的消费身份保留在 Provider 中，实例身份与 owner 信息通过握手传递。
- 帧限制取本地与服务端较小值并预留协议空间；旧服务端没有属性字段或不支持属性探测 RPC 时回退本地限制。可分片请求的所有片段保持同一连接、顺序和批次身份。
- 心跳按报文大小分批发送缓存上下文；提交预留响应空间分批，后续批次失败时仍返回先前已接受的上下文和进度。没有响应 body 时保留空结果，不推断整批提交成功。
- 状态码保留普通错误、owner fencing、consumer fencing、连接替换和致命错误的区别。Poll 返回消费组 Broker 丢失诊断时使旧连接失效，供后续 Consumer 重建；失效连接不会被旧响应重新启用。
- Provider 现在解析全部非空 Poll 响应 body，保留记录、错误、终止和水位线事件。文件响应在类型分派处返回 `ErrUnsupportedTsFile`；混合响应不会返回已经解析的部分记录，不实现文件拉取。

## 记录消息与解析

记录数据采用 Java 当前依赖的 `2.5.1-261023-SNAPSHOT` Tablet 序列化格式，与写入 RPC 和查询 TsBlock 格式分开处理，不依赖 TsFile Go 或原生库。

- 支持 BOOLEAN、INT32、INT64、FLOAT、DOUBLE、TEXT、STRING、TIMESTAMP、DATE、BLOB 和 OBJECT，保留 TAG/FIELD/ATTRIBUTE 类别。TEXT/STRING 暴露为 string，BLOB/OBJECT 为 `[]byte`，DATE 为 UTC 零点的 `time.Time`。
- NULL 由 Bitmap 和缺失值列决定。Binary 的缺省标记先恢复为空内容，再应用 Bitmap；不把空字符串或空 BLOB 自动变成 NULL。日期按 Java 的日期整数规则读取。
- 原始行号用于 `Timestamp/Value`；`Rows()` 通过独立的稳定排序视图按时间递增遍历，相同时间戳保持原始顺序。行值切片不在迭代间复用，调用用户的迭代函数时不持内部锁。
- 树模型始终显示时间戳；表模型优先采用按数据库、表指定的时间投影，未指定时回退消息级标志。旧响应缺少投影尾部时保持 Java 的默认选择时间行为。
- 消息释放幂等并清理记录缓存；提交上下文及其可选 Writer 元数据仍以独立快照返回，水位线和消息类型继续可访问。已借用的批次在释放后数据访问报错。
- 记录续取组装校验完整提交上下文，逐页合并数据和时间投影；最终数量按 Tablet 个数核对，不按行数核对。该组装能力尚未接入 Consumer 的实际续取循环。
- 空记录交付和终止事件在解析层保留上下文，不自动 ACK 或退订；服务端 ERROR 保留严重级别，水位线可转为处理器消息。消费循环中的处理仍待下一步实现。

## 管理 Session

`SubscriptionSession` 对应 Java 的 `ISubscriptionTreeSession` 和 `ISubscriptionTableSession`。方法接收显式 `context.Context`，存在性条件通过参数结构表达。

| Go 方法或类型 | Java 对应 | 合并方式 |
| --- | --- | --- |
| `Open(ctx)`、`Close()` | `open()`、`AutoCloseable.close()` | 打开操作可取消；关闭统一负责释放资源 |
| `CreateTopic(ctx, name, options)`、`CreateTopicOptions` | 四个 `createTopic/createTopicIfNotExists` 重载 | `Attributes` 表示可选属性，`IfNotExists` 表示条件创建 |
| `AlterTopic(ctx, name, attributes)` | `alterTopic(name, properties)` | 属性用 `map[string]string` |
| `AlterTopicOwner(ctx, name, owner)`、`TopicOwner` | 两个 `alterTopicOwner` 重载 | `LeaseDuration` 指针表达可选租约时长 |
| `DropTopic(ctx, name, options)`、`DropOptions` | `dropTopic/dropTopicIfExists` | `IfExists` 表示条件删除 |
| `Topics(ctx)`、`Topic(ctx, name)` | `getTopics()`、`getTopic(name)` | 集合用 slice；单 Topic 不存在时返回 `found == false` |
| `Subscriptions(ctx, topic)` | `getSubscriptions()`、`getSubscriptions(topic)` | `topic == ""` 表示查询全部 |
| `DropSubscription(ctx, id, options)` | `dropSubscription/dropSubscriptionIfExists` | 复用 `DropOptions` |
| `Topic` | `model.Topic` | 保留服务端返回的属性原始文本，不假定其可直接解析为 map |
| `Subscription` | `model.Subscription` | 保留服务端的 `ConsumerIDs` 原始文本，不假定其是 CSV |

## Consumer、提交与进度

`SubscriptionConsumer` 是 Pull 和 Push 的共同接口。它包含 `Positioner`，因此两类消费者共用进度与 seek 契约；这也覆盖 Java 具体 Consumer 从基类继承的公开能力。

| Go 方法或类型 | Java 对应 | 合并方式 |
| --- | --- | --- |
| `SubscriptionConsumer.Open(ctx)`、`Close()` | 各 Consumer 的 `open/close` | 共用生命周期接口 |
| `Subscribe(ctx, topics...)`、`Unsubscribe(ctx, topics...)` | 单个、数组、集合重载 | 统一为 variadic Topic 名称 |
| `Info()`、`SubscriptionConsumerInfo` | `getConsumerId/getConsumerGroupId/getOwnerId/getOwnerEpoch` | 一次返回不含凭据的身份快照，包含服务端分配的 ID |
| `LatestWatermark()` | `getLatestWatermarkTimestamp()` | 额外返回 `ok`，区分尚无水位线与有效时间值 |
| `AllConsumed()` | `allTopicMessagesHaveBeenConsumed()` | 无订阅时为 true；活跃的无界 Topic 阻止完成 |
| `SubscriptionPullConsumer.Poll(ctx, timeout, topics...)` | 四个 `poll` 重载 | `time.Duration` 替代毫秒和 Java Duration 重载；不传 Topic 表示当前全部订阅 |
| `PollWithInfo(ctx, timeout, topics...)`、`PollResult` | 具体 Pull 类的 `pollWithInfo`、`payload.PollResult` | 一并返回消息、缓存条数及处理器水位线 |
| `DrainBufferedMessages(ctx)` | `drainBufferedMessages()` | 显式排空处理器或待交付缓存 |
| `Commit(ctx, messages...)` | 单条/批量 `commitSync` | 同步提交统一为 variadic 消息 |
| `CommitAsync(ctx, messages...)` | `CompletableFuture` 及 `AsyncCommitCallback` 重载 | 返回 `<-chan error`；调用方可直接等待，或用 goroutine 等待后执行回调 |
| `AddProcessor(processor)` | 具体 Pull 类的 `addProcessor` | 按调用顺序添加处理阶段，返回错误而非链式 Builder |
| `ConsumeFunc`、`SubscriptionPushConsumerConfig.Handler` | `ConsumeListener.onReceive`、`ConsumeResult` | `func(ctx, message) error`，nil 表示成功，非 nil 表示消费失败 |
| `SubscriptionPushConsumer.Errors()` | Java 自动拉取异常路径的 Go 显式错误出口 | 首次终止性后台错误通知一次；普通 Handler 失败不视为终止错误 |
| `Positioner.Positions(topic)` | `positions(topic)` | 返回当前已拉取位置的深快照 |
| `CommittedPositions(topic)` | `committedPositions(topic)` | 单独保留已确认、可恢复的位置 |
| `SeekToBeginning(ctx, topic)`、`SeekToEnd(ctx, topic)` | 同名 Java 方法 | 保留两种定位行为 |
| `Seek(ctx, topic, progress)`、`SeekAfter(ctx, topic, progress)` | `seek/seekAfter` | 分别从指定进度重放和从其后继续 |
| `TopicProgress`、`RegionProgress` | 同名 Java 类 | Topic → Region → Writer 的 map，不合并成单 offset |
| `WriterID`、`WriterProgress` | `WriterId`、`WriterProgress` | 可比较 Writer ID 作为 map key；进度保留物理时间和本地序号 |
| `WriterPosition`、`CommitContext`、`InvalidCommitID` | `SubscriptionCommitContext` 的可选 writer 进度及旧式 commit 元数据 | writer 身份与进度成对保留，同时保留路由、重启和 seek-generation 信息 |

`CommitAsync` 的返回 channel 容量为 1，只发送一次结果（成功时为 nil）后关闭。调用方不读取结果不能阻塞清理。取消 context 不撤回服务端已接受的提交；同步或异步提交报错也不代表整批都失败。

`SubscriptionPushConsumer.Errors()` 的 channel 至少缓冲一个错误。TsFile 解析错误必须先停止自动拉取再报告，失败轮次不能进入 Handler 或 ACK；channel 关闭由统一生命周期管理，避免与后台发送竞争。

## 消息、记录与 Processor

| Go 方法或类型 | Java 对应 | Go 中的表达方式 |
| --- | --- | --- |
| `SubscriptionMessage`、`SubscriptionMessageKind`、`RecordMessage`、`WatermarkMessage` | `SubscriptionMessage`、记录和水位线消息类型 | 公共消息种类与协议数字解耦；Pull 和 Push 只交付记录，Processor 可接收水位线 |
| `SubscriptionMessage.Kind()`、`CommitContext()` | `getMessageType/getCommitContext` | 方法返回公开种类和提交上下文快照 |
| `SubscriptionMessage.Records()` | `getResultSets/getRecordTabletIterator`、`SubscriptionRecordHandler` | 返回多数据库、多 Tablet 的 `[]RecordBatch`，不复制 Java ResultSet 继承层次 |
| `SubscriptionMessage.Watermark()` | `getWatermarkTimestamp()` | 用 `ok` 标识是否为水位线消息 |
| `EstimatedSize()`、`Release()` | `estimateSize/removeUserData` | 释放记录缓存后保留消息类型与提交身份，仍可提交 |
| `RecordBatch` | `SubscriptionRecordHandler.SubscriptionResultSet` 和底层 Tablet 视图 | 提供模型、数据库、目标、列信息、时间投影、行数和按索引访问 |
| `Column` | measurement schema、列类型与类别 | 复用现有 `client.TSDataType/ColumnCategory`；时间列单独表示 |
| `RecordBatch.Rows()`、`Row` | `ResultSet`、`RowRecord`、`TSRecord` 的遍历视图 | 使用 `iter.Seq2[Row, error]`；索引从 0 开始 |
| `RecordBatch.TimeSelected()`、`Row.Timestamp` | 消息和表级时间列选择信息 | 每个批次独立保留投影；未选择时间列时 `Row.Timestamp == nil` |
| `RecordBatch.Timestamp(row)` | 底层 Tablet 的时间戳访问 | 即使不投影时间列，处理器仍可读取原始时间戳 |
| `SubscriptionMessageProcessor.Process/Flush/Reset` | `SubscriptionMessageProcessor` 的同名能力 | 处理和排空显式返回 error；consumer 串行调用每个 Processor |
| `BufferingProcessor.Buffered()`、`ProcessorBuffer` | `getBufferedCount/getBufferedCommitContexts` | 缓存能力用可选接口表达，同时报告条数和全部可续租上下文 |
| `TopicResetter.ResetTopic(topic)` | `supportsTopicScopedReset/reset(topic)` | 是否实现接口代替 Java 的能力布尔值 |
| `WatermarkReporter.Watermark()` | `WatermarkProcessor.getWatermark()` | 可选暴露处理器计算的排序水位线；不等同于 Consumer 最新服务端水位线 |

SubscriptionMessageProcessor 保留消息时必须实现 `BufferingProcessor`，让后续心跳实现能够续租未交付记录。seek 只清理目标 Topic；同时订阅多个 Topic 时，未实现 `TopicResetter` 的 Processor 会使 seek 在修改服务端位置前失败；单 Topic 场景允许回退到 `Reset()`。`Flush` 返回的消息经后续处理阶段继续处理，Processor 不得再保留或修改已移交的消息。

## 数据所有权与 context 契约

- Session 查询集合、Consumer 身份、提交上下文和进度返回调用方拥有的快照；进度 map 为深快照。seek 复制输入，不长期借用调用方的 map。
- `SubscriptionMessage.Records()` 的批次、列和行数据是借用的只读视图，有效期到所属消息 `Release()`。需要跨越该有效期保留数据时，调用方负责复制值和字节数组。
- `Release()` 幂等。释放后不再使用已借用的批次、列和行数据；`Records()` 返回 `ErrDataReleased`，消息类型、提交上下文和水位线仍可访问。
- `Row.Values` 的 nil 表示 SQL NULL；DATE 使用 `time.Time`；TIMESTAMP 和记录时间戳使用服务端精度的 `int64`；BLOB/OBJECT 使用 `[]byte`。
- Push Handler 不得释放收到的消息；回调返回后若仍需记录数据，必须先复制。回调 context 随消费交付停止而取消。
- `Open(ctx)` 的 context 只限制打开操作，不持有后台任务生命周期；后台任务随 `Close()` 停止。其他操作 context 仅限制对应操作。
- Pull 超时受 `timeout` 和 context deadline 中较早者约束。操作超时使用 `time.Duration`，不改变消息中的时间戳单位。
- 已打开时重复 `Open`、已关闭时重复 `Close` 无副作用。成功关闭后不能重新打开；要求先排空缓存的关闭失败仍保留排空和提交的恢复路径。

## 错误与当前未实现内容

| Go 错误哨兵 | Java 对应或行为 |
| --- | --- |
| `ErrUnsupportedTsFile` | 本轮明确排除的 TsFile handler；仅解析文件载荷时返回，管理及订阅不提前拒绝 |
| `ErrClosed`、`ErrNotOpen` | Consumer 生命周期检查 |
| `ErrDataReleased` | 记录数据已释放后的访问 |
| `ErrIncompatibleMessage` | `SubscriptionIncompatibleHandlerException` 所表达的消息访问不匹配 |
| `ErrConsumerFenced`、`ErrOwnerFenced` | `SubscriptionConsumerFencedException`、`SubscriptionOwnerFencedException` |

这些哨兵可以被后续操作错误包装，调用方使用 `errors.Is` 识别。TsFile 不支持错误不得触发连接重试或文件载荷确认。Pull 本轮返回错误而不交付任何消息，包括并行分支已经取得的记录；本轮记录不得推进位置、进入处理器或自动提交缓存，此前成功交付及提交的状态不回滚。调用方可退订相关 Topic 后继续拉取。

后续步骤尚需实现公共 Consumer 和构造、实际记录续取循环、多节点协调、提交进度与恢复、后台任务、内置 Watermark/ColumnAlign Processor，以及消费示例和行为测试。内部 Provider 已有提交 RPC，但尚未实现公共 Consumer 的交付和确认链。管理查询复用根模块解码器，本轮不修改其对畸形数据的处理逻辑。当前仅完成静态核对，没有编译、运行测试或执行格式工具，不提供运行验证结论。
