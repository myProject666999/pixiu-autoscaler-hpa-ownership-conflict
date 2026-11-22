# HPA 所有权与冲突处理说明

本文档说明 pixiu-autoscaler 控制器对 HPA 的所有权判定、管理边界、字段范围、
多对象保留规则与事件原因，以及旧辅助函数的保留/调整结论和显式接管开关的取舍。

## 所有权判定

唯一的所有权判定依据是 HPA 的 controller ownerReference 的 **UID + Kind + 名称**
与当前 Deployment 完全匹配（`isManagedByDeployment`，`getHPAsForDeployment` 同口径）。

命名空间内的 HPA 在每次同步时被分为三类（`classifyHPAs`）：

| 分类 | 判定条件 | 处理 |
| --- | --- | --- |
| `managed` | controllerRef 的 UID/Kind/Name 与当前 Deployment 完全匹配 | 当前授权范围：保留一个、更新受管字段、删除多余项 |
| `orphans` | controllerRef 的 Kind/Name 相同但 UID 不同（同名 Deployment 删除重建的遗留） | 不接管、不修改、不删除，等待 GC |
| `conflicts` | 非托管，但占用了期望 HPA 名称，或 `ScaleTargetRef` 的 apiVersion/kind/name 指向同一 Deployment | 不接管、不修改、不删除；跳过创建并发警告事件 |

**managedFields 的 manager 名称不构成接管授权。** 任何对象都不会因为
managedFields、标签或名称相似而被猜测归属后修改或删除。

## 管理边界与冲突处理

- **同名外来对象**：用户自建的 HPA 占用了期望名称（如 `web-<hash>`）时，控制器
  不接管、不修改、不创建，发出含冲突对象名的 `HPAConflict` 警告事件后继续处理其他对象。
  同一冲突在 `eventDedupInterval`（默认 10 分钟）内不重复刷事件；用户删除该对象后，
  下一次同步自动创建。
- **异名同目标**：名称不同但 `ScaleTargetRef`（apiVersion/kind/name）指向同一
  Deployment 的非托管 HPA（如用户的 `web-hpa`）同样被识别为冲突，避免新增第二个
  伸缩控制器。
- **注解移除**：Deployment 的 HPA 注解被移除时，只删除当前授权范围内的托管 HPA，
  非托管对象（含冲突对象、旧 UID 遗留对象）始终保留。

## 同名 Deployment 删除重建（旧 UID 遗留）

旧 HPA 的 controllerRef 指向已删除 Deployment 的 UID，不能立即接管。区分三种情况：

1. **确属旧的已删除目标**：当前同名 Deployment 的 UID 已不同，旧 HPA 等待垃圾回收；
2. **尚未完成垃圾回收**：旧 HPA 可能带有 DeletionTimestamp，GC 进行中；
3. **目标查询失败**：读取失败时返回错误，**不能因读取失败就删除任何对象**。

对前两种情况，控制器进入独立的等待状态：以 `orphanRequeueDelay`（默认 10 秒）
短期重新入队，返回成功，**不消耗 `maxRetries` 的配置失败重试计数**，并发出
`OrphanedHPA` 事件（同样去重）。旧对象被 GC 后，下一次同步正常创建。

## 字段所有权（公开清单）

控制器只管理 `controller.ManagedHPAFields` 中公开的字段：

- `spec.minReplicas`
- `spec.maxReplicas`
- `spec.metrics`
- `spec.scaleTargetRef`
- `metadata.labels[hpa.caoyingjunz.io/targetCustomMetric]`（prometheus 自定义指标标记）

更新通过 `controller.ApplyManagedHPAFields` 做**字段级合并**，不做整体对象替换：
用户添加的 `Spec.Behavior`、无关的注解与标签原样保留。例如托管 HPA 被用户加上
`Behavior` 后，注解把上限从 6 改为 8 时，只有 `maxReplicas` 被更新，`Behavior` 不变。

更新前从保留对象取得并携带正确的 `resourceVersion`；遇到冲突（Conflict）时重新
读取最新对象、重新判断归属，并在有界次数内（`maxUpdateConflictRetries`，默认 3 次）
合并重试，不盲目重放旧对象。重新读取失败时返回错误，不删除任何对象。

> 注：旧流程用无 `resourceVersion` 的对象整体 Update。该写法的实际 API 结果不能
> 断言为必然无条件覆盖；`TestSyncUpdateAlwaysCarriesResourceVersion` 以模拟
> API 服务器拒绝空 `resourceVersion` 的失败用例确认：控制器发出的任何更新都必须
> 携带 `resourceVersion`。

## 多个托管 HPA 的保留规则

`sortManagedHPAs` 提供确定性顺序，保留第一个，删除其余（删除前再次确认归属）：

1. **期望名称**（`<deployment>-<hash>`）优先；
2. 其次**创建时间**早的优先；
3. 仍相同再按**名称**字典序。

## 事件原因

| Reason | 类型 | 含义 |
| --- | --- | --- |
| `CreateHPA` / `FailedCreateHPA` | Normal / Warning | 创建托管 HPA 成功 / 失败 |
| `UpdateHPA` / `FailedUpdateHPA` | Normal / Warning | 字段级更新成功 / 失败 |
| `DeleteHPA` / `FailedDeleteHPA` | Normal / Warning | 删除托管 HPA 成功 / 失败 |
| `FailedNewestHPA` | Warning | 从注解解析期望 HPA 失败 |
| `HPAConflict` | Warning | 存在非托管冲突对象（同名或同目标），跳过创建；消息含冲突对象名，按间隔去重 |
| `OrphanedHPA` | Warning | 旧 UID 遗留 HPA 等待垃圾回收；消息含遗留对象名，按间隔去重 |

## 旧辅助函数的保留/调整结论

- **`ManageByPixiuController`**：**保留但修正，仅作诊断**。原常量
  `PixiuManager = "kubez-autoscaler-controller"` 与本项目实际 user-agent
  （`pixiu-autoscaler-controller`，见 `cmd/app/options`）不符，已修正；
  本地调试遗留的 `PixiuMain = "main"` 已移除（该名称过于宽泛，可能误匹配
  任何名为 main 的二进制写入的对象）。该函数不得用于所有权判定或接管授权。
- **`IsOwnerReference`**：**保留**为通用工具函数；同步路径不使用，
  所有权判定统一走 controller ownerReference 的 UID/Kind/Name 完全匹配。

## 显式接管开关的取舍

**默认关闭，当前版本不提供自动接管。** 自动接管（修改/删除用户的 HPA）一旦误判
归属即破坏用户对象，风险大于"少删一个对象"的便利；而 managedFields、标签、名称
相似都不足以可靠判定用户意图。如未来确有需要，应以显式注解（例如
`hpa.caoyingjunz.io/takeover: "true"`）由用户明确授权后开启；在开关开启之前，
控制器对一切非托管对象只读、只告警。

## 测试

`pkg/controller/autoscaler/autoscaler_controller_test.go` 使用 client-go 的
fake clientset、informer 缓存与 reactor（无需集群），覆盖：无冲突创建、同名外来
对象（含事件去重与用户删除后创建）、异名同目标、Behavior 保留、多个托管对象的
确定性保留、UID 重建等待、读取失败不删除、更新冲突重读重试与有界上限、
resourceVersion 必携带的失败用例、注解移除只删托管对象，并核对写操作序列与事件。
