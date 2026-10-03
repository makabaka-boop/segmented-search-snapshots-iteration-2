# Snapshot Document Store

Node.js 20、无第三方依赖的文档索引服务。它维护最多 1000 份带修订号的活跃文档，写入先进入 WAL 与内存缓冲，再刷成不可变倒排索引段。

## 一致性模型

- **修订号**：同一文档的更新或删除必须使用严格递增的正整数修订号。
- **墓碑**：删除写入一条带修订号的墓碑；重新创建也必须使用更高修订号。
- **WAL + 不可变段**：写操作先 fsync WAL；刷盘时先完整 fsync 段文件，再原子发布 manifest。
- **段合并**：合并产生新的不可变段，manifest 通过原子 rename 发布；旧段只要仍被快照或游标引用就不会删除。
- **快照隔离**：快照复制创建时刻的段集和缓冲。之后的写入、刷盘和合并都不会改变该快照；游标内嵌查询、limit、边界和快照标识。
- **分页**：游标一次性消费并返回下一枚游标。结果按 `docId` 的 Unicode 字典序稳定分页，快照不变，因此不会重复、漏掉或混入新修订。
- **查询订阅**：订阅只在当前进程有效。建立时返回与当前提交序一致的初始匹配快照和水位；此后每笔成功 `put`/`delete` 至多产生一条按提交序编号的变化（`ADDED`/`REMOVED`/`UPDATED`），附文档 ID、前后修订与可复核的匹配证据。轮询携带水位，重复轮询返回相同记录。变化来自有界进程内提交保留（默认 1000 笔，可用 `retentionCommits` 调整）；水位早于保留窗口时返回 `ERR_SUBSCRIPTION_EXPIRED`，必须重新建立订阅，不会悄悄跳过事件。flush、merge、段回收不产生文档变化；WAL 写入失败的提交既不推进水位也不产生变化。
- **崩溃恢复**：重启删除 `.tmp` 半成品，加载原子 manifest，清理未被 manifest 引用的完整段，并重放其后的完整 WAL 记录。
- **回收**：`snapshot.close()`、`closeCursor()` 或显式 `reclaimSegments()` 会在确认快照引用归零后删除旧段；回收中断会在重启时再次清理。

## 查询

词项和短语只包含 ASCII 字母数字：

- `terms`: 词项交集，统一转小写。
- `phrases`: 连续 token 短语匹配，例如 `"quick brown"`。
- 命中文档返回词位证据：
  - `position`：token 序号
  - `start` / `end`：原文字符偏移
  - 短语证据返回每个连续匹配的起点 `starts`

## 编程接口

```js
import { DocumentStore } from './src/index.js';

const store = await DocumentStore.open({ directory: './data' });

await store.put('doc-1', 'quick brown fox', 1);
await store.flush();
await store.put('doc-1', 'quick red fox', 2);
await store.delete('doc-1', 3);

const page = store.query(
  { terms: ['quick', 'fox'], phrases: ['quick fox'] },
  { limit: 50 }
);
if (page.nextCursor) {
  const next = store.queryNext(page.nextCursor);
}

const snapshot = await store.snapshot();
const first = snapshot.query({ terms: ['fox'] }, { limit: 10 });
const second = snapshot.queryPage(first.nextCursor);
await snapshot.close();

// In-process query subscription: initial snapshot + watermark, then poll.
const subscription = await store.subscribe(
  { terms: ['fox'], phrases: ['red fox'] }
);
let watermark = subscription.sequence; // initial snapshot sequence
// ... more store.put / store.delete commits happen ...
const poll = await subscription.poll(watermark);
for (const change of poll.changes) {
  // change.sequence is the commit number; change.type is ADDED / REMOVED / UPDATED;
  // change.revision / change.beforeRevision and change.evidence /
  // change.beforeEvidence let the client re-verify the transition.
}
watermark = poll.currentSequence;
await subscription.close();

await store.merge();
await store.reclaimSegments();
await store.close();
```

主要方法：

- `put(id, body, revision)`
- `delete(id, revision)`
- `flush()`
- `merge(segmentIds?)`
- `reclaimSegments()`
- `snapshot()`、`snapshot.query()`、`snapshot.queryPage(cursor)`、`snapshot.close()`
- `query(query, { limit })`、`queryNext(cursor)`、`closeCursor(cursor)`
- `subscribe(query, { limit? })`、`subscription.poll(watermark)`、`subscription.close()`、`getSubscription(id)`
- `getDocument(id)`、`list()`、`stats()`、`segmentFiles()`

订阅错误码：

- `ERR_INVALID_WATERMARK`：水位不是安全整数、早于订阅建立序列或晚于当前序列（HTTP 400）。
- `ERR_SUBSCRIPTION_EXPIRED`：水位之后的变化已超出有界保留窗口，必须重新建立订阅（HTTP 410，响应含 `reestablish: true`、`oldestSequence`、`currentSequence`）。
- `ERR_SUBSCRIPTION_NOT_FOUND` / `ERR_SUBSCRIPTION_CLOSED`：订阅不存在（如进程重启后）或已关闭（HTTP 404）。

## HTTP 服务

```bash
PORT=8080 STORE_DIR=./data npm start
```

端点：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/health` | 健康与统计 |
| `GET` | `/stats` | 存储统计 |
| `POST` | `/documents` | `{id, body, revision}` |
| `DELETE` | `/documents/:id` | body 为 `{revision}` |
| `POST` | `/flush` | 刷写缓冲段 |
| `POST` | `/merge` | 可传 `{segmentIds}` |
| `POST` | `/reclaim` | 回收无引用旧段 |
| `POST` | `/snapshots` | 创建快照 |
| `POST` | `/snapshots/:id/query` | 快照首页 |
| `POST` | `/snapshots/:id/query/next` | 快照翻页 |
| `POST` | `/snapshots/:id/close` | 释放快照 |
| `POST` | `/query` | 当前状态首页，游标自动固定快照 |
| `POST` | `/query/next` | 当前游标翻页 |
| `POST` | `/cursors/close` | 关闭游标 |
| `POST` | `/subscriptions` | 建立订阅，body `{query, limit?}`；返回 `subscriptionId`、`watermark` 与初始快照 `results` |
| `POST` | `/subscriptions/:id/poll` | body `{watermark}`；重复水位返回相同 `changes` |
| `POST` | `/subscriptions/:id/close` | 关闭订阅 |

## 测试

```bash
npm test
```

测试包括：

1. 基础词项交集、连续短语、修订与墓碑。
2. 重启恢复和段合并。
3. 显式快照跨写入、刷盘、合并的隔离与分页。
4. 确定性随机对拍：每个快照都与直接扫描原始操作日志的参考实现比较。
5. 查询订阅：交错写入/删除/flush/merge/reclaim/重启，变化序列与直接扫描文档的参考实现逐条核对；覆盖幂等轮询、有界保留过期、WAL 故障注入与重启拒绝旧水位。
6. 故障注入：段写入、段 rename、manifest 写入、manifest 发布后、段回收阶段、WAL append。

### 故障注入

仅用于测试：

```js
const store = await DocumentStore.open({
  directory,
  faultInjection: {
    walAppend: true,
    segmentWrite: true,
    segmentRename: true,
    flushManifestWrite: true,
    flushManifestRename: true,
    afterFlushManifestPublish: true,
    mergeSegmentWrite: true,
    mergeSegmentRename: true,
    mergeManifestWrite: true,
    mergeManifestRename: true,
    afterMergeManifestPublish: true,
    beforeReclaim: true,
    afterReclaimDelete: true
  }
});
```

注入异常表示模拟进程在该点立即停止；测试随后重新打开目录并验证完整可查询状态。
