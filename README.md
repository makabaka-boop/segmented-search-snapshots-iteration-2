# Snapshot Document Store

Node.js 20、无第三方依赖的文档索引服务。它维护最多 1000 份带修订号的活跃文档，写入先进入 WAL 与内存缓冲，再刷成不可变倒排索引段。

## 一致性模型

- **修订号**：同一文档的更新或删除必须使用严格递增的正整数修订号。
- **墓碑**：删除写入一条带修订号的墓碑；重新创建也必须使用更高修订号。
- **WAL + 不可变段**：写操作先 fsync WAL；刷盘时先完整 fsync 段文件，再原子发布 manifest。
- **段合并**：合并产生新的不可变段，manifest 通过原子 rename 发布；旧段只要仍被快照或游标引用就不会删除。
- **快照隔离**：快照复制创建时刻的段集和缓冲。之后的写入、刷盘和合并都不会改变该快照；游标内嵌查询、limit、边界和快照标识。
- **分页**：游标一次性消费并返回下一枚游标。结果按 `docId` 的 Unicode 字典序稳定分页，快照不变，因此不会重复、漏掉或混入新修订。
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
- `subscribe(query, { limit })`、`pollSubscription(id, { watermark, limit })`、`closeSubscription(id)`
- `getDocument(id)`、`list()`、`stats()`、`segmentFiles()`

## 查询订阅（进程内）

档案员保存一条查询后，不必反复翻查全量结果：订阅只推送结果集相对当前提交序的增量。

- `subscribe(query, { limit })` 在同一个提交边界上返回**初始匹配快照首页**（普通快照页，`nextCursor` 可继续翻页）以及 `subscriptionId` 与 `watermark`（当前提交序）。初始快照页固定在订阅时刻，之后的提交不会混入。
- 此后每笔**成功的** `put`/`delete` 至多产生一条按提交序编号的变化记录：
  - `ADDED`：文档新进入结果集（`before: null`）
  - `REMOVED`：文档退出结果集（`after: null`，含删除与改后不再匹配）
  - `UPDATED`：文档前后都匹配但换了修订
  - 每条记录附 `id`、`sequence`、前后 `revision`、`body` 及可复核的词位/短语 `evidence`。
- `pollSubscription(id, { watermark, limit })`：携带上次水位轮询。重复携带同一水位返回**完全相同**的记录（幂等，不消费）；不传水位时使用订阅水位。返回新的 `watermark`、`currentSequence`、`hasMore`。
- **有界保留**：变化记录按订阅保留（默认 `subscriptionRetention: 1000` 条）。水位早于保留窗口时抛 `ERR_SUBSCRIPTION_WINDOW_EXPIRED`（HTTP 410，`resubscribe: true`），要求重新建立订阅，绝不悄悄跳过事件。水位超过当前提交序抛 `ERR_SUBSCRIPTION_WATERMARK_INVALID`。
- WAL 写入失败的提交既不推进提交序/水位，也不产生任何变化记录。
- `flush`、`merge`、段回收不产生文档变化、不推进水位；订阅不引用段，因此旧快照分页与游标寿命不受订阅影响。
- 订阅仅存在于当前进程内存。重启后旧 `subscriptionId`/水位一律被拒绝（`ERR_SUBSCRIPTION_UNKNOWN`，HTTP 404），需重新订阅并以恢复后的提交序重建快照。

```js
const first = await store.subscribe({ terms: ['red', 'fox'] }, { limit: 50 });
// first.results / first.nextCursor：订阅时刻的初始快照
let watermark = first.watermark;
// ... 其他客户端持续 put/delete ...
const page = await store.pollSubscription(first.subscriptionId, { watermark });
for (const change of page.changes) {
  // change.type: ADDED | REMOVED | UPDATED
}
watermark = page.watermark; // 下次轮询携带；重复携带旧值会得到相同记录
```

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
| `POST` | `/subscriptions` | 建立订阅，返回初始快照首页、`subscriptionId`、`watermark` |
| `POST` | `/subscriptions/:id/poll` | body 为 `{watermark?, limit?}`，返回增量变化 |
| `POST` | `/subscriptions/:id/close` | 关闭订阅 |

订阅轮询的状态码：`404` 表示订阅在本进程不存在（如重启后，需重建）；`410` 表示水位已越过有界保留窗口（`resubscribe: true`）；`400` 表示非法水位或参数。

## 测试

```bash
npm test
```

测试包括：

1. 基础词项交集、连续短语、修订与墓碑。
2. 重启恢复和段合并。
3. 显式快照跨写入、刷盘、合并的隔离与分页。
4. 确定性随机对拍：每个快照都与直接扫描原始操作日志的参考实现比较。
5. 故障注入：段写入、段 rename、manifest 写入、manifest 发布后、段回收、WAL 追加撕裂。
6. 查询订阅：初始快照、ADDED/REMOVED/UPDATED 变化序列与证据、幂等轮询、有界保留过期、flush/merge/reclaim 静默、WAL 故障不泄露变化、重启拒绝旧水位、HTTP 生命周期。
7. 订阅随机对拍：交错写入/删除/合并/回收/重启，逐轮与直接扫描操作日志的模型核对变化序列。

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

注入异常表示模拟进程在该点立即停止；测试随后重新打开目录并验证完整可查询状态。`walAppend` 是一次性故障：它在下一次 WAL 追加的首字节落盘后、记录写完前抛出，留下无换行的撕裂尾部（重启时被截断），用于验证失败提交不推进提交序、不产生订阅变化。
