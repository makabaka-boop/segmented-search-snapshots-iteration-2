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
- `getDocument(id)`、`list()`、`stats()`、`segmentFiles()`

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

## 测试

```bash
npm test
```

测试包括：

1. 基础词项交集、连续短语、修订与墓碑。
2. 重启恢复和段合并。
3. 显式快照跨写入、刷盘、合并的隔离与分页。
4. 确定性随机对拍：每个快照都与直接扫描原始操作日志的参考实现比较。
5. 故障注入：段写入、段 rename、manifest 写入、manifest 发布后、段回收阶段。

### 故障注入

仅用于测试：

```js
const store = await DocumentStore.open({
  directory,
  faultInjection: {
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
