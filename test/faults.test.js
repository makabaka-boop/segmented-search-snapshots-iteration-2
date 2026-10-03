import test from 'node:test';
import assert from 'node:assert/strict';
import { readdirSync, appendFileSync } from 'node:fs';
import { join } from 'node:path';
import { DocumentStore } from '../src/index.js';
import { cleanup, makeTempDir } from './reference.js';

const dirs = [];
test.after(() => cleanup(dirs));

async function reopen(path, options = {}) {
  return DocumentStore.open({
    directory: path,
    cursorTtlMs: 60_000,
    ...options
  });
}

async function expectFault(stage, operation) {
  await assert.rejects(
    operation,
    (error) => error.faultStage === stage,
    `expected injected fault ${stage}`
  );
}

async function seedAndAssert(path) {
  const store = await reopen(path);
  assert.equal(store.stats().liveDocuments, 1);
  assert.deepEqual(store.query({ terms: ['red', 'fox'] }).results.map((x) => x.id), ['a']);
  assert.equal(store.getDocument('a').body, 'red fox');
  await store.close();
}

test('partial segment file leaves complete WAL state queryable after restart', async () => {
  const path = makeTempDir('fault-segment-write');
  dirs.push(path);
  let store = await reopen(path, { faultInjection: { segmentWrite: true } });
  await store.put('a', 'red fox', 1);
  await expectFault('segmentWrite', store.flush());
  await store.close();

  store = await reopen(path);
  assert.equal(store.stats().bufferedOperations, 1);
  assert.deepEqual(store.query({ terms: ['red', 'fox'] }).results.map((x) => x.id), ['a']);
  await store.flush();
  await store.close();
  await seedAndAssert(path);
});

test('complete but unpublished segment is garbage collected and WAL replays', async () => {
  const path = makeTempDir('fault-segment-rename');
  dirs.push(path);
  let store = await reopen(path, { faultInjection: { segmentRename: true } });
  await store.put('a', 'red fox', 1);
  await expectFault('segmentRename', store.flush());
  await store.close();

  store = await reopen(path);
  assert.deepEqual(store.segmentFiles(), []);
  assert.equal(store.stats().bufferedOperations, 1);
  await store.flush();
  await store.close();
  await seedAndAssert(path);
});

test('partial flush manifest is ignored while completed segment remains stale', async () => {
  const path = makeTempDir('fault-flush-manifest');
  dirs.push(path);
  let store = await reopen(path, { faultInjection: { flushManifestWrite: true } });
  await store.put('a', 'red fox', 1);
  await expectFault('flushManifestWrite', store.flush());
  await store.close();

  store = await reopen(path);
  assert.deepEqual(store.manifest.segmentIds, []);
  assert.deepEqual(store.segmentFiles(), []);
  assert.equal(store.stats().bufferedOperations, 1);
  await store.flush();
  await store.close();
  await seedAndAssert(path);
});

test('published manifest survives a failure immediately after atomic rename', async () => {
  const path = makeTempDir('fault-after-publish');
  dirs.push(path);
  let store = await reopen(path, { faultInjection: { afterFlushManifestPublish: true } });
  await store.put('a', 'red fox', 1);
  await expectFault('afterFlushManifestPublish', store.flush());
  await store.close();

  store = await reopen(path);
  assert.equal(store.stats().bufferedOperations, 0);
  assert.deepEqual(store.manifest.segmentIds, [1]);
  assert.equal(store.getDocument('a').body, 'red fox');
  await store.close();
});

test('old segments stay until old snapshot releases, then are reclaimed', async () => {
  const path = makeTempDir('merge-reclaim');
  dirs.push(path);
  const store = await reopen(path);

  await store.put('a', 'red fox one', 1);
  await store.flush();
  await store.put('b', 'brown fox two', 2);
  await store.flush();

  const snapshot = await store.snapshot();
  assert.deepEqual(snapshot.query({ terms: ['fox'] }).results.map((x) => x.id).sort(), ['a', 'b']);

  const merged = await store.merge();
  assert.deepEqual(readdirSync(path).filter((name) => name.endsWith('.json')).sort(), [
    'manifest.json',
    'segment-00000001.json',
    'segment-00000002.json',
    'segment-00000003.json'
  ]);

  // The old cursor remains anchored to pre-merge segments and pages cleanly.
  const first = snapshot.query({ terms: ['fox'] }, { limit: 1 });
  const second = snapshot.queryPage(first.nextCursor);
  assert.deepEqual([...first.results, ...second.results].map((x) => x.id), ['a', 'b']);
  assert.equal(second.nextCursor, undefined);

  await snapshot.close();
  assert.deepEqual(store.segmentFiles(), [`segment-${String(merged.segmentId).padStart(8, '0')}.json`]);
  await store.close();
});

test('torn trailing WAL append is truncated and complete records remain', async () => {
  const path = makeTempDir('torn-wal');
  dirs.push(path);
  let store = await reopen(path);
  await store.put('a', 'red fox', 1);
  await store.close();

  appendFileSync(join(path, 'wal.log'), '{"type":"put","sequence":2,"id":"b","revisio');
  store = await reopen(path);
  assert.equal(store.stats().sequence, 1);
  assert.deepEqual(store.query({ terms: ['fox'] }).results.map((x) => x.id), ['a']);
  await store.put('b', 'brown fox', 2);
  await store.close();

  store = await reopen(path);
  assert.deepEqual(store.query({ terms: ['fox'] }).results.map((x) => x.id).sort(), ['a', 'b']);
  await store.close();
});

test('failure during reclaim is repaired after restart', async () => {
  const path = makeTempDir('reclaim-fault');
  dirs.push(path);
  let store = await reopen(path);
  await store.put('a', 'red fox', 1);
  await store.flush();
  await store.put('b', 'brown fox', 2);
  await store.flush();

  let snapshot = await store.snapshot();
  await store.merge();
  await store.put('c', 'quick fox', 3);
  await store.flush();
  await snapshot.close();
  await store.close();

  store = await reopen(path, { faultInjection: { afterReclaimDelete: true } });
  snapshot = await store.snapshot();
  await store.merge();
  await expectFault('afterReclaimDelete', snapshot.close());
  await store.close();

  store = await reopen(path);
  assert.deepEqual(store.query({ terms: ['fox'] }).results.map((x) => x.id).sort(), ['a', 'b', 'c']);
  assert.equal(store.segmentFiles().length, 1);
  await store.close();
});

test('failed WAL append neither advances commit sequence nor leaks subscription changes', async () => {
  const path = makeTempDir('fault-wal-append');
  dirs.push(path);
  let store = await reopen(path);
  await store.put('a', 'red fox already durable', 1);
  await store.flush();
  await store.close();

  // Reopen with a one-shot torn-append fault on the first new write.
  store = await reopen(path, { faultInjection: { walAppend: true } });
  const sub = await store.subscribe({ terms: ['red', 'fox'] });
  const watermark = sub.watermark;

  // The fault tears the append mid-record; the write must be rejected and no
  // state or subscription record may appear for it.
  await expectFault(
    'walAppend',
    store.put('b', 'red fox torn append', 2)
  );
  assert.equal(store.stats().sequence, watermark);
  const poll = await store.pollSubscription(sub.subscriptionId, { watermark });
  assert.deepEqual(poll.changes, []);
  assert.equal(poll.currentSequence, watermark);

  // The one-shot fault is spent; a retried write in the same process still
  // must not accidentally succeed because the torn tail blocks the fd. Model
  // the crash instead: close through the torn tail and reopen. The partial
  // WAL line is truncated and the failed document never existed.
  await store.close();
  store = await reopen(path);
  assert.equal(store.stats().sequence, watermark);
  assert.deepEqual(store.query({ terms: ['red', 'fox'] }).results.map((x) => x.id), ['a']);

  // The old in-process subscription is gone; a fresh one starts clean and
  // observes subsequent commits normally.
  await assert.rejects(
    store.pollSubscription(sub.subscriptionId, { watermark }),
    (error) => error.code === 'ERR_SUBSCRIPTION_UNKNOWN'
  );
  const fresh = await store.subscribe({ terms: ['red', 'fox'] });
  await store.put('b', 'red fox retried cleanly', 2);
  const changes = await store.pollSubscription(fresh.subscriptionId);
  assert.deepEqual(changes.changes.map((record) => [record.id, record.type, record.after.revision]), [
    ['b', 'ADDED', 2]
  ]);
  await store.close();
});
