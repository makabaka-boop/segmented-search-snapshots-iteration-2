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
