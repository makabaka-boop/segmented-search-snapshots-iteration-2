import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { DocumentStore } from '../src/index.js';
import { cleanup, compareQueryResults, lcg, makeTempDir, ReferenceModel } from './reference.js';

const dirs = [];

function dir(label) {
  const path = makeTempDir(label);
  dirs.push(path);
  return path;
}

test.after(() => cleanup(dirs));

test('writes confirmed after a flush survive a clean restart', async () => {
  const path = dir('restart-durability');
  let store = await DocumentStore.open({ directory: path });
  await store.put('a', 'alpha one', 1);
  await store.put('b', 'beta one', 2);
  await store.flush();

  await store.put('c', 'gamma three', 3);
  await store.put('a', 'alpha two', 4);
  await store.delete('b', 5);

  // Acknowledged writes must already be on the visible WAL, not only in
  // process memory (a flush must not strand the append fd on a stale inode).
  const walSequences = readFileSync(join(path, 'wal.log'), 'utf8')
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line).sequence);
  assert.deepEqual(walSequences, [3, 4, 5]);

  const before = {
    gamma: store.query({ terms: ['gamma'] }).results.map((doc) => doc.id),
    revisionOfA: store.getDocument('a').revision,
    list: store.list().map((doc) => [doc.id, doc.revision])
  };
  assert.deepEqual(before.gamma, ['c']);
  assert.equal(before.revisionOfA, 4);
  assert.equal(store.getDocument('b'), null);

  await store.close();
  store = await DocumentStore.open({ directory: path });

  assert.deepEqual(store.query({ terms: ['gamma'] }).results.map((doc) => doc.id), before.gamma);
  assert.equal(store.getDocument('a').revision, before.revisionOfA);
  assert.equal(store.getDocument('b'), null);
  assert.deepEqual(store.list().map((doc) => [doc.id, doc.revision]), before.list);

  // The recovered tombstone still governs revision checks.
  await assert.rejects(store.put('b', 'beta again', 4), /not newer/);
  await store.put('b', 'beta again', 6);
  assert.equal(store.getDocument('b').revision, 6);
  await store.close();
});

test('sequence numbers continue from the durable manifest after restart', async () => {
  const path = dir('restart-sequence');
  let store = await DocumentStore.open({ directory: path });
  await store.put('a', 'alpha one', 1);
  await store.put('b', 'beta one', 2);
  await store.flush();
  await store.close();

  store = await DocumentStore.open({ directory: path });
  assert.equal(store.stats().sequence, 2);
  const write = await store.put('c', 'gamma three', 3);
  assert.equal(write.sequence, 3);
  assert.equal(store.stats().sequence, 3);
  await store.close();

  store = await DocumentStore.open({ directory: path });
  assert.equal(store.stats().sequence, 3);
  assert.deepEqual(store.query({ terms: ['gamma'] }).results.map((doc) => doc.id), ['c']);

  // A buffered newer revision must shadow the older segment entry everywhere.
  await store.put('a', 'alpha two', 4);
  assert.equal(store.getDocument('a').body, 'alpha two');
  assert.deepEqual(store.query({ terms: ['alpha'] }).results.map((doc) => doc.body), ['alpha two']);
  const snapshot = await store.snapshot();
  assert.equal(snapshot.getDocument('a').body, 'alpha two');
  await snapshot.close();
  await store.close();
});

test('flush, merge and delete cycles across restarts match a direct scan', async () => {
  const path = dir('restart-cycles');
  const model = new ReferenceModel();
  const revisions = new Map();
  const random = lcg(20261002);
  const nextRevision = (id) => {
    const revision = (revisions.get(id) ?? 0) + 1;
    revisions.set(id, revision);
    return revision;
  };

  let store = await DocumentStore.open({ directory: path });
  for (let cycle = 0; cycle < 6; cycle++) {
    for (let i = 0; i < 6; i++) {
      const id = `doc-${Math.floor(random() * 8)}`;
      const revision = nextRevision(id);
      if (random() < 0.3) {
        await store.delete(id, revision);
        model.delete(id, revision);
      } else {
        const body = `cycle ${cycle} doc ${id} red fox ${i}`;
        await store.put(id, body, revision);
        model.put(id, body, revision);
      }
    }
    if (cycle % 2 === 0) await store.flush();
    if (cycle === 3) await store.merge();

    const query = { terms: ['red', 'fox'] };
    compareQueryResults(assert, store.query(query).results, model.query(query));
    await store.close();

    store = await DocumentStore.open({ directory: path });
    compareQueryResults(assert, store.query(query).results, model.query(query));
    assert.equal(store.stats().sequence, model.sequence);
  }

  // Revisions and tombstones observed before the restarts still hold.
  for (const [id, revision] of revisions) {
    const expected = model.docsById.get(id);
    const actual = store.getDocument(id);
    if (expected.deleted) {
      assert.equal(actual, null, `tombstone for ${id} survives restart`);
    } else {
      assert.equal(actual.revision, revision, `revision of ${id} survives restart`);
    }
  }
  await store.close();
});
