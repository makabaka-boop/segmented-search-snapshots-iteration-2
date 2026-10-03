import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { DocumentStore } from '../src/index.js';
import { cleanup, compareQueryResults, makeTempDir, ReferenceModel } from './reference.js';

const dirs = [];

function dir(label) {
  const path = makeTempDir(label);
  dirs.push(path);
  return path;
}

test.after(() => cleanup(dirs));

test('indexes updates, tombstones, terms and consecutive phrases', async () => {
  const path = dir('basic');
  const store = await DocumentStore.open({ directory: path });

  await store.put('a', 'quick brown fox quick', 1);
  await store.put('b', 'Quick! brown; deer 123', 2);
  await store.put('c', 'fox hound quick brown dog', 3);
  await store.delete('b', 4);
  await store.put('a', 'quick brown fox updated', 5);

  const termResult = store.query({ terms: ['QUICK', 'fox'] });
  const reference = new ReferenceModel();
  reference.put('a', 'quick brown fox quick', 1);
  reference.put('b', 'Quick! brown; deer 123', 2);
  reference.put('c', 'fox hound quick brown dog', 3);
  reference.delete('b', 4);
  reference.put('a', 'quick brown fox updated', 5);
  compareQueryResults(assert, termResult.results, reference.query({ terms: ['QUICK', 'fox'] }));

  const a = termResult.results.find((doc) => doc.id === 'a');
  assert.deepEqual(a.evidence.terms.quick.map((hit) => hit.position), [0]);
  assert.deepEqual(a.evidence.terms.fox.map((hit) => hit.position), [2]);
  assert.equal(a.revision, 5);
  assert.equal(store.getDocument('b'), null);

  const phrase = store.query({ phrases: ['brown fox'] });
  assert.deepEqual(phrase.results.map((doc) => doc.id).sort(), ['a']);
  assert.deepEqual(phrase.results[0].evidence.phrases[0].starts, [1]);
  assert.deepEqual(store.query({ phrases: ['fox brown'] }).results, []);
  assert.deepEqual(store.query({ terms: ['123'] }).results, []);

  await store.close();
});

test('revisions must increase and unique live documents are capped', async () => {
  const path = dir('limits');
  const store = await DocumentStore.open({ directory: path, maxDocuments: 2 });

  await store.put('a', 'alpha', 1);
  await store.put('b', 'bravo', 1);
  await assert.rejects(store.put('c', 'charlie', 1), /live documents/);
  await assert.rejects(store.put('a', 'alpha newer', 1), /not newer/);
  await store.delete('a', 2);
  await store.put('c', 'charlie', 3);
  assert.deepEqual(store.list().map((doc) => doc.id), ['b', 'c']);

  await store.close();
});

test('immutable segments recover writes after restart and support merge', async () => {
  const path = dir('segments');
  let store = await DocumentStore.open({ directory: path });

  await store.put('a', 'alpha one', 1);
  await store.flush();
  const segment = JSON.parse(readFileSync(join(path, 'segment-00000001.json'), 'utf8'));
  assert.deepEqual(segment.postings.alpha, [{ id: 'a', positions: [{ position: 0, start: 0, end: 5 }] }]);
  assert.equal(segment.documents[0].body, 'alpha one');
  await store.put('b', 'beta one', 2);
  await store.flush();
  await store.put('a', 'alpha two', 3);
  await store.delete('b', 4);
  await store.flush();

  await store.close();
  store = await DocumentStore.open({ directory: path });
  assert.equal(store.stats().bufferedOperations, 0);
  assert.deepEqual(store.query({ terms: ['alpha'] }).results.map((x) => x.revision), [3]);
  assert.deepEqual(store.query({ terms: ['beta'] }).results, []);

  const merge = await store.merge();
  assert.equal(merge.replacedSegmentIds.length, 3);
  await store.close();

  store = await DocumentStore.open({ directory: path });
  assert.deepEqual(store.manifest.segmentIds, [merge.segmentId]);
  assert.deepEqual(store.query({ terms: ['alpha'] }).results.map((x) => x.id), ['a']);
  await store.close();
});

test('explicit snapshots compare against direct raw-document scans', async () => {
  const path = dir('snapshot');
  const model = new ReferenceModel();
  const store = await DocumentStore.open({ directory: path });

  const bodies = [
    'red fox jumps log',
    'blue fox and red dog',
    'red red fox',
    'fox red',
    'another unrelated document'
  ];
  bodies.forEach((body, index) => {
    model.put(`doc${index}`, body, 1);
  });
  for (const [id, body, revision] of bodies.map((body, index) => [`doc${index}`, body, 1])) {
    await store.put(id, body, revision);
  }

  const snapshot = await store.snapshot();
  const expectedSequence = model.sequence;
  const expectedDocs = model.atSequence(expectedSequence);

  await store.put('doc0', 'mutated red fox after snapshot', 2);
  await store.put('doc5', 'fresh red fox document', 1);
  await store.delete('doc2', 2);
  model.put('doc0', 'mutated red fox after snapshot', 2);
  model.put('doc5', 'fresh red fox document', 1);
  model.delete('doc2', 2);
  await store.flush();

  const queries = [
    {},
    { terms: ['red'] },
    { terms: ['red', 'fox'] },
    { phrases: ['red fox'] },
    { terms: ['fox'], phrases: ['red fox'] },
    { phrases: ['red', 'red fox'] }
  ];

  for (const query of queries) {
    const expected = model.query(query, expectedDocs);
    const result = snapshot.query(query, { limit: 2 });
    let rows = result.results;
    let cursor = result.nextCursor;
    while (cursor) {
      const next = snapshot.queryPage(cursor);
      rows = rows.concat(next.results);
      cursor = next.nextCursor;
    }
    compareQueryResults(assert, rows, expected);
    assert.equal(result.sequence, expectedSequence);
  }

  const current = model.query({ terms: ['red', 'fox'] });
  await snapshot.close();
  compareQueryResults(assert, store.query({ terms: ['red', 'fox'] }).results, current);
  await store.close();
});

test('closing an explicit snapshot waits to reclaim until its cursor completes', async () => {
  const path = dir('snapshot-cursor-lifecycle');
  const store = await DocumentStore.open({ directory: path });
  await store.put('a', 'red fox one', 1);
  await store.flush();
  await store.put('b', 'red fox two', 2);
  await store.flush();

  const snapshot = await store.snapshot();
  const first = snapshot.query({ terms: ['red', 'fox'] }, { limit: 1 });
  assert.equal(store.stats().activeCursors, 1);
  await snapshot.close();
  assert.equal(store.stats().activeSnapshots, 1);

  const second = snapshot.queryPage(first.nextCursor);
  assert.deepEqual(
    [first.results[0].id, second.results[0].id],
    ['a', 'b']
  );
  assert.equal(second.nextCursor, undefined);
  assert.equal(store.stats().activeSnapshots, 0);
  assert.equal(store.stats().activeCursors, 0);

  await store.close();
});
