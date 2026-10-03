import test from 'node:test';
import assert from 'node:assert/strict';
import { DocumentStore } from '../src/index.js';
import { cleanup, makeTempDir, ReferenceModel } from './reference.js';

const dirs = [];

function dir(label) {
  const path = makeTempDir(label);
  dirs.push(path);
  return path;
}

test.after(() => cleanup(dirs));

test('subscription returns a snapshot consistent with its initial commit sequence', async () => {
  const path = dir('sub-init');
  const store = await DocumentStore.open({ directory: path });
  const model = new ReferenceModel();

  const seed = [
    ['a', 'red fox jumps', 1],
    ['b', 'red fox other', 1],
    ['c', 'unrelated document', 1]
  ];
  for (const [id, body, revision] of seed) {
    await store.put(id, body, revision);
    model.put(id, body, revision);
  }

  const query = { terms: ['red', 'fox'] };
  const initial = await store.subscribe(query, { limit: 1 });
  assert.equal(initial.watermark, model.sequence);
  assert.equal(initial.sequence, initial.watermark);
  assert.ok(initial.subscriptionId.startsWith('sub_'));
  assert.equal(initial.total, 2);
  assert.deepEqual(initial.results.map((row) => row.id), ['a']);

  // The initial page cursor is a normal store cursor pinned to the
  // subscription's initial snapshot; later commits cannot enter it.
  await store.put('d', 'red fox late arrival', 2);
  const page2 = store.queryNext(initial.nextCursor);
  assert.deepEqual(page2.results.map((row) => row.id), ['b']);
  assert.equal(page2.nextCursor, undefined);
  assert.equal(page2.sequence, initial.watermark);

  await store.closeSubscription(initial.subscriptionId);
  assert.equal(store.stats().activeSubscriptions, 0);
  await store.close();
});

test('ADDED, UPDATED and REMOVED records follow commit sequence with verifiable evidence', async () => {
  const path = dir('sub-changes');
  const store = await DocumentStore.open({ directory: path });
  const model = new ReferenceModel();
  const revisions = new Map();
  const nextRevision = (id) => {
    const revision = (revisions.get(id) ?? 0) + 1;
    revisions.set(id, revision);
    return revision;
  };

  await store.put('noise', 'alpha beta unrelated', 1);
  model.put('noise', 'alpha beta unrelated', 1);

  const query = { terms: ['red', 'fox'] };
  const sub = await store.subscribe(query);
  const base = sub.watermark;

  const commits = [
    ['put', 'a', 'red fox jumps over log'],
    ['put', 'b', 'something entirely different'],
    ['put', 'c', 'red fox chapter one'],
    ['put', 'a', 'red fox again revised'],
    ['put', 'b', 'now containing red fox here'],
    ['delete', 'c'],
    ['put', 'c', 'red fox reborn'],
    ['put', 'a', 'no longer about the query'],
    ['put', 'a', 'red fox final redraft']
  ];
  for (const [op, id, body] of commits) {
    const revision = nextRevision(id);
    if (op === 'delete') {
      await store.delete(id, revision);
      model.delete(id, revision);
    } else {
      await store.put(id, body, revision);
      model.put(id, body, revision);
    }
  }

  const poll = await store.pollSubscription(sub.subscriptionId);
  assert.equal(poll.watermark, model.sequence);
  assert.equal(poll.currentSequence, model.sequence);
  assert.equal(poll.hasMore, false);

  const expected = model.subscriptionChanges(query, base, model.sequence);
  assert.equal(poll.changes.length, expected.length);
  assert.deepEqual(poll.changes, expected);

  // Spot-check the event content: types, revisions and phrase/term evidence.
  const byKey = new Map(poll.changes.map((record) => [`${record.sequence}:${record.id}`, record]));
  assert.deepEqual(poll.changes.map((record) => record.type), [
    'ADDED',
    'ADDED',
    'UPDATED',
    'ADDED',
    'REMOVED',
    'ADDED',
    'REMOVED',
    'ADDED'
  ]);

  const firstA = poll.changes[0];
  assert.equal(firstA.before, null);
  assert.equal(firstA.after.revision, 1);
  assert.equal(firstA.after.body, 'red fox jumps over log');
  assert.deepEqual(firstA.after.evidence.terms.red.map((hit) => hit.position), [0]);
  assert.deepEqual(firstA.after.evidence.terms.fox.map((hit) => hit.position), [1]);

  const removal = poll.changes.find((record) => record.type === 'REMOVED' && record.id === 'c');
  assert.equal(removal.after, null);
  assert.equal(removal.before.revision, 1);
  assert.equal(removal.before.deleted, false);
  assert.equal(removal.before.body, 'red fox chapter one');

  // Deleting a currently matching document yields a tombstone-side revision.
  const bRevision = nextRevision('b');
  await store.delete('b', bRevision);
  model.delete('b', bRevision);

  const tombstonePoll = await store.pollSubscription(sub.subscriptionId, {
    watermark: poll.watermark
  });
  assert.equal(tombstonePoll.changes.length, 1);
  const tombstone = tombstonePoll.changes[0];
  assert.equal(tombstone.type, 'REMOVED');
  assert.equal(tombstone.before.revision, bRevision - 1);
  assert.equal(tombstone.after, null);

  await store.close();
});

test('phrase subscription evidence exposes consecutive start positions', async () => {
  const path = dir('sub-phrase');
  const store = await DocumentStore.open({ directory: path });
  const sub = await store.subscribe({ terms: ['fox'], phrases: ['red fox'] });

  await store.put('a', 'red fox then red fox again', 1);
  await store.put('b', 'fox but no red', 1);

  const poll = await store.pollSubscription(sub.subscriptionId);
  assert.deepEqual(poll.changes.map((record) => record.id), ['a']);
  assert.deepEqual(poll.changes[0].after.evidence.phrases[0].starts, [0, 3]);

  await store.close();
});

test('repeated polls with the same watermark return the same records', async () => {
  const path = dir('sub-idempotent');
  const store = await DocumentStore.open({ directory: path });
  const sub = await store.subscribe({ terms: ['fox'] });

  await store.put('a', 'fox one', 1);
  await store.put('b', 'fox two', 1);

  const first = await store.pollSubscription(sub.subscriptionId);
  const second = await store.pollSubscription(sub.subscriptionId, { watermark: sub.watermark });
  const third = await store.pollSubscription(sub.subscriptionId, { watermark: sub.watermark });
  assert.deepEqual(second.changes, first.changes);
  assert.deepEqual(third.changes, first.changes);
  assert.equal(second.watermark, first.watermark);

  // Advancing by one record keeps the rest re-readable from a partial watermark.
  const partial = await store.pollSubscription(sub.subscriptionId, {
    watermark: sub.watermark,
    limit: 1
  });
  assert.equal(partial.changes.length, 1);
  assert.equal(partial.hasMore, true);
  assert.equal(partial.watermark, 1);
  const rest = await store.pollSubscription(sub.subscriptionId, {
    watermark: partial.watermark
  });
  assert.deepEqual(rest.changes.map((record) => record.id), ['b']);
  assert.equal(rest.hasMore, false);

  // Polling at the current watermark returns no changes.
  const caughtUp = await store.pollSubscription(sub.subscriptionId, {
    watermark: rest.watermark
  });
  assert.deepEqual(caughtUp.changes, []);
  assert.equal(caughtUp.hasMore, false);

  await store.close();
});

test('flush, merge and reclaim emit no changes and do not move the watermark', async () => {
  const path = dir('sub-segments');
  const store = await DocumentStore.open({ directory: path });
  const model = new ReferenceModel();

  await store.put('a', 'red fox one', 1);
  await store.put('b', 'red fox two', 1);
  await store.flush();
  model.put('a', 'red fox one', 1);
  model.put('b', 'red fox two', 1);

  const sub = await store.subscribe({ terms: ['red', 'fox'] });
  const watermark = sub.watermark;

  await store.put('c', 'red fox three', 2);
  model.put('c', 'red fox three', 2);
  await store.flush();
  await store.merge();
  await store.reclaimSegments();

  const poll = await store.pollSubscription(sub.subscriptionId);
  assert.equal(poll.changes.length, 1);
  assert.equal(poll.changes[0].id, 'c');
  assert.equal(poll.changes[0].sequence, watermark + 1);
  assert.equal(poll.currentSequence, watermark + 1);

  // Another merge/reclaim cycle with no writes still yields nothing.
  await store.put('d', 'delta', 3);
  await store.put('e', 'echo', 1);
  await store.flush();
  await store.merge();
  await store.reclaimSegments();
  const quiet = await store.pollSubscription(sub.subscriptionId, { watermark: poll.watermark });
  assert.deepEqual(quiet.changes, []);

  // The current query result still agrees with the direct-scan model.
  assert.deepEqual(
    store.query({ terms: ['red', 'fox'] }).results.map((row) => row.id).sort(),
    ['a', 'b', 'c']
  );

  await store.close();
});

test('watermarks older than the bounded retention window are explicitly rejected', async () => {
  const path = dir('sub-retention');
  const store = await DocumentStore.open({ directory: path, subscriptionRetention: 3 });
  const sub = await store.subscribe({ terms: ['fox'] });

  for (let seq = 1; seq <= 6; seq++) {
    await store.put(`doc${seq}`, `fox number ${seq}`, 1);
  }

  await assert.rejects(
    store.pollSubscription(sub.subscriptionId, { watermark: sub.watermark }),
    (error) =>
      error.code === 'ERR_SUBSCRIPTION_WINDOW_EXPIRED' &&
      error.resubscribe === true &&
      error.windowStart > sub.watermark &&
      error.currentSequence === 6
  );

  // A watermark still inside the window works: only the last three ADDED
  // records are retained, each one commit apart.
  const recent = await store.pollSubscription(sub.subscriptionId, { watermark: 3 });
  assert.equal(recent.windowStart, 4);
  assert.deepEqual(recent.changes.map((record) => record.id), ['doc4', 'doc5', 'doc6']);

  await store.closeSubscription(sub.subscriptionId);
  await store.close();
});

test('unrelated commits age retention without forcing false expiry of quiet subscriptions', async () => {
  const path = dir('sub-retention-quiet');
  const store = await DocumentStore.open({ directory: path, subscriptionRetention: 2 });
  const sub = await store.subscribe({ terms: ['fox'] });
  const base = sub.watermark;

  // Many matching commits fill and drain the window...
  for (let seq = 1; seq <= 5; seq++) {
    await store.put(`match${seq}`, 'fox body', 1);
  }
  // ...then only non-matching commits happen. The empty window answer is the
  // current state, so polling from the last known watermark must not error.
  await store.put('other1', 'alpha only', 1);
  await store.put('other2', 'beta only', 1);

  const atLastMatch = await store.pollSubscription(sub.subscriptionId, { watermark: 5 });
  assert.deepEqual(atLastMatch.changes, []);
  assert.equal(atLastMatch.watermark, 7);

  // But a watermark before the first retained sequence is still refused.
  await assert.rejects(
    store.pollSubscription(sub.subscriptionId, { watermark: base }),
    /retention window/
  );

  await store.close();
});

test('an empty query matches all live docs and reports delete then recreate', async () => {
  const path = dir('sub-match-all');
  const store = await DocumentStore.open({ directory: path });
  await store.put('a', 'anything here', 1);

  const sub = await store.subscribe({});
  assert.equal(sub.total, 1);
  assert.equal(sub.results[0].evidence, null);

  await store.delete('a', 2);
  await store.put('a', 'recreated body', 3);
  const poll = await store.pollSubscription(sub.subscriptionId, { watermark: sub.watermark });
  assert.deepEqual(
    poll.changes.map((record) => [record.type, record.before?.revision, record.after?.revision]),
    [['REMOVED', 1, undefined], ['ADDED', undefined, 3]]
  );

  await store.close();
});

test('unknown, future and restarted watermarks are rejected and require resubscription', async () => {
  const path = dir('sub-restart');
  let store = await DocumentStore.open({ directory: path });
  await store.put('a', 'fox one', 1);
  const sub = await store.subscribe({ terms: ['fox'] });
  await store.put('b', 'fox two', 2);

  await assert.rejects(
    store.pollSubscription('sub_does-not-exist'),
    (error) => error.code === 'ERR_SUBSCRIPTION_UNKNOWN' && error.resubscribe === true
  );
  await assert.rejects(
    store.pollSubscription(sub.subscriptionId, { watermark: 999 }),
    (error) => error.code === 'ERR_SUBSCRIPTION_WATERMARK_INVALID' && error.resubscribe === true
  );
  await assert.rejects(
    store.pollSubscription(sub.subscriptionId, { watermark: -1 }),
    (error) => error.code === 'ERR_SUBSCRIPTION_WATERMARK_INVALID'
  );
  assert.throws(
    () => store.pollSubscription(sub.subscriptionId, { watermark: 'one' }),
    (error) => error.code === 'ERR_INVALID_WATERMARK'
  );

  await store.close();
  store = await DocumentStore.open({ directory: path });

  // The directory kept the data but the subscription registry is process-local.
  assert.deepEqual(store.query({ terms: ['fox'] }).results.map((row) => row.id), ['a', 'b']);
  await assert.rejects(
    store.pollSubscription(sub.subscriptionId, { watermark: 1 }),
    (error) => error.code === 'ERR_SUBSCRIPTION_UNKNOWN' && error.resubscribe === true
  );

  // A fresh subscription starts from the recovered commit sequence.
  const fresh = await store.subscribe({ terms: ['fox'] });
  assert.equal(fresh.watermark, 2);
  assert.equal(fresh.total, 2);
  const quiet = await store.pollSubscription(fresh.subscriptionId);
  assert.deepEqual(quiet.changes, []);
  await store.put('c', 'fox three', 3);
  const moved = await store.pollSubscription(fresh.subscriptionId);
  assert.deepEqual(moved.changes.map((record) => [record.id, record.type]), [['c', 'ADDED']]);

  await store.close();
});
