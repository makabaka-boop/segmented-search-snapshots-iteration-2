import test from 'node:test';
import assert from 'node:assert/strict';
import { DocumentStore } from '../src/index.js';
import {
  cleanup,
  compareQueryResults,
  expectedChanges,
  lcg,
  makeTempDir,
  ReferenceModel
} from './reference.js';

const dirs = [];

function dir(label) {
  const path = makeTempDir(label);
  dirs.push(path);
  return path;
}

test.after(() => cleanup(dirs));

function assertChangesEqual(actual, expected) {
  assert.deepEqual(
    actual.map((change) => ({
      sequence: change.sequence,
      type: change.type,
      id: change.id,
      revision: change.revision,
      beforeRevision: change.beforeRevision,
      evidence: change.evidence,
      beforeEvidence: change.beforeEvidence
    })),
    expected
  );
}

test('initial snapshot and change stream follow commit order across put, delete, flush, merge, reclaim', async () => {
  const path = dir('subscription-lifecycle');
  const store = await DocumentStore.open({ directory: path });
  const model = new ReferenceModel();
  const revisions = new Map();
  const nextRevision = (id) => {
    const revision = (revisions.get(id) ?? 0) + 1;
    revisions.set(id, revision);
    return revision;
  };
  const query = { terms: ['red', 'fox'], phrases: ['red fox'] };

  const put = async (id, body) => {
    const revision = nextRevision(id);
    await store.put(id, body, revision);
    model.put(id, body, revision);
  };
  const remove = async (id) => {
    const revision = nextRevision(id);
    await store.delete(id, revision);
    model.delete(id, revision);
  };

  await put('a', 'red fox here');
  await put('noise', 'unrelated blue log');
  const sub = await store.subscribe(query);

  compareQueryResults(
    assert,
    sub.snapshot.results,
    model.query(query, model.atSequence(sub.sequence))
  );
  assert.equal(sub.sequence, model.sequence);
  assert.equal(sub.snapshot.sequence, model.sequence);
  assert.equal(store.stats().activeSubscriptions, 1);

  // seq 3: ADDED; seq 4: unrelated revision -> no record; seq 5: UPDATED.
  await put('b', 'red fox jumps');
  await put('noise', 'still unrelated');
  await put('b', 'the red fox jumps again');

  // seq 6: REMOVED (phrase gone); seq 7: ADDED again at tombstone+1 rev.
  await put('a', 'only red without fox');
  await put('a', 'red fox returns', nextRevision('a'));

  const poll = await sub.poll(sub.sequence);
  assert.equal(poll.watermark, sub.sequence);
  assert.equal(poll.currentSequence, model.sequence);
  assertChangesEqual(poll.changes, expectedChanges(model, query, sub.sequence));
  assert.deepEqual(
    poll.changes.map((change) => [change.sequence, change.type, change.id]),
    [
      [3, 'ADDED', 'b'],
      [5, 'UPDATED', 'b'],
      [6, 'REMOVED', 'a'],
      [7, 'ADDED', 'a']
    ]
  );
  assert.equal(poll.changes[0].revision, 1);
  assert.equal(poll.changes[0].beforeRevision, null);
  assert.equal(poll.changes[1].revision, 2);
  assert.equal(poll.changes[1].beforeRevision, 1);
  assert.equal(poll.changes[2].revision, 2);
  assert.equal(poll.changes[2].beforeRevision, 1);

  // REMOVED exposes pre-removal evidence, which re-checks as a direct scan.
  const removed = poll.changes.find((change) => change.type === 'REMOVED');
  assert.ok(removed.beforeEvidence.phrases.some((entry) => entry.phrase === 'red fox'));
  assert.equal(removed.evidence, null);

  // flush/merge/reclaim never create document change records.
  await store.flush();
  await remove('b');
  await store.flush();
  await store.merge();
  await store.reclaimSegments();
  const afterMaintenance = await sub.poll(sub.sequence);
  assertChangesEqual(
    afterMaintenance.changes,
    expectedChanges(model, query, sub.sequence)
  );
  assert.deepEqual(
    afterMaintenance.changes.slice(-1).map((change) => [change.type, change.id, change.revision]),
    [['REMOVED', 'b', 3]]
  );
  assert.equal(afterMaintenance.currentSequence, model.sequence);

  // Repeated poll with the same watermark is identical; advancing to the
  // latest watermark returns an empty suffix.
  assert.deepEqual((await sub.poll(sub.sequence)).changes, afterMaintenance.changes);
  assert.deepEqual((await sub.poll(model.sequence)).changes, []);
  const suffix = await sub.poll(poll.currentSequence);
  assertChangesEqual(
    suffix.changes,
    expectedChanges(model, query, poll.currentSequence)
  );
  assert.deepEqual(
    suffix.changes.map((change) => [change.sequence, change.type]),
    [[8, 'REMOVED']]
  );

  await sub.close();
  assert.equal(store.stats().activeSubscriptions, 0);
  await store.close();
});

test('subscriptions created at different watermarks independently report suffixes', async () => {
  const path = dir('subscription-suffixes');
  const store = await DocumentStore.open({ directory: path });
  const model = new ReferenceModel();
  const query = { terms: ['fox'] };

  await store.put('a', 'fox', 1);
  model.put('a', 'fox', 1);
  const early = await store.subscribe(query);

  await store.put('b', 'fox two', 1);
  model.put('b', 'fox two', 1);
  const middle = await store.subscribe(query);

  await store.delete('a', 2);
  model.delete('a', 2);

  const earlyPoll = await early.poll(early.sequence);
  assertChangesEqual(
    earlyPoll.changes,
    expectedChanges(model, query, early.sequence)
  );
  const middlePoll = await middle.poll(middle.sequence);
  assertChangesEqual(
    middlePoll.changes,
    expectedChanges(model, query, middle.sequence)
  );
  assert.deepEqual(
    middlePoll.changes.map((change) => [change.type, change.id]),
    [['REMOVED', 'a']]
  );
  await early.close();
  await middle.close();
  await store.close();
});

test('match-all subscriptions report every live/tombstone transition with null evidence', async () => {
  const path = dir('subscription-matchall');
  const store = await DocumentStore.open({ directory: path });
  const model = new ReferenceModel();

  const sub = await store.subscribe({});
  assert.equal(sub.snapshot.total, 0);
  await store.put('a', 'one body', 1);
  model.put('a', 'one body', 1);
  await store.put('b', 'two body', 1);
  model.put('b', 'two body', 1);
  await store.put('a', 'one body revised', 2);
  model.put('a', 'one body revised', 2);
  await store.delete('b', 2);
  model.delete('b', 2);

  const poll = await sub.poll(sub.sequence);
  assertChangesEqual(poll.changes, expectedChanges(model, {}, sub.sequence));
  assert.deepEqual(
    poll.changes.map((change) => change.type),
    ['ADDED', 'ADDED', 'UPDATED', 'REMOVED']
  );
  for (const change of poll.changes) {
    assert.equal(change.evidence, null);
    assert.equal(change.beforeEvidence, null);
  }
  await sub.close();
  await store.close();
});

test('polling past the bounded retention window demands re-establishment', async () => {
  const path = dir('subscription-retention');
  const store = await DocumentStore.open({ directory: path, retentionCommits: 3 });
  const query = { terms: ['fox'] };

  const sub = await store.subscribe(query);
  assert.equal(sub.sequence, 0);

  // Create 5 commits; only the last 3 (3,4,5) are retained.
  await store.put('d1', 'fox', 1);
  await store.put('d2', 'fox', 2);
  await store.put('d3', 'fox', 3);
  await store.put('d4', 'fox', 4);
  await store.put('d5', 'fox', 5);
  assert.equal(store.stats().retainedCommits, 3);

  // Watermark 2 would need commit 3, which is still retained.
  assert.equal((await sub.poll(2)).changes.length, 3);
  // Watermark 1 needs commit 2: gone, so refuse instead of skipping events.
  let expired;
  await assert.rejects(
    async () => {
      try {
        await sub.poll(1);
      } catch (error) {
        expired = error;
        throw error;
      }
    },
    (error) => error.code === 'ERR_SUBSCRIPTION_EXPIRED'
  );
  assert.equal(expired.oldestSequence, 2);
  assert.equal(expired.currentSequence, 5);
  assert.equal(expired.reestablish, true);

  // A fresh subscription starts from the current commit with a full snapshot.
  const fresh = await store.subscribe(query);
  assert.equal(fresh.sequence, 5);
  assert.deepEqual(
    fresh.snapshot.results.map((doc) => doc.id),
    ['d1', 'd2', 'd3', 'd4', 'd5']
  );
  assert.deepEqual((await fresh.poll(5)).changes, []);

  await sub.close();
  await fresh.close();
  await store.close();
});

test('an empty retained feed never expires a subscription at its own watermark', async () => {
  const path = dir('subscription-empty-feed');
  const store = await DocumentStore.open({ directory: path, retentionCommits: 2 });
  const sub = await store.subscribe({ terms: ['fox'] });

  await store.flush(); // no commits, no feed entries
  assert.deepEqual((await sub.poll(0)).changes, []);

  await store.put('a', 'fox', 1);
  await store.put('b', 'fox', 1);
  assert.deepEqual(
    (await sub.poll(0)).changes.map((change) => change.id),
    ['a', 'b']
  );
  await sub.close();
  await store.close();
});

test('invalid watermarks are rejected without skipping data', async () => {
  const path = dir('subscription-watermark');
  const store = await DocumentStore.open({ directory: path });
  const sub = await store.subscribe({ terms: ['fox'] });
  await store.put('a', 'fox', 1);

  const invalidWatermark = (error) => error.code === 'ERR_INVALID_WATERMARK';
  await assert.rejects(sub.poll(-1), invalidWatermark);
  await assert.rejects(sub.poll(sub.sequence - 1), invalidWatermark);
  await assert.rejects(sub.poll(999), invalidWatermark);
  // Rejected polls do not consume anything.
  const valid = await sub.poll(sub.sequence);
  assert.deepEqual(
    valid.changes.map((change) => [change.type, change.id]),
    [['ADDED', 'a']]
  );
  await sub.close();
  assert.throws(
    () => sub.poll(0),
    (error) => error.code === 'ERR_SUBSCRIPTION_CLOSED'
  );
  assert.throws(
    () => store.getSubscription(sub.id),
    (error) => error.code === 'ERR_SUBSCRIPTION_NOT_FOUND'
  );
  await store.close();
});

test('failed WAL append neither advances the commit nor leaks a subscription change', async () => {
  const path = dir('subscription-wal-fault');
  const store = await DocumentStore.open({
    directory: path,
    faultInjection: { walAppend: true }
  });
  const sub = await store.subscribe({ terms: ['fox'] });

  await assert.rejects(
    store.put('a', 'red fox', 1),
    (error) => error.faultStage === 'walAppend'
  );
  await assert.rejects(
    store.delete('a', 2),
    (error) => error.faultStage === 'walAppend'
  );

  assert.equal(store.stats().sequence, 0);
  assert.equal(store.stats().retainedCommits, 0);
  assert.equal(store.getDocument('a'), null);
  assert.deepEqual((await sub.poll(0)).changes, []);
  assert.equal((await sub.poll(0)).currentSequence, 0);
  await sub.close();
  await store.close();

  // On disk the failed appends left nothing behind.
  const reopened = await DocumentStore.open({ directory: path });
  assert.equal(reopened.stats().sequence, 0);
  await reopened.put('a', 'red fox', 1);
  assert.equal(reopened.stats().sequence, 1);
  assert.deepEqual(
    reopened.query({ terms: ['fox'] }).results.map((doc) => doc.id),
    ['a']
  );
  await reopened.close();
});

test('process restart rejects old subscription ids and watermarks', async () => {
  const path = dir('subscription-restart');
  let store = await DocumentStore.open({ directory: path });
  const sub = await store.subscribe({ terms: ['fox'] });
  await store.put('a', 'fox one', 1);
  await store.flush();
  await store.put('b', 'fox two', 1);
  await store.close();

  store = await DocumentStore.open({ directory: path });
  assert.throws(
    () => store.getSubscription(sub.id),
    (error) => error.code === 'ERR_SUBSCRIPTION_NOT_FOUND'
  );

  const fresh = await store.subscribe({ terms: ['fox'] });
  assert.equal(fresh.sequence, 2);
  assert.deepEqual(
    fresh.snapshot.results.map((doc) => doc.id).sort(),
    ['a', 'b']
  );
  assert.deepEqual((await fresh.poll(fresh.sequence)).changes, []);

  // New commits after restart get fresh, gap-free sequence numbers.
  await store.put('c', 'fox three', 2);
  const tail = await fresh.poll(fresh.sequence);
  assert.deepEqual(
    tail.changes.map((change) => [change.type, change.id]),
    [['ADDED', 'c']]
  );
  await fresh.close();
  await store.close();
});

test('held snapshots and pagination cursors are unaffected by subscription retention and changes', async () => {
  const path = dir('subscription-isolation');
  const store = await DocumentStore.open({ directory: path, retentionCommits: 2 });
  const query = { terms: ['fox'] };

  await store.put('a', 'fox one', 1);
  await store.put('b', 'fox two', 1);
  await store.flush();
  const snapshot = await store.snapshot();
  const sub = await store.subscribe(query);

  await store.put('a', 'fox one revised', 2);
  await store.put('c', 'fox three', 1);
  await store.delete('b', 3);
  await store.flush();
  await store.merge();

  // Old snapshot pages still show the pre-subscription content and cursor
  // advances normally.
  const first = snapshot.query(query, { limit: 1 });
  assert.deepEqual(first.results.map((doc) => doc.id), ['a']);
  assert.equal(first.results[0].revision, 1);
  const second = snapshot.queryPage(first.nextCursor);
  assert.deepEqual(second.results.map((doc) => doc.id), ['b']);
  assert.equal(second.nextCursor, undefined);

  await snapshot.close();
  await sub.close();
  await store.close();
});

const VOCAB = ['alpha', 'beta', 'red', 'fox', 'quick', 'brown', 'dog', 'log'];
const PHRASES = ['red fox', 'quick brown', 'brown fox', 'fox log'];

function bodyFor(random) {
  const length = 2 + Math.floor(random() * 7);
  return Array.from({ length }, () => VOCAB[Math.floor(random() * VOCAB.length)]).join(' ');
}

function randomQuery(random) {
  const terms = [];
  for (const term of VOCAB.slice(2)) {
    if (random() < 0.3) terms.push(term);
  }
  const phrases = PHRASES.filter(() => random() < 0.25);
  return { terms, phrases };
}

test('randomized interleave of writes, flushes, merges and restarts matches direct-scan changes', async () => {
  const path = dir('subscription-random');
  const random = lcg(424242);
  let store = await DocumentStore.open({ directory: path, maxDocuments: 30 });
  const model = new ReferenceModel();
  const revisions = new Map();
  const nextRevision = (id) => {
    const revision = (revisions.get(id) ?? 0) + 1;
    revisions.set(id, revision);
    return revision;
  };

  const queries = [randomQuery(random), randomQuery(random), randomQuery(random)];
  let subs = await Promise.all(
    queries.map(async (query) => ({ query, sub: await store.subscribe(query) }))
  );

  let restarted = 0;
  for (let round = 0; round < 10; round++) {
    for (let i = 0; i < 5; i++) {
      const id = `d${Math.floor(random() * 12)}`;
      const revision = nextRevision(id);
      try {
        if (random() < 0.3) {
          await store.delete(id, revision);
          model.delete(id, revision);
        } else {
          const body = bodyFor(random);
          await store.put(id, body, revision);
          model.put(id, body, revision);
        }
      } catch (error) {
        // Revision conflicts and capacity rejections consume no commit.
        if (
          error.code !== 'ERR_REVISION_CONFLICT' &&
          error.code !== 'ERR_STORE_LIMIT'
        ) {
          throw error;
        }
      }
    }
    if (random() < 0.6) await store.flush();
    if (round === 4) await store.merge();
    if (random() < 0.3) await store.reclaimSegments();

    for (const { query, sub } of subs) {
      const watermark = sub.lastWatermark ?? sub.sequence;
      const poll = await sub.poll(watermark);
      const expected = expectedChanges(model, query, watermark);
      assertChangesEqual(poll.changes, expected);
      assert.equal(poll.currentSequence, model.sequence);
      sub.lastWatermark = model.sequence;
    }

    if (round === 3 || round === 7) {
      await store.close();
      store = await DocumentStore.open({ directory: path, maxDocuments: 30 });
      restarted++;
      // Old subscription ids must be refused after restart.
      for (const { sub } of subs) {
        assert.throws(
          () => store.getSubscription(sub.id),
          (error) => error.code === 'ERR_SUBSCRIPTION_NOT_FOUND'
        );
      }
      subs = await Promise.all(
        queries.map(async (query) => ({ query, sub: await store.subscribe(query) }))
      );
      for (const { query, sub } of subs) {
        assert.equal(sub.sequence, model.sequence);
        compareQueryResults(
          assert,
          sub.snapshot.results,
          model.query(query, model.atSequence(model.sequence))
        );
        sub.lastWatermark = model.sequence;
      }
    }
  }

  assert.ok(restarted === 2);
  for (const { sub } of subs) await sub.close();
  await store.close();
});
