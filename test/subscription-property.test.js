import test from 'node:test';
import assert from 'node:assert/strict';
import { DocumentStore } from '../src/index.js';
import { cleanup, lcg, makeTempDir, ReferenceModel } from './reference.js';

const VOCAB = ['alpha', 'beta', 'gamma', 'delta', 'red', 'fox', 'quick', 'brown', 'dog', 'log'];
const PHRASES = ['red fox', 'quick brown', 'brown fox', 'alpha beta', 'fox log'];
const dirs = [];

function bodyFor(random) {
  const length = 2 + Math.floor(random() * 8);
  return Array.from({ length }, () => VOCAB[Math.floor(random() * VOCAB.length)]).join(' ');
}

function randomQuery(random) {
  const terms = [];
  const phrases = [];
  for (const term of VOCAB.slice(4)) {
    if (random() < 0.3) terms.push(term);
  }
  for (const phrase of PHRASES) {
    if (random() < 0.24) phrases.push(phrase);
  }
  return { terms, phrases };
}

test('interleaved puts, deletes, flush, merge and restarts keep subscription changes exact', async () => {
  const path = makeTempDir('sub-property');
  dirs.push(path);
  const random = lcg(20261003);
  const model = new ReferenceModel();
  const revisions = new Map();

  let store = await DocumentStore.open({
    directory: path,
    maxDocuments: 40,
    subscriptionRetention: 1000
  });

  const query = { terms: ['red', 'fox'], phrases: ['red fox'] };
  let sub = await store.subscribe(query, { limit: 2 });
  let watermark = sub.watermark;

  // Initial snapshot must equal the model at the subscribing commit.
  function assertInitialSnapshot(page, atSequence) {
    const expected = model.query(query, model.atSequence(atSequence));
    let rows = page.results;
    let cursor = page.nextCursor;
    while (cursor) {
      const next = store.queryNext(cursor);
      rows = rows.concat(next.results);
      cursor = next.nextCursor;
    }
    assert.equal(page.watermark, atSequence);
    assert.deepEqual(
      rows.map((row) => row.id).sort(),
      [...expected.keys()].sort()
    );
  }
  assertInitialSnapshot(sub, watermark);

  const nextRevision = (id) => {
    const revision = (revisions.get(id) ?? 0) + 1;
    revisions.set(id, revision);
    return revision;
  };

  for (let round = 0; round < 14; round++) {
    const roundStartWatermark = watermark;
    // 1..6 interleaved commits per round.
    const commitCount = 1 + Math.floor(random() * 6);
    for (let i = 0; i < commitCount; i++) {
      const id = `d${Math.floor(random() * 30)}`;
      const revision = nextRevision(id);
      if (random() < 0.28) {
        try {
          await store.delete(id, revision);
          model.delete(id, revision);
        } catch (error) {
          // Revision conflicts against the live model never happen because we
          // track revisions identically; only the cap can reject a recreate.
          if (error.code !== 'ERR_STORE_LIMIT') throw error;
          revisions.set(id, revision - 1);
        }
      } else {
        const body = bodyFor(random);
        try {
          await store.put(id, body, revision);
          model.put(id, body, revision);
        } catch (error) {
          if (error.code !== 'ERR_STORE_LIMIT') throw error;
          revisions.set(id, revision - 1);
        }
      }
    }

    // Poll from the last delivered watermark, sometimes page-limited.
    const useLimit = random() < 0.5;
    let poll = await store.pollSubscription(sub.subscriptionId, useLimit ? {
      watermark,
      limit: 1 + Math.floor(random() * 3)
    } : { watermark });

    let got = poll.changes;
    watermark = poll.watermark;
    while (poll.hasMore) {
      poll = await store.pollSubscription(sub.subscriptionId, { watermark, limit: 2 });
      got = got.concat(poll.changes);
      watermark = poll.watermark;
    }

    const expected = model.subscriptionChanges(
      query,
      // Compare only this round's window against the model between the
      // watermark before the round and the current commit.
      roundStartWatermark,
      model.sequence
    );
    assert.deepEqual(got, expected, `round ${round} change sequence matches direct scan`);
    assert.equal(watermark, model.sequence);

    // Structural invariants.
    let previousSequence = -1;
    for (const record of got) {
      assert.ok(record.sequence > previousSequence, 'records ordered by commit sequence');
      previousSequence = record.sequence;
      assert.ok(['ADDED', 'REMOVED', 'UPDATED'].includes(record.type));
      if (record.type === 'ADDED') assert.equal(record.before, null);
      if (record.type === 'REMOVED') assert.equal(record.after, null);
      if (record.type === 'UPDATED') {
        assert.ok(record.before.matched && record.after.matched);
        assert.ok(record.after.revision > record.before.revision);
      }
      // Evidence is independently recomputable by scanning the doc body.
      const side = record.after ?? record.before;
      if (side && side.evidence) {
        for (const phrase of side.evidence.phrases) {
          assert.deepEqual(phrase.terms, ['red', 'fox']);
          assert.ok(phrase.starts.length > 0);
        }
      }
    }

    // Maintenance operations: they must never generate changes.
    if (random() < 0.6) await store.flush();
    if (round === 4 || round === 9) await store.merge();
    if (random() < 0.3) await store.reclaimSegments();

    const quiet = await store.pollSubscription(sub.subscriptionId, { watermark });
    assert.deepEqual(quiet.changes, [], `maintenance emits no changes in round ${round}`);
    assert.equal(quiet.currentSequence, model.sequence);

    // The live query result always equals the model's direct scan.
    const live = store.query(query);
    assert.deepEqual(
      live.results.map((row) => row.id).sort(),
      [...model.query(query).keys()].sort(),
      `live query matches model in round ${round}`
    );

    // Every few rounds, crash-simulate a restart. The subscription must die
    // with the process and a new one resume from the recovered sequence.
    if (round === 5 || round === 11) {
      const oldSubscriptionId = sub.subscriptionId;
      await store.close();
      store = await DocumentStore.open({
        directory: path,
        maxDocuments: 40,
        subscriptionRetention: 1000
      });
      assert.equal(store.stats().sequence, model.sequence);
      await assert.rejects(
        store.pollSubscription(oldSubscriptionId, { watermark }),
        (error) => error.code === 'ERR_SUBSCRIPTION_UNKNOWN' && error.resubscribe === true
      );
      sub = await store.subscribe(query, { limit: 5 });
      assertInitialSnapshot(sub, model.sequence);
      watermark = model.sequence;
    }
  }

  await store.close();
  cleanup(dirs);
});
