import test from 'node:test';
import assert from 'node:assert/strict';
import { DocumentStore } from '../src/index.js';
import { cleanup, compareQueryResults, lcg, makeTempDir, ReferenceModel } from './reference.js';

const VOCAB = ['alpha', 'beta', 'gamma', 'delta', 'red', 'fox', 'quick', 'brown', 'dog', 'log'];
const PHRASES = [
  'red fox',
  'quick brown',
  'brown fox',
  'alpha beta',
  'gamma delta',
  'fox log'
];
const dirs = [];

function bodyFor(random) {
  const length = 2 + Math.floor(random() * 8);
  return Array.from({ length }, () => VOCAB[Math.floor(random() * VOCAB.length)]).join(' ');
}

function randomQuery(random) {
  const terms = [];
  const phrases = [];
  for (const term of VOCAB.slice(4)) {
    if (random() < 0.28) terms.push(term);
  }
  for (const phrase of PHRASES) {
    if (random() < 0.22) phrases.push(phrase);
  }
  return { terms, phrases };
}

test('randomized snapshots and pagination match direct scan through flush and merge', async () => {
  const path = makeTempDir('property');
  dirs.push(path);
  const store = await DocumentStore.open({ directory: path, maxDocuments: 80 });
  const model = new ReferenceModel();
  const random = lcg(20261001);
  const revisions = new Map();
  const liveIds = new Set();
  const heldSnapshots = [];

  function nextRevision(id) {
    const revision = (revisions.get(id) ?? 0) + 1;
    revisions.set(id, revision);
    return revision;
  }

  for (let round = 0; round < 12; round++) {
    for (let i = 0; i < 8; i++) {
      if (random() < 0.22 && liveIds.size) {
        const id = [...liveIds][Math.floor(random() * liveIds.size)];
        const revision = nextRevision(id);
        await store.delete(id, revision);
        model.delete(id, revision);
        liveIds.delete(id);
      } else if (liveIds.size < 65) {
        const id = `d${Math.floor(random() * 70)}`;
        if (!revisions.has(id) || !liveIds.has(id)) {
          const revision = nextRevision(id);
          const body = bodyFor(random);
          await store.put(id, body, revision);
          model.put(id, body, revision);
          liveIds.add(id);
        }
      }
    }

    const snapshot = await store.snapshot();
    heldSnapshots.push(snapshot);
    const query = randomQuery(random);
    const expected = model.query(query, model.atSequence(snapshot.sequence));
    const limit = 1 + Math.floor(random() * 4);
    const first = snapshot.query(query, { limit });

    assert.equal(first.sequence, snapshot.sequence);
    let rows = first.results;
    let cursor = first.nextCursor;
    while (cursor) {
      const currentCursor = cursor;
      const next = snapshot.queryPage(cursor);
      assert.throws(() => snapshot.queryPage(currentCursor), /already advanced/);
      rows = rows.concat(next.results);
      cursor = next.nextCursor;
    }
    compareQueryResults(assert, rows, expected);

    if (random() < 0.7) await store.flush();
    if (round === 5) await store.merge();

    // Mutations after snapshot creation cannot enter a page belonging to it.
    for (let i = 0; i < 3; i++) {
      const id = `noise${round}-${i}`;
      const revision = nextRevision(id);
      const body = `red fox ${query.terms.join(' ')} ${query.phrases.join(' ')}`;
      try {
        await store.put(id, body, revision);
        model.put(id, body, revision);
        liveIds.add(id);
      } catch {
        // The bounded store may legitimately reject replacement noise.
      }
    }
  }

  for (const snapshot of heldSnapshots) await snapshot.close();
  await store.close();
  cleanup(dirs);
});
