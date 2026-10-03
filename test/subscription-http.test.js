import test from 'node:test';
import assert from 'node:assert/strict';
import { createDocumentServer } from '../src/server.js';
import { cleanup, makeTempDir } from './reference.js';

const dirs = [];
test.after(() => cleanup(dirs));

async function start(label) {
  const path = makeTempDir(label);
  dirs.push(path);
  const server = await createDocumentServer({ directory: path, subscriptionRetention: 3 });
  await new Promise((resolve) => server.listen(0, resolve));
  const port = server.address().port;
  const base = `http://127.0.0.1:${port}`;

  async function call(method, route, body) {
    const response = await fetch(`${base}${route}`, {
      method,
      headers: { 'content-type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body)
    });
    const json = await response.json();
    return { status: response.status, json };
  }
  return { server, call, stop: () => server.stop() };
}

test('HTTP subscription lifecycle: create, initial snapshot, polled changes and close', async (t) => {
  const { server, call, stop } = await start('http-sub');
  t.after(stop);

  await call('POST', '/documents', { id: 'a', body: 'red fox first', revision: 1 });
  const created = await call('POST', '/subscriptions', {
    query: { terms: ['red', 'fox'] },
    limit: 10
  });
  assert.equal(created.status, 200);
  assert.ok(created.json.subscriptionId);
  assert.equal(created.json.watermark, 1);
  assert.deepEqual(created.json.results.map((row) => row.id), ['a']);
  const subscriptionId = created.json.subscriptionId;

  await call('POST', '/documents', { id: 'b', body: 'red fox second', revision: 1 });
  await call('DELETE', '/documents/a', { revision: 2 });
  await call('POST', '/documents', { id: 'c', body: 'unrelated text', revision: 1 });

  const poll = await call('POST', `/subscriptions/${subscriptionId}/poll`, { watermark: 1 });
  assert.equal(poll.status, 200);
  // Commit 3 (the unrelated doc) still advances the commit sequence even
  // though it yields no change record.
  assert.equal(poll.json.watermark, 4);
  assert.deepEqual(
    poll.json.changes.map((record) => [record.type, record.id]),
    [['ADDED', 'b'], ['REMOVED', 'a']]
  );
  assert.equal(poll.json.changes[1].before.revision, 1);
  assert.equal(poll.json.changes[0].after.evidence.terms.fox[0].position, 1);

  // Idempotent replay with the same watermark returns the same records.
  const replay = await call('POST', `/subscriptions/${subscriptionId}/poll`, { watermark: 1 });
  assert.deepEqual(replay.json.changes, poll.json.changes);

  // flush/merge/reclaim never produce records.
  await call('POST', '/flush', {});
  await call('POST', '/merge', {});
  await call('POST', '/reclaim', {});
  const quiet = await call('POST', `/subscriptions/${subscriptionId}/poll`, { watermark: 4 });
  assert.deepEqual(quiet.json.changes, []);

  const closed = await call('POST', `/subscriptions/${subscriptionId}/close`, {});
  assert.equal(closed.status, 200);
  const gone = await call('POST', `/subscriptions/${subscriptionId}/poll`, {});
  assert.equal(gone.status, 404);
  assert.equal(gone.json.code, 'ERR_SUBSCRIPTION_UNKNOWN');
  assert.equal(gone.json.resubscribe, true);

  await server.stop();
});

test('HTTP polls past the retention window get 410 Gone with a resubscribe hint', async (t) => {
  const { server, call, stop } = await start('http-retention');
  t.after(stop);

  const created = await call('POST', '/subscriptions', { query: { terms: ['fox'] } });
  const subscriptionId = created.json.subscriptionId;
  for (let seq = 1; seq <= 5; seq++) {
    await call('POST', '/documents', { id: `d${seq}`, body: `fox body ${seq}`, revision: 1 });
  }

  const expired = await call('POST', `/subscriptions/${subscriptionId}/poll`, { watermark: 0 });
  assert.equal(expired.status, 410);
  assert.equal(expired.json.code, 'ERR_SUBSCRIPTION_WINDOW_EXPIRED');
  assert.equal(expired.json.resubscribe, true);
  assert.ok(expired.json.windowStart > 0);
  assert.equal(expired.json.currentSequence, 5);

  // A fresh watermark inside the window still pages normally.
  const recent = await call('POST', `/subscriptions/${subscriptionId}/poll`, { watermark: 2 });
  assert.equal(recent.status, 200);
  assert.deepEqual(recent.json.changes.map((record) => record.id), ['d3', 'd4', 'd5']);

  await server.stop();
});

test('HTTP subscriptions are process-local: a restarted server rejects old ids', async (t) => {
  const path = makeTempDir('http-restart');
  dirs.push(path);
  let server = await createDocumentServer({ directory: path });
  t.after(() => server.stop());
  await new Promise((resolve) => server.listen(0, resolve));
  const port = server.address().port;
  const base = `http://127.0.0.1:${port}`;
  async function call(method, route, body) {
    const response = await fetch(`${base}${route}`, {
      method,
      headers: { 'content-type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body)
    });
    return { status: response.status, json: await response.json() };
  }

  await call('POST', '/documents', { id: 'a', body: 'fox durable', revision: 1 });
  const created = await call('POST', '/subscriptions', { query: { terms: ['fox'] } });
  const oldId = created.json.subscriptionId;
  await server.stop();

  server = await createDocumentServer({ directory: path });
  await new Promise((resolve) => server.listen(0, resolve));
  const port2 = server.address().port;
  async function call2(method, route, body) {
    const response = await fetch(`http://127.0.0.1:${port2}${route}`, {
      method,
      headers: { 'content-type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body)
    });
    return { status: response.status, json: await response.json() };
  }

  const stale = await call2('POST', `/subscriptions/${oldId}/poll`, { watermark: 1 });
  assert.equal(stale.status, 404);
  assert.equal(stale.json.code, 'ERR_SUBSCRIPTION_UNKNOWN');
  assert.equal(stale.json.resubscribe, true);

  const fresh = await call2('POST', '/subscriptions', { query: { terms: ['fox'] } });
  assert.equal(fresh.status, 200);
  assert.equal(fresh.json.watermark, 1);
  assert.deepEqual(fresh.json.results.map((row) => row.id), ['a']);

  await server.stop();
});
