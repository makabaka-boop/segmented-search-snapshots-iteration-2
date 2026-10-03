import test from 'node:test';
import assert from 'node:assert/strict';
import { request as httpRequest } from 'node:http';
import { createDocumentServer } from '../src/server.js';
import { cleanup, makeTempDir } from './reference.js';

const dirs = [];
test.after(() => cleanup(dirs));

async function stopServer(server) {
  // server.stop() closes the listening socket and the store, but lingering
  // keep-alive sockets can still pin the test event loop. Destroy any active
  // connection first.
  server.closeAllConnections?.();
  await server.stop();
}

function request(server, method, path, body) {
  const payload = body === undefined ? null : Buffer.from(JSON.stringify(body));
  return new Promise((resolve, reject) => {
    const req = httpRequest(
      {
        method,
        host: '127.0.0.1',
        port: server.address().port,
        path,
        headers: {
          connection: 'close',
          ...(payload
            ? { 'content-type': 'application/json', 'content-length': payload.length }
            : {})
        }
      },
      (response) => {
        const chunks = [];
        response.on('data', (chunk) => chunks.push(chunk));
        response.on('end', () => {
          const text = Buffer.concat(chunks).toString('utf8');
          resolve({ status: response.statusCode, body: text ? JSON.parse(text) : {} });
        });
      }
    );
    req.on('error', reject);
    if (payload) req.end(payload);
    else req.end();
  });
}

test('HTTP subscription lifecycle: create, poll idempotently, expire and restart handling', async () => {
  const path = makeTempDir('http-subscriptions');
  dirs.push(path);

  let server = await createDocumentServer({
    directory: path,
    retentionCommits: 3
  });
  await new Promise((resolve) => server.listen(0, resolve));

  await request(server, 'POST', '/documents', { id: 'a', body: 'red fox one', revision: 1 });

  const created = await request(server, 'POST', '/subscriptions', {
    query: { terms: ['fox'] }
  });
  assert.equal(created.status, 200);
  const subId = created.body.subscriptionId;
  assert.equal(created.body.watermark, 1);
  assert.deepEqual(created.body.results.map((doc) => doc.id), ['a']);

  await request(server, 'POST', '/documents', { id: 'b', body: 'red fox two', revision: 1 });
  await request(server, 'POST', '/documents', { id: 'c', body: 'red fox three', revision: 1 });
  await request(server, 'DELETE', '/documents/a', { revision: 2 });

  // Repeated polls with the same watermark return identical records.
  const first = await request(server, 'POST', `/subscriptions/${subId}/poll`, { watermark: 1 });
  const again = await request(server, 'POST', `/subscriptions/${subId}/poll`, { watermark: 1 });
  assert.equal(first.status, 200);
  assert.deepEqual(first.body, again.body);
  assert.deepEqual(
    first.body.changes.map((change) => [change.sequence, change.type, change.id]),
    [
      [2, 'ADDED', 'b'],
      [3, 'ADDED', 'c'],
      [4, 'REMOVED', 'a']
    ]
  );
  assert.equal(first.body.changes[2].beforeRevision, 1);
  assert.ok(first.body.changes[2].beforeEvidence);

  // Push the store past the 3-commit retention window. Sequences 2..4 are
  // discarded while 5..7 remain; a watermark of 3 would need the missing
  // sequence 4 and must demand re-establishment rather than skip it.
  await request(server, 'POST', '/documents', { id: 'e', body: 'fox five', revision: 1 });
  await request(server, 'POST', '/documents', { id: 'f', body: 'fox six', revision: 1 });
  await request(server, 'POST', '/documents', { id: 'g', body: 'fox seven', revision: 1 });

  const expired = await request(server, 'POST', `/subscriptions/${subId}/poll`, {
    watermark: 3
  });
  assert.equal(expired.status, 410);
  assert.equal(expired.body.code, 'ERR_SUBSCRIPTION_EXPIRED');
  assert.equal(expired.body.reestablish, true);
  assert.equal(expired.body.currentSequence, 7);

  // A watermark still inside the window serves its suffix; a future
  // watermark is a 400 and never skips events.
  const within = await request(server, 'POST', `/subscriptions/${subId}/poll`, {
    watermark: 4
  });
  assert.equal(within.status, 200);
  assert.deepEqual(
    within.body.changes.map((change) => change.id),
    ['e', 'f', 'g']
  );
  const bad = await request(server, 'POST', `/subscriptions/${subId}/poll`, {
    watermark: 99
  });
  assert.equal(bad.status, 400);
  assert.equal(bad.body.code, 'ERR_INVALID_WATERMARK');

  const closed = await request(server, 'POST', `/subscriptions/${subId}/close`, {});
  assert.equal(closed.status, 200);
  const afterClose = await request(server, 'POST', `/subscriptions/${subId}/poll`, {
    watermark: 7
  });
  assert.equal(afterClose.status, 404);
  assert.equal(afterClose.body.code, 'ERR_SUBSCRIPTION_NOT_FOUND');

  // Unknown subscription is a 404.
  const unknown = await request(server, 'POST', '/subscriptions/sub_nope/poll', {
    watermark: 0
  });
  assert.equal(unknown.status, 404);
  assert.equal(unknown.body.code, 'ERR_SUBSCRIPTION_NOT_FOUND');

  await stopServer(server);

  // Restart: the old subscription id is rejected (process-scoped state).
  server = await createDocumentServer({ directory: path, retentionCommits: 3 });
  await new Promise((resolve) => server.listen(0, resolve));
  const stale = await request(server, 'POST', `/subscriptions/${subId}/poll`, {
    watermark: 1
  });
  assert.equal(stale.status, 404);
  assert.equal(stale.body.code, 'ERR_SUBSCRIPTION_NOT_FOUND');

  const rebuilt = await request(server, 'POST', '/subscriptions', {
    query: { terms: ['fox'] }
  });
  assert.equal(rebuilt.status, 200);
  assert.equal(rebuilt.body.watermark, 7);
  assert.deepEqual(
    rebuilt.body.results.map((doc) => doc.id).sort(),
    ['b', 'c', 'e', 'f', 'g']
  );

  // Commits after a restart still interleave with normal document traffic.
  await request(server, 'POST', '/documents', { id: 'd', body: 'fox four', revision: 1 });
  const tail = await request(server, 'POST', `/subscriptions/${rebuilt.body.subscriptionId}/poll`, {
    watermark: 7
  });
  assert.deepEqual(
    tail.body.changes.map((change) => [change.type, change.id]),
    [['ADDED', 'd']]
  );

  await stopServer(server);
  // The node:test process would otherwise stay alive on lingering keep-alive
  // watchers even though every response already closed its connection.
  process.exitCode = 0;
});
