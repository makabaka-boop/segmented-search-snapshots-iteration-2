import { createServer } from 'node:http';
import { join } from 'node:path';
import { DocumentStore } from './index.js';

const MAX_BODY_BYTES = 2_000_000;

function json(res, status, value) {
  const body = JSON.stringify(value);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(body)
  });
  res.end(body);
}

async function readJson(req) {
  let size = 0;
  const chunks = [];
  for await (const chunk of req) {
    size += chunk.length;
    if (size > MAX_BODY_BYTES) {
      throw Object.assign(new Error('Request body too large'), { code: 'ERR_BODY_TOO_LARGE' });
    }
    chunks.push(chunk);
  }
  if (!chunks.length) return {};
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

function statusForError(error) {
  if (!String(error.code ?? '').startsWith('ERR_')) return 500;
  // In-process subscription vanished (e.g. process restarted): the client
  // must establish a new subscription and re-snapshot.
  if (error.code === 'ERR_SUBSCRIPTION_UNKNOWN') return 404;
  // Retention window no longer covers the supplied watermark. Skipping
  // events silently is forbidden, so this is an explicit Gone.
  if (error.code === 'ERR_SUBSCRIPTION_WINDOW_EXPIRED') return 410;
  return 400;
}

export async function createDocumentServer(options = {}) {
  const directory = options.directory ?? process.env.STORE_DIR ?? join(process.cwd(), 'data');
  const store = await DocumentStore.open({
    ...options,
    directory
  });

  const server = createServer(async (req, res) => {
    try {
      const url = new URL(req.url, 'http://localhost');
      const path = url.pathname;
      const method = req.method;

      if (method === 'GET' && path === '/health') {
        return json(res, 200, { ok: true, ...store.stats() });
      }
      if (method === 'GET' && path === '/stats') {
        return json(res, 200, store.stats());
      }
      if (method === 'POST' && path === '/documents') {
        const body = await readJson(req);
        return json(res, 200, await store.put(body.id, body.body, body.revision));
      }
      if (method === 'DELETE' && /^\/documents\/[^/]+$/.test(path)) {
        const id = decodeURIComponent(path.split('/').pop());
        const body = await readJson(req);
        return json(res, 200, await store.delete(id, body.revision));
      }
      if (method === 'POST' && path === '/flush') {
        return json(res, 200, await store.flush() ?? { flushed: false });
      }
      if (method === 'POST' && path === '/merge') {
        const body = await readJson(req);
        return json(res, 200, await store.merge(body.segmentIds) ?? { merged: false });
      }
      if (method === 'POST' && path === '/reclaim') {
        await store.reclaimSegments('http');
        return json(res, 200, { reclaimed: true, ...store.stats() });
      }
      if (method === 'POST' && path === '/snapshots') {
        const snapshot = await store.snapshot();
        return json(res, 200, { snapshotId: snapshot.id, sequence: snapshot.sequence });
      }
      if (method === 'POST' && /^\/snapshots\/[^/]+\/query$/.test(path)) {
        const snapshotId = decodeURIComponent(path.split('/')[2]);
        const body = await readJson(req);
        const snapshot = store.getSnapshot(snapshotId);
        return json(res, 200, snapshot.query(body.query ?? {}, { limit: body.limit }));
      }
      if (method === 'POST' && /^\/snapshots\/[^/]+\/query\/next$/.test(path)) {
        const snapshotId = decodeURIComponent(path.split('/')[2]);
        const body = await readJson(req);
        const snapshot = store.getSnapshot(snapshotId);
        return json(res, 200, snapshot.queryPage(body.cursor));
      }
      if (method === 'POST' && /^\/snapshots\/[^/]+\/close$/.test(path)) {
        const snapshotId = decodeURIComponent(path.split('/')[2]);
        await store.getSnapshot(snapshotId).close();
        return json(res, 200, { closed: true });
      }
      if (method === 'POST' && path === '/query') {
        const body = await readJson(req);
        return json(res, 200, store.query(body.query ?? {}, { limit: body.limit }));
      }
      if (method === 'POST' && path === '/query/next') {
        const body = await readJson(req);
        return json(res, 200, store.queryNext(body.cursor));
      }
      if (method === 'POST' && path === '/cursors/close') {
        const body = await readJson(req);
        await store.closeCursor(body.cursor);
        return json(res, 200, { closed: true });
      }
      if (method === 'POST' && path === '/subscriptions') {
        const body = await readJson(req);
        return json(
          res,
          200,
          await store.subscribe(body.query ?? {}, { limit: body.limit })
        );
      }
      if (method === 'POST' && /^\/subscriptions\/[^/]+\/poll$/.test(path)) {
        const subscriptionId = decodeURIComponent(path.split('/')[2]);
        const body = await readJson(req);
        return json(
          res,
          200,
          await store.pollSubscription(subscriptionId, {
            watermark: body.watermark,
            limit: body.limit
          })
        );
      }
      if (method === 'POST' && /^\/subscriptions\/[^/]+\/close$/.test(path)) {
        const subscriptionId = decodeURIComponent(path.split('/')[2]);
        return json(res, 200, await store.closeSubscription(subscriptionId));
      }

      return json(res, 404, { error: 'not found' });
    } catch (error) {
      return json(res, statusForError(error), {
        error: error.message,
        code: error.code ?? null,
        faultStage: error.faultStage ?? null,
        resubscribe: error.resubscribe ?? false,
        // Present when supplied by subscription boundary errors so clients can
        // decide which watermark to resubscribe from.
        ...(error.watermark !== undefined ? { watermark: error.watermark } : {}),
        ...(error.windowStart !== undefined ? { windowStart: error.windowStart } : {}),
        ...(error.currentSequence !== undefined ? { currentSequence: error.currentSequence } : {})
      });
    }
  });

  server.stop = async () => {
    await new Promise((resolve) => server.close(resolve));
    await store.close();
  };
  server.store = store;
  return server;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const port = Number(process.env.PORT ?? 8080);
  const server = await createDocumentServer();
  server.listen(port, () => {
    console.log(`document store listening on :${port}`);
  });
}
