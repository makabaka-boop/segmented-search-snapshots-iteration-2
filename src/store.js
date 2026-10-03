import { randomUUID } from 'node:crypto';
import {
  existsSync,
  readdirSync,
  readFileSync,
  rmSync,
  unlinkSync
} from 'node:fs';
import { join } from 'node:path';
import { atomicWriteJson, fsyncDirectory, mkdirp } from './atomic-file.js';
import { deepFreeze } from './freeze.js';
import { buildPostings } from './tokenizer.js';
import { WriteAheadLog } from './wal.js';
import { IndexSnapshot } from './snapshot.js';
import { evaluateDocument, matchQuery, normalizeQuery } from './query.js';

const MANIFEST_FILE = 'manifest.json';
const WAL_FILE = 'wal.log';
const STORE_VERSION = 1;

export class RevisionConflictError extends Error {
  constructor(message) {
    super(message);
    this.name = 'RevisionConflictError';
    this.code = 'ERR_REVISION_CONFLICT';
  }
}

export class StoreLimitError extends Error {
  constructor(message) {
    super(message);
    this.name = 'StoreLimitError';
    this.code = 'ERR_STORE_LIMIT';
  }
}

export class DocumentStore {
  constructor(options = {}) {
    this.directory = options.directory;
    this.maxDocuments = options.maxDocuments ?? 1000;
    this.cursorTtlMs = options.cursorTtlMs ?? 10 * 60 * 1000;
    this.#faults = options.faultInjection ?? {};

    this.manifest = null;
    this.segments = new Map();
    this.base = new Map();
    this.buffer = new Map();
    this.bufferSequence = 0;
    this.wal = null;
    this.snapshots = new Map();
    this.cursors = new Map();
    this.subscriptions = new Map();
    this.subscriptionRetention = options.subscriptionRetention ?? 1000;
    if (
      !Number.isSafeInteger(this.subscriptionRetention) ||
      this.subscriptionRetention < 1
    ) {
      throw Object.assign(new Error('subscriptionRetention must be a positive safe integer'), {
        code: 'ERR_INVALID_SUBSCRIPTION_RETENTION'
      });
    }
    this.closed = false;
    this.#chain = Promise.resolve();
    this.#reaper = setInterval(() => this.#expireCursors(), this.cursorTtlMs);
    if (this.#reaper.unref) this.#reaper.unref();
  }

  #faults;
  #chain;
  #reaper;

  static async open(options) {
    if (!options?.directory) {
      throw new Error('directory is required');
    }
    const store = new DocumentStore(options);
    await store.#recover();
    return store;
  }

  #fail(stage, context = {}) {
    const configured = this.#faults[stage];
    if (!configured) return;
    // The WAL-append fault is one-shot: it models a single torn append, so a
    // retried append after reopen must succeed. Consume it only after the
    // enabled check above (assignment would otherwise make #fail a no-op).
    if (stage === 'walAppend') this.#faults.walAppend = false;
    const error = configured === true ? new Error(`Injected failure: ${stage}`) : configured;
    error.faultStage = stage;
    error.faultContext = context;
    throw error;
  }

  #manifestPath() {
    return join(this.directory, MANIFEST_FILE);
  }

  #walPath() {
    return join(this.directory, WAL_FILE);
  }

  #segmentPath(id) {
    return join(this.directory, `segment-${String(id).padStart(8, '0')}.json`);
  }

  async #recover() {
    mkdirp(this.directory);

    for (const name of readdirSync(this.directory)) {
      if (name.endsWith('.tmp')) {
        rmSync(join(this.directory, name), { force: true });
      }
    }

    const manifestPath = this.#manifestPath();
    if (existsSync(manifestPath)) {
      this.manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
      if (
        this.manifest.version !== STORE_VERSION ||
        !Array.isArray(this.manifest.segmentIds) ||
        !Number.isSafeInteger(this.manifest.lastSequence) ||
        !Number.isSafeInteger(this.manifest.nextSegmentId)
      ) {
        throw Object.assign(new Error('Invalid index manifest'), { code: 'ERR_CORRUPT_MANIFEST' });
      }
    } else {
      this.manifest = {
        version: STORE_VERSION,
        lastSequence: 0,
        segmentIds: [],
        nextSegmentId: 1
      };
    }

    const referenced = new Set(this.manifest.segmentIds);
    const segmentFiles = readdirSync(this.directory)
      .filter((name) => /^segment-\d{8}\.json$/.test(name))
      .map((name) => {
        const id = Number(name.slice('segment-'.length, -'.json'.length));
        return { id, name, path: join(this.directory, name) };
      });

    for (const file of segmentFiles) {
      if (!referenced.has(file.id)) {
        rmSync(file.path, { force: true });
      }
    }
    fsyncDirectory(this.directory);

    for (const id of this.manifest.segmentIds) {
      const path = this.#segmentPath(id);
      if (!existsSync(path)) {
        throw Object.assign(new Error(`Missing segment ${id}`), { code: 'ERR_MISSING_SEGMENT' });
      }
      const rawSegment = JSON.parse(readFileSync(path, 'utf8'));
      const segment = deepFreeze(this.#validateSegment(id, rawSegment));
      this.segments.set(id, segment);
      for (const doc of segment.docs) {
        const current = this.base.get(doc.id);
        if (!current || doc.sequence > current.sequence) {
          this.base.set(doc.id, doc);
        }
      }
    }

    // Sequence numbers continue from the durable manifest so replayed WAL
    // records and writes after reopen stay strictly increasing across
    // restarts instead of restarting at 1.
    this.bufferSequence = this.manifest.lastSequence;
    const records = WriteAheadLog.readRecords(this.#walPath(), { repairTornTail: true });
    for (const record of records) {
      if (record.sequence <= this.manifest.lastSequence) continue;
      this.#applyRecord(record, { recovery: true });
    }

    this.wal = new WriteAheadLog(this.#walPath(), {
      hooks: {
        afterFirstChunk: (context) => this.#fail('walAppend', context)
      }
    });
    fsyncDirectory(this.directory);
  }

  #withLock(job) {
    const run = this.#chain.then(() => job());
    // A failed background job must not make later operations unchainable.
    this.#chain = run.then(
      () => undefined,
      () => undefined
    );
    return run;
  }

  #ensureOpen() {
    if (this.closed) {
      throw Object.assign(new Error('Store is closed'), { code: 'ERR_STORE_CLOSED' });
    }
  }

  #current(id) {
    return this.buffer.has(id) ? this.buffer.get(id) : this.base.get(id);
  }

  #assertRevision(operation, id, revision, current) {
    if (!Number.isSafeInteger(revision) || revision <= 0) {
      throw Object.assign(new Error('revision must be a positive safe integer'), {
        code: 'ERR_INVALID_REVISION'
      });
    }
    if (current && revision <= current.revision) {
      throw new RevisionConflictError(
        `${operation} revision ${revision} for ${id} is not newer than ${current.revision}`
      );
    }
  }

  #applyRecord(record) {
    if (
      !record ||
      !Number.isSafeInteger(record.sequence) ||
      record.sequence <= this.manifest.lastSequence
    ) {
      throw Object.assign(new Error('Invalid WAL sequence'), { code: 'ERR_CORRUPT_WAL' });
    }
    const expectedSequence = this.bufferSequence + 1;
    if (record.sequence !== expectedSequence) {
      throw Object.assign(new Error(`Expected WAL sequence ${expectedSequence}, got ${record.sequence}`), {
        code: 'ERR_CORRUPT_WAL'
      });
    }
    this.bufferSequence = Math.max(this.bufferSequence, record.sequence);
    if (record.type === 'put') {
      const positions = buildPostings(record.body);
      this.buffer.set(record.id, {
        id: record.id,
        revision: record.revision,
        sequence: record.sequence,
        deleted: false,
        body: record.body,
        postings: positions
      });
    } else if (record.type === 'delete') {
      this.buffer.set(record.id, {
        id: record.id,
        revision: record.revision,
        sequence: record.sequence,
        deleted: true,
        body: null,
        postings: new Map()
      });
    } else {
      throw Object.assign(new Error(`Unknown WAL type: ${record.type}`), {
        code: 'ERR_CORRUPT_WAL'
      });
    }
  }

  #nextSequence() {
    return this.bufferSequence + 1;
  }

  // ---- In-process query subscriptions ------------------------------------
  //
  // A subscription pins only a normalized query, the watermark (commit
  // sequence already delivered) and a bounded Map of change records. It never
  // references segments, so flush/merge/reclaim and snapshot/cursor lifetimes
  // are unaffected. The registry lives in process memory only: after reopen an
  // old subscription id (and its watermark) is simply unknown.

  subscribe(query, options = {}) {
    const limit = options.limit ?? 50;
    if (!Number.isInteger(limit) || limit <= 0 || limit > 1000) {
      throw Object.assign(new Error('limit must be an integer between 1 and 1000'), {
        code: 'ERR_INVALID_LIMIT'
      });
    }
    // Validate the query before entering the lock so a malformed query never
    // creates a snapshot or a subscription.
    const plan = normalizeQuery(query);
    return this.#withLock(() => {
      this.#ensureOpen();
      const sequence = this.bufferSequence;
      const id = `sub_${randomUUID()}`;
      this.subscriptions.set(id, {
        id,
        query: plan,
        watermark: sequence,
        // Sequence of the oldest record still retained; null while the window
        // is empty, in which case every watermark <= current sequence simply
        // has nothing to deliver.
        trimHorizon: null,
        changes: new Map()
      });

      // The initial snapshot is taken on the same commit and inside the same
      // serialized job that registered the watermark, so its sequence is
      // exactly the watermark and its pages can never see or miss a commit.
      const snapshot = this.#temporarySnapshot();
      let result;
      try {
        result = snapshot.query(plan, { limit });
      } catch (error) {
        this.snapshots.delete(snapshot.id);
        this.subscriptions.delete(id);
        throw error;
      }
      if (!result.nextCursor) this.snapshots.delete(snapshot.id);
      result.subscriptionId = id;
      result.watermark = sequence;
      return result;
    });
  }

  #versionSide(version, plan) {
    if (!version) return null;
    if (version.deleted) {
      return {
        revision: version.revision,
        sequence: version.sequence,
        deleted: true,
        body: null,
        matched: false,
        evidence: null
      };
    }
    const { matched, evidence } = evaluateDocument(version, plan);
    return {
      revision: version.revision,
      sequence: version.sequence,
      deleted: false,
      body: version.body,
      matched,
      evidence: matched ? this.#cloneEvidence(evidence) : null
    };
  }

  #cloneEvidence(evidence) {
    // Evidence references in-memory posting arrays. Copy via JSON so callers
    // cannot mutate index internals through delivered change records.
    return evidence == null ? null : JSON.parse(JSON.stringify(evidence));
  }

  // Runs synchronously at the tail of a successful put/delete job, still
  // holding the commit lock, so every subscription observes the same commit
  // boundary as the storage layer. Flush, merge and reclaim never call it.
  #publishCommit(sequence, docId, before, after) {
    for (const subscription of this.subscriptions.values()) {
      const plan = subscription.query;
      const beforeSide = this.#versionSide(before, plan);
      const afterSide = this.#versionSide(after, plan);
      const wasMatched = beforeSide !== null && !beforeSide.deleted && beforeSide.matched;
      const isMatched = afterSide !== null && !afterSide.deleted && afterSide.matched;

      let type;
      if (isMatched && wasMatched) type = 'UPDATED';
      else if (isMatched && !wasMatched) type = 'ADDED';
      else if (!isMatched && wasMatched) type = 'REMOVED';
      else continue; // neither version is a live match: no result-set change

      const record = deepFreeze({
        sequence,
        type,
        id: docId,
        before: wasMatched ? beforeSide : null,
        after: isMatched ? afterSide : null
      });
      subscription.changes.set(sequence, record);

      while (subscription.changes.size > this.subscriptionRetention) {
        const oldestKey = subscription.changes.keys().next().value;
        subscription.changes.delete(oldestKey);
        subscription.trimHorizon = oldestKey + 1;
      }
      // If trimming drained the window, subsequent unrelated commits (that
      // produce no records) must not strand the subscription: reset to null so
      // a poll at the latest state legitimately reports "no changes".
      if (subscription.changes.size === 0) subscription.trimHorizon = null;
    }
  }

  #getSubscription(subscriptionId) {
    const subscription = this.subscriptions.get(subscriptionId);
    if (!subscription) {
      throw Object.assign(
        new Error('Subscription is unknown or expired; create a new subscription'),
        { code: 'ERR_SUBSCRIPTION_UNKNOWN', resubscribe: true }
      );
    }
    return subscription;
  }

  pollSubscription(subscriptionId, options = {}) {
    const limit = options.limit ?? null;
    if (limit !== null && (!Number.isInteger(limit) || limit <= 0 || limit > 1000)) {
      throw Object.assign(new Error('limit must be an integer between 1 and 1000'), {
        code: 'ERR_INVALID_LIMIT'
      });
    }
    const watermark = options.watermark;
    if (watermark !== undefined && !Number.isSafeInteger(watermark)) {
      throw Object.assign(new Error('watermark must be a safe integer commit sequence'), {
        code: 'ERR_INVALID_WATERMARK'
      });
    }
    return this.#withLock(() => {
      this.#ensureOpen();
      const subscription = this.#getSubscription(subscriptionId);
      const from = watermark === undefined ? subscription.watermark : watermark;
      if (from < 0 || from > this.bufferSequence) {
        throw Object.assign(
          new Error(`Watermark ${from} is outside this process commit range 0..${this.bufferSequence}`),
          {
            code: 'ERR_SUBSCRIPTION_WATERMARK_INVALID',
            resubscribe: true,
            watermark: from,
            currentSequence: this.bufferSequence
          }
        );
      }
      // The first record this watermark would ask for is at from + 1. If even
      // that sequence has left the bounded window the gap cannot be bridged,
      // so refuse rather than silently skipping events.
      if (subscription.trimHorizon !== null && from + 1 < subscription.trimHorizon) {
        throw Object.assign(
          new Error(
            `Subscription retention window starts at ${subscription.trimHorizon}; ` +
              `watermark ${from} is too old, create a new subscription`
          ),
          {
            code: 'ERR_SUBSCRIPTION_WINDOW_EXPIRED',
            resubscribe: true,
            watermark: from,
            windowStart: subscription.trimHorizon,
            currentSequence: this.bufferSequence
          }
        );
      }

      const records = [];
      for (const [sequence, record] of subscription.changes) {
        if (sequence <= from) continue;
        records.push(record);
        if (limit !== null && records.length >= limit) break;
      }
      const lastSequence = records.length ? records.at(-1).sequence : from;
      const caughtUp = limit === null || lastSequence === this.bufferSequence || records.length < limit;
      return {
        subscriptionId: subscription.id,
        watermark: caughtUp ? this.bufferSequence : lastSequence,
        currentSequence: this.bufferSequence,
        changes: records,
        hasMore: !caughtUp,
        windowStart: subscription.trimHorizon
      };
    });
  }

  closeSubscription(subscriptionId) {
    return this.#withLock(() => {
      const existed = this.subscriptions.delete(subscriptionId);
      return { closed: true, existed };
    });
  }

  #liveCount() {
    const latest = new Map(this.base);
    for (const [id, doc] of this.buffer) latest.set(id, doc);
    let count = 0;
    for (const doc of latest.values()) if (!doc.deleted) count++;
    return count;
  }

  put(id, body, revision) {
    if (typeof id !== 'string' || id.length === 0) {
      throw Object.assign(new Error('Document id must be a non-empty string'), {
        code: 'ERR_INVALID_ID'
      });
    }
    if (typeof body !== 'string') {
      throw Object.assign(new Error('Document body must be a string'), {
        code: 'ERR_INVALID_BODY'
      });
    }
    return this.#withLock(() => {
      this.#ensureOpen();
      const current = this.#current(id);
      this.#assertRevision('put', id, revision, current);
      if ((!current || current.deleted) && this.#liveCount() >= this.maxDocuments) {
        throw new StoreLimitError(`Store already contains ${this.maxDocuments} live documents`);
      }
      const sequence = this.#nextSequence();
      const before = current ?? null;
      // The append fsyncs before returning. If it throws (disk error or an
      // injected fault) neither #applyRecord nor #publishCommit run, so the
      // commit sequence and every subscription watermark stay put and no
      // change record leaks for an unacknowledged write.
      this.wal.append({ type: 'put', sequence, id, revision, body });
      this.#applyRecord({ type: 'put', sequence, id, revision, body });
      const after = this.buffer.get(id);
      this.#publishCommit(sequence, id, before, after);
      return { id, revision, sequence };
    });
  }

  delete(id, revision) {
    if (typeof id !== 'string' || id.length === 0) {
      throw Object.assign(new Error('Document id must be a non-empty string'), {
        code: 'ERR_INVALID_ID'
      });
    }
    return this.#withLock(() => {
      this.#ensureOpen();
      const current = this.#current(id);
      this.#assertRevision('delete', id, revision, current);
      if ((!current || current.deleted) && this.#liveCount() >= this.maxDocuments) {
        throw new StoreLimitError(`Store already contains ${this.maxDocuments} document tombstones`);
      }
      const sequence = this.#nextSequence();
      const before = current ?? null;
      this.wal.append({ type: 'delete', sequence, id, revision });
      this.#applyRecord({ type: 'delete', sequence, id, revision });
      const after = this.buffer.get(id);
      this.#publishCommit(sequence, id, before, after);
      return { id, revision, sequence, deleted: true };
    });
  }

  #segmentFromDocs(segmentId, docs) {
    const sortedDocs = [...docs].sort((a, b) => a.id.localeCompare(b.id));
    const terms = new Map();

    for (const doc of sortedDocs) {
      if (doc.deleted) continue;
      for (const [term, positions] of doc.postings) {
        if (!terms.has(term)) terms.set(term, []);
        terms.get(term).push({ id: doc.id, positions });
      }
    }

    return {
      version: STORE_VERSION,
      id: segmentId,
      documents: sortedDocs.map((doc) => ({
        id: doc.id,
        revision: doc.revision,
        sequence: doc.sequence,
        deleted: doc.deleted,
        body: doc.deleted ? null : doc.body
      })),
      postings: Object.fromEntries(
        [...terms.entries()]
          .sort(([left], [right]) => left.localeCompare(right))
          .map(([term, hits]) => [
            term,
            hits.sort((left, right) => left.id.localeCompare(right.id)).map((hit) => ({
              id: hit.id,
              positions: hit.positions.map((position) => ({ ...position }))
            }))
          ])
      )
    };
  }

  #segmentFromBuffer(segmentId, buffer) {
    return this.#segmentFromDocs(segmentId, buffer.values());
  }

  #validateSegment(id, segment) {
    if (
      segment.version !== STORE_VERSION ||
      segment.id !== id ||
      !Array.isArray(segment.documents) ||
      typeof segment.postings !== 'object' ||
      segment.postings === null
    ) {
      throw Object.assign(new Error(`Invalid segment ${id}`), { code: 'ERR_CORRUPT_SEGMENT' });
    }

    const postingsByDoc = new Map(segment.documents.map((doc) => [doc.id, new Map()]));
    for (const [term, entries] of Object.entries(segment.postings)) {
      if (!Array.isArray(entries)) {
        throw Object.assign(new Error(`Invalid postings in segment ${id}`), {
          code: 'ERR_CORRUPT_SEGMENT'
        });
      }
      for (const entry of entries) {
        const target = postingsByDoc.get(entry.id);
        if (!target || !Array.isArray(entry.positions)) {
          throw Object.assign(new Error(`Invalid posting in segment ${id}`), {
            code: 'ERR_CORRUPT_SEGMENT'
          });
        }
        target.set(term, entry.positions);
      }
    }

    const docs = segment.documents.map((doc) => {
      if (
        typeof doc.id !== 'string' ||
        !Number.isSafeInteger(doc.sequence) ||
        typeof doc.deleted !== 'boolean'
      ) {
        throw Object.assign(new Error(`Invalid document in segment ${id}`), {
          code: 'ERR_CORRUPT_SEGMENT'
        });
      }
      return {
        id: doc.id,
        revision: doc.revision,
        sequence: doc.sequence,
        deleted: doc.deleted,
        body: doc.deleted ? null : doc.body,
        postings: doc.deleted ? new Map() : postingsByDoc.get(doc.id)
      };
    });
    return { ...segment, docs };
  }

  #writeSegment(segment, stages) {
    atomicWriteJson(this.#segmentPath(segment.id), segment, {
      afterFirstChunk: () => this.#fail(stages.write, { segmentId: segment.id }),
      afterRename: () => this.#fail(stages.rename, { segmentId: segment.id })
    });
  }

  #writeManifest(manifest, stages = {}) {
    atomicWriteJson(this.#manifestPath(), manifest, {
      afterFirstChunk: () => this.#fail(stages.write ?? 'manifestWrite', { manifest }),
      afterRename: () => this.#fail(stages.rename ?? 'manifestRename', { manifest })
    });
  }

  flush() {
    return this.#withLock(() => {
      this.#ensureOpen();
      if (this.buffer.size === 0) return null;

      const sequence = this.bufferSequence;
      const segmentId = this.manifest.nextSegmentId;
      const segment = this.#segmentFromBuffer(segmentId, this.buffer);
      const flushedBuffer = new Map(this.buffer);

      this.#writeSegment(segment, { write: 'segmentWrite', rename: 'segmentRename' });
      this.#fail('beforeFlushManifest', { segmentId });

      const nextManifest = {
        ...this.manifest,
        lastSequence: sequence,
        segmentIds: [...this.manifest.segmentIds, segmentId],
        nextSegmentId: segmentId + 1
      };
      this.#writeManifest(nextManifest, {
        write: 'flushManifestWrite',
        rename: 'flushManifestRename'
      });
      this.#fail('afterFlushManifestPublish', { segmentId });

      this.manifest = nextManifest;
      const frozen = deepFreeze(this.#validateSegment(segmentId, segment));
      this.segments.set(segmentId, frozen);
      for (const doc of frozen.docs) this.base.set(doc.id, doc);
      for (const id of flushedBuffer.keys()) this.buffer.delete(id);
      this.bufferSequence = sequence;

      this.wal.rewrite([]);
      // Segment reclamation is deliberately deferred. The caller can invoke
      // reclaimSegments() after old snapshots/cursors are released.
      return { segmentId, sequence, documents: flushedBuffer.size };
    });
  }

  merge(segmentIds) {
    return this.#withLock(() => {
      this.#ensureOpen();
      const requested = segmentIds ?? this.manifest.segmentIds;
      if (!Array.isArray(requested)) {
        throw Object.assign(new Error('segmentIds must be an array'), {
          code: 'ERR_INVALID_SEGMENT_IDS'
        });
      }
      const selected = requested
        .map((id) => Number(id))
        .filter((id) => Number.isSafeInteger(id) && id > 0)
        .sort((a, b) => a - b);
      if (selected.length !== requested.length || new Set(selected).size !== selected.length) {
        throw Object.assign(new Error('segmentIds must be unique positive integer segment ids'), {
          code: 'ERR_INVALID_SEGMENT_IDS'
        });
      }
      if (selected.length < 2) return null;
      for (const id of selected) {
        if (!this.segments.has(id)) {
          throw Object.assign(new Error(`Unknown segment ${id}`), { code: 'ERR_UNKNOWN_SEGMENT' });
        }
      }

      const latest = new Map();
      for (const id of selected) {
        for (const doc of this.segments.get(id).docs) {
          const current = latest.get(doc.id);
          if (!current || doc.sequence > current.sequence) latest.set(doc.id, doc);
        }
      }

      const mergedId = this.manifest.nextSegmentId;
      const merged = this.#segmentFromDocs(mergedId, latest.values());

      this.#writeSegment(merged, {
        write: 'mergeSegmentWrite',
        rename: 'mergeSegmentRename'
      });
      this.#fail('beforeMergeManifest', { mergedId, oldSegmentIds: selected });

      const selectedSet = new Set(selected);
      const retained = this.manifest.segmentIds.filter((id) => !selectedSet.has(id));
      const nextManifest = {
        ...this.manifest,
        segmentIds: [...retained, mergedId],
        nextSegmentId: mergedId + 1
      };
      this.#writeManifest(nextManifest, {
        write: 'mergeManifestWrite',
        rename: 'mergeManifestRename'
      });
      this.#fail('afterMergeManifestPublish', { mergedId, oldSegmentIds: selected });

      this.manifest = nextManifest;
      for (const id of selected) this.segments.delete(id);
      const frozenMerged = deepFreeze(this.#validateSegment(mergedId, merged));
      this.segments.set(mergedId, frozenMerged);

      this.base = new Map();
      for (const segment of this.segments.values()) {
        for (const doc of segment.docs) {
          const current = this.base.get(doc.id);
          if (!current || doc.sequence > current.sequence) this.base.set(doc.id, doc);
        }
      }

      return {
        segmentId: mergedId,
        replacedSegmentIds: selected,
        documents: merged.documents.length,
        liveDocuments: frozenMerged.docs.filter((doc) => !doc.deleted).length
      };
    });
  }

  #referencedSegmentIds() {
    const ids = new Set(this.manifest.segmentIds);
    for (const entry of this.snapshots.values()) {
      for (const segment of entry.snapshot.segments) ids.add(segment.id);
    }
    return ids;
  }

  reclaimSegments(stage = 'manual') {
    return this.#withLock(() => this.#reclaimUnreferencedSegments(stage));
  }

  async #reclaimUnreferencedSegments(stage) {
    this.#fail('beforeReclaim', { stage });
    const referenced = this.#referencedSegmentIds();
    for (const name of readdirSync(this.directory)) {
      const match = name.match(/^segment-(\d{8})\.json$/);
      if (!match) continue;
      const id = Number(match[1]);
      if (!referenced.has(id)) {
        unlinkSync(join(this.directory, name));
        this.segments.delete(id);
        this.#fail('afterReclaimDelete', { stage, segmentId: id });
      }
    }
    fsyncDirectory(this.directory);
  }

  getSnapshot(snapshotId) {
    const entry = this.snapshots.get(snapshotId);
    if (!entry) {
      throw Object.assign(new Error('Snapshot is closed'), { code: 'ERR_SNAPSHOT_CLOSED' });
    }
    return entry.snapshot;
  }

  snapshot() {
    return this.#withLock(() => {
      this.#ensureOpen();
      const snapshot = new IndexSnapshot(this, {
        id: `snap_${randomUUID()}`,
        sequence: this.bufferSequence,
        segments: this.manifest.segmentIds.map((id) => this.segments.get(id)),
        buffer: this.buffer
      });
      this.snapshots.set(snapshot.id, { snapshot, ownerRefs: 1, cursorRefs: 0 });
      return snapshot;
    });
  }

  #temporarySnapshot() {
    const snapshot = new IndexSnapshot(this, {
      id: `snap_${randomUUID()}`,
      sequence: this.bufferSequence,
      segments: this.manifest.segmentIds.map((id) => this.segments.get(id)),
      buffer: this.buffer
    });
    this.snapshots.set(snapshot.id, { snapshot, ownerRefs: 0, cursorRefs: 0 });
    return snapshot;
  }

  _issueCursor(snapshot, cursor) {
    const entry = this.snapshots.get(snapshot.id);
    if (!entry) throw Object.assign(new Error('Snapshot has been closed'), { code: 'ERR_SNAPSHOT_CLOSED' });
    entry.cursorRefs++;
    const tokenRecord = {
      v: 1,
      nonce: randomUUID(),
      snapshotId: snapshot.id,
      query: cursor.query,
      limit: cursor.limit,
      afterId: cursor.afterId
    };
    const token = Buffer.from(JSON.stringify(tokenRecord)).toString('base64url');
    this.cursors.set(token, {
      token,
      snapshot,
      cursor: tokenRecord,
      lastUsed: Date.now()
    });
    return token;
  }

  #decodeCursor(token) {
    try {
      const record = JSON.parse(Buffer.from(String(token), 'base64url').toString('utf8'));
      if (record.v !== 1 || !record.nonce || !record.snapshotId) throw new Error('bad cursor');
      return record;
    } catch (error) {
      throw Object.assign(new Error('Invalid pagination cursor'), { code: 'ERR_INVALID_CURSOR' });
    }
  }

  #materializePage(snapshot, visible, evidenceById, ids) {
    return ids.map((id) => {
      const doc = visible.get(id);
      return {
        id: doc.id,
        revision: doc.revision,
        sequence: doc.sequence,
        body: doc.body,
        source: doc.source,
        evidence: evidenceById.get(id) ?? null
      };
    });
  }

  query(query, options = {}) {
    this.#ensureOpen();
    const snapshot = this.#temporarySnapshot();
    let result;
    try {
      result = snapshot.query(query, options);
    } catch (error) {
      this.snapshots.delete(snapshot.id);
      throw error;
    }
    if (!result.nextCursor) this.snapshots.delete(snapshot.id);
    return result;
  }

  queryNext(token) {
    this.#ensureOpen();
    const record = this.#decodeCursor(token);
    const entry = this.cursors.get(token);
    if (!entry || entry.cursor.nonce !== record.nonce) {
      throw Object.assign(new Error('Cursor is closed or was already advanced'), {
        code: 'ERR_CURSOR_CLOSED'
      });
    }
    return this.#queryCursorEntry(entry);
  }

  _queryCursor(token, expectedSnapshot) {
    const record = this.#decodeCursor(token);
    const entry = this.cursors.get(token);
    if (!entry || entry.cursor.nonce !== record.nonce) {
      throw Object.assign(new Error('Cursor is closed or was already advanced'), {
        code: 'ERR_CURSOR_CLOSED'
      });
    }
    if (expectedSnapshot && entry.snapshot.id !== expectedSnapshot.id) {
      throw Object.assign(new Error('Cursor belongs to another snapshot'), {
        code: 'ERR_CURSOR_MISMATCH'
      });
    }
    return this.#queryCursorEntry(entry);
  }

  #queryCursorEntry(entry) {
    entry.lastUsed = Date.now();
    const snapshot = entry.snapshot;
    const { query, limit, afterId } = entry.cursor;
    const visible = snapshot.buildVisible();
    const { ids, evidenceById } = matchQuery(visible, query);
    const afterIndex = ids.indexOf(afterId);
    if (afterIndex < 0) {
      throw Object.assign(new Error('Cursor boundary no longer exists'), {
        code: 'ERR_INVALID_CURSOR'
      });
    }

    const pageIds = ids.slice(afterIndex + 1, afterIndex + 1 + limit);
    const result = {
      snapshotId: snapshot.id,
      sequence: snapshot.sequence,
      query: normalizeQuery(query),
      limit,
      total: ids.length,
      results: this.#materializePage(snapshot, visible, evidenceById, pageIds)
    };

    const { removedSnapshot } = this.#removeCursorEntry(entry);
    if (afterIndex + 1 + limit < ids.length) {
      result.nextCursor = this._issueCursor(snapshot, {
        query,
        limit,
        afterId: pageIds.at(-1)
      });
    } else if (removedSnapshot) {
      setImmediate(() => {
        this.#withLock(() => this.#reclaimUnreferencedSegments('cursorRelease')).catch(() => undefined);
      });
    }
    return result;
  }

  #removeCursorEntry(entry) {
    for (const [token, candidate] of this.cursors) {
      if (candidate === entry) {
        this.cursors.delete(token);
        const removed = this.#decrementSnapshot(entry.snapshot);
        return { token, removedSnapshot: removed ? entry.snapshot : null };
      }
    }
    return { token: null, removedSnapshot: null };
  }

  #decrementSnapshot(snapshot) {
    const entry = this.snapshots.get(snapshot.id);
    if (!entry) return false;
    entry.cursorRefs--;
    if (entry.ownerRefs + entry.cursorRefs <= 0) {
      this.snapshots.delete(snapshot.id);
      return true;
    }
    return false;
  }

  async _releaseSnapshot(snapshot) {
    const entry = this.snapshots.get(snapshot.id);
    if (!entry) return;
    entry.ownerRefs--;
    if (entry.ownerRefs + entry.cursorRefs > 0) return;
    this.snapshots.delete(snapshot.id);
    await this.#withLock(() => this.#reclaimUnreferencedSegments('snapshotRelease'));
  }

  async closeCursor(token) {
    const record = this.#decodeCursor(token);
    const entry = this.cursors.get(token);
    if (!entry || entry.cursor.nonce !== record.nonce) return;
    const { removedSnapshot } = this.#removeCursorEntry(entry);
    if (removedSnapshot) {
      await this.#withLock(() => this.#reclaimUnreferencedSegments('cursorRelease'));
    }
  }

  #expireCursors() {
    if (this.closed) return;
    const cutoff = Date.now() - this.cursorTtlMs;
    for (const [token, entry] of [...this.cursors]) {
      if (entry.lastUsed <= cutoff) {
        const { removedSnapshot } = this.#removeCursorEntry(entry);
        if (removedSnapshot) {
          setImmediate(() => {
            this.#withLock(() => this.#reclaimUnreferencedSegments('cursorExpiry')).catch(() => undefined);
          });
        }
      }
    }
  }

  getDocument(id) {
    this.#ensureOpen();
    const snapshot = this.#temporarySnapshot();
    try {
      return snapshot.getDocument(id);
    } finally {
      this.snapshots.delete(snapshot.id);
    }
  }

  list() {
    this.#ensureOpen();
    const snapshot = this.#temporarySnapshot();
    try {
      return snapshot.list();
    } finally {
      this.snapshots.delete(snapshot.id);
    }
  }

  stats() {
    this.#ensureOpen();
    return {
      sequence: this.bufferSequence,
      durableSequence: this.manifest.lastSequence,
      liveDocuments: this.#liveCount(),
      bufferedOperations: this.buffer.size,
      segments: this.manifest.segmentIds.slice(),
      activeSnapshots: this.snapshots.size,
      activeCursors: this.cursors.size,
      activeSubscriptions: this.subscriptions.size,
      maxDocuments: this.maxDocuments
    };
  }

  segmentFiles() {
    return readdirSync(this.directory)
      .filter((name) => /^segment-\d{8}\.json$/.test(name))
      .sort();
  }

  async close() {
    if (this.closed) return;
    this.closed = true;
    clearInterval(this.#reaper);
    await this.#withLock(() => {
      // Subscriptions are in-process only; a reopened directory never revives
      // them, so old watermarks must be refused by design.
      this.subscriptions.clear();
      this.wal.close();
    });
  }
}

export { IndexSnapshot, matchQuery, evaluateDocument, normalizeQuery };
