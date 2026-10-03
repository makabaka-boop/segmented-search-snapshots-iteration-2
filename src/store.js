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
import { matchDocumentPlan, matchQuery, matchQueryPlan, normalizeQuery } from './query.js';
import { QuerySubscription } from './subscription.js';

const MANIFEST_FILE = 'manifest.json';
const WAL_FILE = 'wal.log';
const STORE_VERSION = 1;

function clonePostings(postings) {
  const clone = new Map();
  const entries = postings instanceof Map
    ? postings.entries()
    : Object.entries(postings ?? {});
  for (const [term, positions] of entries) {
    clone.set(term, positions.map((position) => ({ ...position })));
  }
  return clone;
}

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

export class SubscriptionExpiredError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = 'SubscriptionExpiredError';
    this.code = 'ERR_SUBSCRIPTION_EXPIRED';
    Object.assign(this, details);
  }
}

export class DocumentStore {
  constructor(options = {}) {
    this.directory = options.directory;
    this.maxDocuments = options.maxDocuments ?? 1000;
    this.cursorTtlMs = options.cursorTtlMs ?? 10 * 60 * 1000;
    this.retentionCommits = options.retentionCommits ?? 1000;
    if (
      !Number.isSafeInteger(this.retentionCommits) ||
      this.retentionCommits < 1
    ) {
      throw Object.assign(new Error('retentionCommits must be a positive safe integer'), {
        code: 'ERR_INVALID_RETENTION'
      });
    }
    this.#faults = options.faultInjection ?? {};

    this.manifest = null;
    this.segments = new Map();
    this.base = new Map();
    this.buffer = new Map();
    this.bufferSequence = 0;
    this.wal = null;
    this.snapshots = new Map();
    this.cursors = new Map();
    // Bounded in-process feed of successful document commits (put/delete
    // only; flush, merge and reclamation never append). Each entry keeps the
    // pre/post document state so subscriptions can derive whether a query
    // match was ADDED, REMOVED or UPDATED after the fact.
    this.commitFeed = [];
    this.subscriptions = new Map();
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

    this.wal = new WriteAheadLog(this.#walPath(), { faultInjection: this.#faults });
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

  #captureDocState(doc) {
    if (!doc) return null;
    return {
      id: doc.id,
      revision: doc.revision,
      sequence: doc.sequence,
      deleted: doc.deleted,
      body: doc.deleted ? null : doc.body,
      postings: clonePostings(doc.postings)
    };
  }

  // Called only after a WAL record has been fsynced and applied, i.e. at the
  // same commit boundary the write returns. Operations that fail before this
  // point leave no trace in the feed, so they cannot leak into a
  // subscription.
  #appendCommit(entry) {
    this.commitFeed.push(entry);
    if (this.commitFeed.length > this.retentionCommits) {
      this.commitFeed.splice(0, this.commitFeed.length - this.retentionCommits);
    }
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
      const before = this.#captureDocState(current);
      this.wal.append({ type: 'put', sequence, id, revision, body });
      this.#applyRecord({ type: 'put', sequence, id, revision, body });
      const after = this.#captureDocState(this.buffer.get(id));
      this.#appendCommit({ sequence, type: 'put', id, revision, before, after });
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
      const before = this.#captureDocState(current);
      this.wal.append({ type: 'delete', sequence, id, revision });
      this.#applyRecord({ type: 'delete', sequence, id, revision });
      const after = this.#captureDocState(this.buffer.get(id));
      this.#appendCommit({ sequence, type: 'delete', id, revision, before, after });
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

  // Materialize the full current match set. Runs under the commit lock and
  // releases its temporary snapshot before returning, so the initial
  // snapshot is exactly the set visible at one commit boundary.
  #materializeCurrent(plan) {
    const snapshot = this.#temporarySnapshot();
    try {
      const visible = snapshot.buildVisible();
      const { ids, evidenceById } = matchQueryPlan(visible, plan);
      return {
        sequence: snapshot.sequence,
        total: ids.length,
        results: this.#materializePage(evidenceById, ids, visible)
      };
    } finally {
      this.snapshots.delete(snapshot.id);
    }
  }

  subscribe(query, options = {}) {
    return this.#withLock(() => {
      this.#ensureOpen();
      const plan = normalizeQuery(query);
      const limit = options.limit ?? this.maxDocuments;
      if (!Number.isInteger(limit) || limit <= 0) {
        throw Object.assign(new Error('limit must be a positive integer'), {
          code: 'ERR_INVALID_LIMIT'
        });
      }

      const current = this.#materializeCurrent(plan);
      if (current.total > limit) {
        throw Object.assign(
          new Error(`Initial snapshot has ${current.total} matches, limit is ${limit}; narrow the query`),
          { code: 'ERR_INITIAL_SNAPSHOT_TOO_LARGE', total: current.total, limit }
        );
      }

      const subscription = new QuerySubscription(this, {
        plan,
        sequence: current.sequence,
        snapshot: {
          sequence: current.sequence,
          total: current.total,
          results: current.results
        }
      });
      this.subscriptions.set(subscription.id, subscription);
      return subscription;
    });
  }

  getSubscription(subscriptionId) {
    const subscription = this.subscriptions.get(subscriptionId);
    if (!subscription) {
      throw Object.assign(
        new Error('Subscription does not exist in this process; re-establish it with a fresh snapshot'),
        {
          code: 'ERR_SUBSCRIPTION_NOT_FOUND',
          subscriptionId,
          reestablish: true
        }
      );
    }
    return subscription;
  }

  #subscriptionChange(subscription, commit) {
    const beforeMatch = commit.before ? matchDocumentPlan(commit.before, subscription.plan) : null;
    const afterMatch = commit.after ? matchDocumentPlan(commit.after, subscription.plan) : null;
    const wasMatched = beforeMatch !== null;
    const isMatched = afterMatch !== null;
    if (!wasMatched && !isMatched) return null;

    // A match-all query still transitions, but exposes no term/phrase
    // evidence, matching the query-page convention of evidence: null.
    const emptyQuery =
      subscription.plan.terms.length === 0 && subscription.plan.phrases.length === 0;

    return {
      sequence: commit.sequence,
      type: isMatched && wasMatched ? 'UPDATED' : isMatched ? 'ADDED' : 'REMOVED',
      id: commit.id,
      // Revision after this commit (for a REMOVED this is the tombstone
      // revision) and the revision that preceded it.
      revision: commit.after ? commit.after.revision : null,
      beforeRevision: commit.before ? commit.before.revision : null,
      evidence: isMatched && !emptyQuery ? afterMatch : null,
      // Pre-removal evidence lets a client re-verify why the document left.
      beforeEvidence: wasMatched && !emptyQuery ? beforeMatch : null
    };
  }

  _pollSubscription(subscription, watermark) {
    return this.#withLock(() => {
      this.#ensureOpen();
      if (this.subscriptions.get(subscription.id) !== subscription) {
        throw Object.assign(new Error('Subscription is closed'), {
          code: 'ERR_SUBSCRIPTION_CLOSED',
          subscriptionId: subscription.id
        });
      }
      if (
        !Number.isSafeInteger(watermark) ||
        watermark < subscription.sequence ||
        watermark > this.bufferSequence
      ) {
        throw Object.assign(
          new Error(
            `Watermark ${watermark} is outside [${subscription.sequence}, ${this.bufferSequence}] for this subscription`
          ),
          {
            code: 'ERR_INVALID_WATERMARK',
            subscriptionId: subscription.id,
            watermark,
            startSequence: subscription.sequence,
            currentSequence: this.bufferSequence
          }
        );
      }

      // Oldest commit the feed can still serve. When the feed is empty every
      // subsequent commit is still derivable (there are none), so the
      // horizon sits just after the current sequence.
      const horizon = this.commitFeed.length
        ? this.commitFeed[0].sequence
        : this.bufferSequence + 1;
      if (watermark + 1 < horizon) {
        throw new SubscriptionExpiredError(
          `Subscription history from sequence ${watermark + 1} has been discarded; re-establish the subscription for a fresh snapshot`,
          {
            subscriptionId: subscription.id,
            watermark,
            oldestSequence: horizon - 1,
            currentSequence: this.bufferSequence,
            reestablish: true
          }
        );
      }

      const changes = [];
      for (const commit of this.commitFeed) {
        if (commit.sequence <= watermark) continue;
        const change = this.#subscriptionChange(subscription, commit);
        if (change) changes.push(change);
      }
      // Feed entries are appended in commit order and kept sorted, so the
      // change list inherits commit ordering with at most one record per
      // successful put/delete.
      return {
        subscriptionId: subscription.id,
        query: subscription.plan,
        watermark,
        currentSequence: this.bufferSequence,
        changes
      };
    });
  }

  _closeSubscription(subscription) {
    return this.#withLock(() => {
      this.subscriptions.delete(subscription.id);
    });
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

  #materializePage(evidenceById, ids, visibleById) {
    return ids.map((id) => {
      const doc = visibleById.get(id);
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
      results: this.#materializePage(evidenceById, pageIds, visible)
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
      retainedCommits: this.commitFeed.length,
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
      this.wal.close();
    });
  }
}

export { IndexSnapshot, matchQuery, matchQueryPlan, matchDocumentPlan, normalizeQuery };
export { QuerySubscription } from './subscription.js';
