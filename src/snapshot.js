import { matchQuery, normalizeQuery } from './query.js';

function cloneBuffer(buffer) {
  const cloned = new Map();
  for (const [id, doc] of buffer) {
    cloned.set(id, {
      id: doc.id,
      revision: doc.revision,
      sequence: doc.sequence,
      deleted: doc.deleted,
      body: doc.body,
      postings: clonePostings(doc.postings)
    });
  }
  return cloned;
}

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

export class IndexSnapshot {
  #store;
  #released = false;

  constructor(store, { id, sequence, segments, buffer }) {
    this.id = id;
    this.sequence = sequence;
    this.segments = segments.map((segment) => ({
      id: segment.id,
      docs: segment.docs.map((doc) => ({
        id: doc.id,
        revision: doc.revision,
        sequence: doc.sequence,
        deleted: doc.deleted,
        body: doc.body ?? null,
        postings: clonePostings(doc.postings)
      }))
    }));
    this.buffer = cloneBuffer(buffer);
    this.#store = store;
  }

  buildVisible() {
    const versions = new Map();

    for (const segment of this.segments) {
      for (const doc of segment.docs) {
        const current = versions.get(doc.id);
        if (!current || doc.sequence > current.sequence) {
          versions.set(doc.id, {
            id: doc.id,
            revision: doc.revision,
            sequence: doc.sequence,
            deleted: doc.deleted,
            body: doc.body ?? null,
            postings: doc.postings ?? {},
            source: { type: 'segment', segmentId: segment.id }
          });
        }
      }
    }

    for (const doc of this.buffer.values()) {
      const current = versions.get(doc.id);
      if (!current || doc.sequence > current.sequence) {
        versions.set(doc.id, { ...doc, source: { type: 'buffer' } });
      }
    }

    return versions;
  }

  getDocument(id) {
    const visible = this.buildVisible();
    const doc = visible.get(id);
    return doc && !doc.deleted ? doc : null;
  }

  list() {
    const visible = this.buildVisible();
    return [...visible.values()]
      .filter((doc) => !doc.deleted)
      .sort((a, b) => a.id.localeCompare(b.id))
      .map(({ postings, ...doc }) => doc);
  }

  query(query, options = {}) {
    const limit = options.limit ?? 50;
    if (!Number.isInteger(limit) || limit <= 0 || limit > 1000) {
      throw Object.assign(new Error('limit must be an integer between 1 and 1000'), {
        code: 'ERR_INVALID_LIMIT'
      });
    }

    const normalized = normalizeQuery(query);
    const visible = this.buildVisible();
    const matched = matchQuery(visible, query);
    const page = matched.ids.slice(0, limit);
    const result = {
      snapshotId: this.id,
      sequence: this.sequence,
      query: normalized,
      limit,
      total: matched.ids.length,
      results: this.#materialize(visible, matched.evidenceById, page)
    };
    if (matched.ids.length > limit) {
      result.nextCursor = this.#store._issueCursor(this, {
        query,
        limit,
        afterId: page.at(-1)
      });
    }
    return result;
  }

  queryPage(cursor) {
    return this.#store._queryCursor(cursor, this);
  }

  #materialize(visible, evidenceById, ids) {
    return ids.map((id) => {
      const doc = visible.get(id);
      const evidence = evidenceById.get(id);
      return {
        id: doc.id,
        revision: doc.revision,
        sequence: doc.sequence,
        body: doc.body,
        source: doc.source,
        evidence: evidence ?? null
      };
    });
  }

  async close() {
    if (this.#released) return;
    this.#released = true;
    await this.#store._releaseSnapshot(this);
  }
}
