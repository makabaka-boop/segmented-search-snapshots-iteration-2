import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { normalizeQuery, tokenize } from '../src/index.js';

export function makeTempDir(label) {
  return mkdtempSync(join(tmpdir(), `doc-store-${label}-`));
}

export function cleanup(paths) {
  for (const path of paths) rmSync(path, { recursive: true, force: true });
}

export class ReferenceModel {
  constructor() {
    this.operations = [];
    this.docsById = new Map();
    this.sequence = 0;
  }

  put(id, body, revision) {
    const seq = ++this.sequence;
    this.operations.push({ id, body, revision, sequence: seq, deleted: false });
    this.docsById.set(id, { id, body, revision, sequence: seq, deleted: false });
  }

  delete(id, revision) {
    const seq = ++this.sequence;
    this.operations.push({
      id,
      body: null,
      revision,
      sequence: seq,
      deleted: true
    });
    this.docsById.set(id, { id, body: null, revision, sequence: seq, deleted: true });
  }

  atSequence(sequence) {
    const docs = new Map();
    for (const op of this.operations) {
      if (op.sequence > sequence) break;
      docs.set(op.id, { ...op });
    }
    return docs;
  }
  query(rawQuery, docsById = this.docsById) {
    const plan = normalizeQuery(rawQuery);
    const matched = new Map();

    for (const doc of [...docsById.values()].sort((a, b) => a.id.localeCompare(b.id))) {
      if (doc.deleted) continue;
      const tokens = tokenize(doc.body);
      const byTerm = new Map();
      for (const token of tokens) {
        if (!byTerm.has(token.term)) byTerm.set(token.term, []);
        byTerm.get(token.term).push({
          position: token.position,
          start: token.start,
          end: token.end
        });
      }

      const terms = {};
      let ok = true;
      for (const term of plan.terms) {
        const hits = byTerm.get(term);
        if (!hits) {
          ok = false;
          break;
        }
        terms[term] = hits;
      }
      if (!ok) continue;

      const phrases = [];
      for (const phrase of plan.phrases) {
        const starts = [];
        const first = byTerm.get(phrase.terms[0]) ?? [];
        for (const hit of first) {
          let found = true;
          for (let i = 1; i < phrase.terms.length; i++) {
            const wanted = hit.position + i;
            if (!(byTerm.get(phrase.terms[i]) ?? []).some((candidate) => candidate.position === wanted)) {
              found = false;
              break;
            }
          }
          if (found) starts.push(hit.position);
        }
        if (!starts.length) {
          ok = false;
          break;
        }
        phrases.push({ phrase: phrase.text, terms: phrase.terms, starts });
        for (const term of phrase.terms) terms[term] = byTerm.get(term) ?? [];
      }
      if (!ok) continue;

      matched.set(doc.id, {
        id: doc.id,
        revision: doc.revision,
        sequence: doc.sequence,
        body: doc.body,
        evidence: plan.terms.length || plan.phrases.length ? { terms, phrases } : null
      });
    }
    return matched;
  }
}

export function compareQueryResults(t, actualRows, expectedMap) {
  const actualMap = new Map(
    actualRows.map((row) => [
      row.id,
      {
        id: row.id,
        revision: row.revision,
        sequence: row.sequence,
        body: row.body,
        evidence: row.evidence
      }
    ])
  );

  t.equal(actualMap.size, expectedMap.size, 'matching document count');
  t.deepEqual([...actualMap.keys()].sort(), [...expectedMap.keys()].sort());
  for (const [id, expected] of expectedMap) {
    t.deepEqual(actualMap.get(id), expected, `document ${id} matches direct scan`);
  }
}

// Evidence for one document under a normalized query plan, computed by a
// direct tokenization of the raw body rather than the store's postings.
export function referenceEvidence(doc, plan) {
  if (!doc || doc.deleted) return null;
  const tokens = tokenize(doc.body);
  const byTerm = new Map();
  for (const token of tokens) {
    if (!byTerm.has(token.term)) byTerm.set(token.term, []);
    byTerm.get(token.term).push({
      position: token.position,
      start: token.start,
      end: token.end
    });
  }

  if (plan.terms.length === 0 && plan.phrases.length === 0) {
    return { terms: {}, phrases: [] };
  }

  for (const term of plan.terms) {
    if (!byTerm.has(term)) return null;
  }
  const phrases = [];
  const terms = {};
  for (const phrase of plan.phrases) {
    const starts = [];
    const first = byTerm.get(phrase.terms[0]) ?? [];
    for (const hit of first) {
      let found = true;
      for (let i = 1; i < phrase.terms.length; i++) {
        const wanted = hit.position + i;
        if (!(byTerm.get(phrase.terms[i]) ?? []).some((candidate) => candidate.position === wanted)) {
          found = false;
          break;
        }
      }
      if (found) starts.push(hit.position);
    }
    if (!starts.length) return null;
    phrases.push({ phrase: phrase.text, terms: phrase.terms, starts });
  }
  for (const term of plan.terms) terms[term] = byTerm.get(term);
  for (const phrase of plan.phrases) {
    for (const term of phrase.terms) terms[term] = byTerm.get(term) ?? [];
  }
  return { terms, phrases };
}

// Direct-scan oracle for the subscription change stream: replay the
// operation log between two watermarks and derive the single change each
// commit produces for the query, ignoring flush/merge entirely.
export function expectedChanges(model, rawQuery, fromSequence, toSequence = model.sequence) {
  const plan = normalizeQuery(rawQuery);
  const emptyQuery = plan.terms.length === 0 && plan.phrases.length === 0;

  // Replay the operation log up to the starting watermark so `before` for
  // the first derived change is exactly what that commit replaced.
  const state = new Map();
  for (const op of model.operations) {
    if (op.sequence > fromSequence) break;
    state.set(op.id, { ...op });
  }

  const changes = [];
  for (const op of model.operations) {
    if (op.sequence <= fromSequence) continue;
    if (op.sequence > toSequence) break;
    const before = state.get(op.id) ?? null;
    const after = { ...op };
    state.set(op.id, after);
    const beforeEvidence = referenceEvidence(before, plan);
    const afterEvidence = referenceEvidence(after, plan);
    const wasMatched = beforeEvidence !== null;
    const isMatched = afterEvidence !== null;
    if (!wasMatched && !isMatched) continue;
    changes.push({
      sequence: op.sequence,
      type: isMatched && wasMatched ? 'UPDATED' : isMatched ? 'ADDED' : 'REMOVED',
      id: op.id,
      revision: op.revision,
      beforeRevision: before ? before.revision : null,
      evidence: isMatched && !emptyQuery ? afterEvidence : null,
      beforeEvidence: wasMatched && !emptyQuery ? beforeEvidence : null
    });
  }
  return changes;
}

// Deterministic LCG so randomized failures are reproducible.
export function lcg(seed) {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(1664525, state) + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}
