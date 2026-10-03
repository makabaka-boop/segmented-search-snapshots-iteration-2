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
    this.operations.push({ type: 'put', id, body, revision, sequence: seq });
    this.docsById.set(id, { id, body, revision, sequence: seq, deleted: false });
  }

  delete(id, revision) {
    const seq = ++this.sequence;
    this.operations.push({ type: 'delete', id, revision, sequence: seq });
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

// Deterministic LCG so randomized failures are reproducible.
export function lcg(seed) {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(1664525, state) + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}
