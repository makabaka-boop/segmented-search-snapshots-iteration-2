import { normalizeTerm, tokenize } from './tokenizer.js';

export function normalizeQuery(query = {}) {
  const terms = Array.from(new Set((query.terms ?? []).map((term) => normalizeTerm(term)))).sort();
  const phraseSet = new Set();
  const phrases = [];

  for (const text of query.phrases ?? []) {
    const phraseTerms = tokenize(text).map((token) => token.term);
    if (phraseTerms.length === 0) {
      throw Object.assign(new Error('Phrase must contain at least one ASCII word'), {
        code: 'ERR_INVALID_PHRASE'
      });
    }
    const key = phraseTerms.join(' ');
    if (!phraseSet.has(key)) {
      phraseSet.add(key);
      phrases.push({ text: String(text), terms: phraseTerms });
    }
  }
  phrases.sort((a, b) => a.terms.join(' ').localeCompare(b.terms.join(' ')));
  return { terms, phrases };
}

export function termPositions(visibleDoc, term) {
  const postings = visibleDoc.postings;
  if (postings instanceof Map) return postings.get(term) ?? [];
  return postings?.[term] ?? [];
}

function hasConsecutive(visibleDoc, phraseTerms) {
  const starts = [];
  const first = termPositions(visibleDoc, phraseTerms[0]);
  for (const { position: start } of first) {
    let matches = true;
    for (let offset = 1; offset < phraseTerms.length; offset++) {
      const expected = start + offset;
      const positions = termPositions(visibleDoc, phraseTerms[offset]);
      if (!positions.some((entry) => entry.position === expected)) {
        matches = false;
        break;
      }
    }
    if (matches) starts.push(start);
  }
  return starts;
}

/**
 * Evaluate one visible document against a normalized query plan. Returns
 * { matched, evidence }. Deleted documents never match. An empty plan (no
 * terms, no phrases) matches every live document with null evidence, matching
 * the evidence convention used by full-result query pages.
 *
 * Shared by snapshot queries and in-process subscriptions so storage commits,
 * query matches and subscription change records can never disagree.
 */
export function evaluateDocument(doc, plan) {
  if (doc == null || doc.deleted) return { matched: false, evidence: null };

  if (plan.terms.some((term) => termPositions(doc, term).length === 0)) {
    return { matched: false, evidence: null };
  }

  const phraseEvidence = [];
  for (const phrase of plan.phrases) {
    const starts = hasConsecutive(doc, phrase.terms);
    if (starts.length === 0) return { matched: false, evidence: null };
    phraseEvidence.push({
      phrase: phrase.text,
      terms: phrase.terms,
      starts
    });
  }

  if (plan.terms.length === 0 && plan.phrases.length === 0) {
    return { matched: true, evidence: null };
  }

  const termEvidence = {};
  for (const term of plan.terms) {
    termEvidence[term] = termPositions(doc, term);
  }
  for (const phrase of plan.phrases) {
    for (const term of phrase.terms) {
      if (!(term in termEvidence)) termEvidence[term] = termPositions(doc, term);
    }
  }

  return { matched: true, evidence: { terms: termEvidence, phrases: phraseEvidence } };
}

export function matchQuery(visibleMap, rawQuery) {
  const plan = normalizeQuery(rawQuery);
  const evidenceById = new Map();
  const ids = [];

  for (const [id, doc] of visibleMap) {
    const result = evaluateDocument(doc, plan);
    if (!result.matched) continue;
    ids.push(id);
    if (result.evidence) evidenceById.set(id, result.evidence);
  }

  ids.sort();
  return { plan, ids, evidenceById };
}
