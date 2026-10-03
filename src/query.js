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

function chooseCandidateTerm(plan, visible) {
  const requiredTerms = [
    ...plan.terms.map((term) => ({ term, phrase: null })),
    ...plan.phrases.map((phrase) => ({ term: phrase.terms[0], phrase }))
  ];

  let best = null;
  let bestCount = Infinity;
  for (const item of requiredTerms) {
    let count = 0;
    for (const doc of visible.values()) {
      if (termPositions(doc, item.term).length > 0) count++;
    }
    if (count < bestCount) {
      best = item;
      bestCount = count;
    }
  }
  return best;
}

/**
 * Evaluate one visible document against an already normalized query plan.
 * Returns the same evidence shape as a full query, or null when the document
 * is deleted or fails to match. Subscription change records reuse this so a
 * single-document re-check produces exactly the evidence a query page would.
 */
export function matchDocumentPlan(doc, plan) {
  if (!doc || doc.deleted) return null;

  if (plan.terms.length === 0 && plan.phrases.length === 0) {
    return { terms: {}, phrases: [] };
  }

  if (plan.terms.some((term) => termPositions(doc, term).length === 0)) {
    return null;
  }

  const phraseEvidence = [];
  for (const phrase of plan.phrases) {
    const starts = hasConsecutive(doc, phrase.terms);
    if (starts.length === 0) return null;
    phraseEvidence.push({
      phrase: phrase.text,
      terms: phrase.terms,
      starts
    });
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

  return { terms: termEvidence, phrases: phraseEvidence };
}

export function matchQueryPlan(visibleMap, plan) {
  const evidenceById = new Map();
  if (plan.terms.length === 0 && plan.phrases.length === 0) {
    return {
      plan,
      ids: [...visibleMap.keys()].filter((id) => !visibleMap.get(id).deleted).sort(),
      evidenceById
    };
  }

  const ids = [];
  for (const [id, doc] of visibleMap) {
    const evidence = matchDocumentPlan(doc, plan);
    if (evidence) {
      ids.push(id);
      evidenceById.set(id, evidence);
    }
  }

  ids.sort();
  return { plan, ids, evidenceById };
}

export function matchQuery(visibleMap, rawQuery) {
  const plan = normalizeQuery(rawQuery);
  return matchQueryPlan(visibleMap, plan);
}
