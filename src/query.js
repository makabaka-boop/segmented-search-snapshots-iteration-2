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

export function matchQuery(visibleMap, rawQuery) {
  const plan = normalizeQuery(rawQuery);
  const evidenceById = new Map();
  if (plan.terms.length === 0 && plan.phrases.length === 0) {
    return {
      plan,
      ids: [...visibleMap.keys()].filter((id) => !visibleMap.get(id).deleted).sort(),
      evidenceById
    };
  }

  const candidate = chooseCandidateTerm(plan, visibleMap);
  const ids = [];

  for (const [id, doc] of visibleMap) {
    if (doc.deleted) continue;

    let candidateHit = false;
    if (candidate.phrase) {
      candidateHit = hasConsecutive(doc, candidate.phrase.terms).length > 0;
    } else {
      candidateHit = termPositions(doc, candidate.term).length > 0;
    }
    if (!candidateHit) continue;

    if (plan.terms.some((term) => termPositions(doc, term).length === 0)) continue;

    const phraseEvidence = [];
    let phraseFailed = false;
    for (const phrase of plan.phrases) {
      const starts = hasConsecutive(doc, phrase.terms);
      if (starts.length === 0) {
        phraseFailed = true;
        break;
      }
      phraseEvidence.push({
        phrase: phrase.text,
        terms: phrase.terms,
        starts
      });
    }
    if (phraseFailed) continue;

    const termEvidence = {};
    for (const term of plan.terms) {
      termEvidence[term] = termPositions(doc, term);
    }
    for (const phrase of plan.phrases) {
      for (const term of phrase.terms) {
        if (!(term in termEvidence)) termEvidence[term] = termPositions(doc, term);
      }
    }

    ids.push(id);
    evidenceById.set(id, { terms: termEvidence, phrases: phraseEvidence });
  }

  ids.sort();
  return { plan, ids, evidenceById };
}
