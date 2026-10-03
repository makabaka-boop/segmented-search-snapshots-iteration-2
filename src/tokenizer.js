const ASCII_WORD_RE = /[a-zA-Z0-9]+/g;

/**
 * Tokenize ASCII words.  Positions are token ordinal numbers; offset fields
 * locate the token in the original body and can be returned as query evidence.
 */
export function tokenize(text) {
  if (text == null) return [];
  const source = String(text);
  const tokens = [];
  for (const match of source.matchAll(ASCII_WORD_RE)) {
    tokens.push({
      term: match[0].toLowerCase(),
      position: tokens.length,
      start: match.index,
      end: match.index + match[0].length
    });
  }
  return tokens;
}

export function buildPostings(text) {
  const postings = new Map();
  for (const token of tokenize(text)) {
    let list = postings.get(token.term);
    if (!list) {
      list = [];
      postings.set(token.term, list);
    }
    list.push({
      position: token.position,
      start: token.start,
      end: token.end
    });
  }
  return postings;
}

export function normalizeTerm(value) {
  const text = String(value);
  const tokens = tokenize(text);
  if (tokens.length !== 1 || tokens[0].start !== 0 || tokens[0].end !== text.length) {
    throw Object.assign(new Error(`Term must contain exactly one ASCII word: ${text}`), {
      code: 'ERR_INVALID_TERM'
    });
  }
  return tokens[0].term;
}
