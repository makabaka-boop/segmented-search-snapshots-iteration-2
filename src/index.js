export {
  DocumentStore,
  RevisionConflictError,
  StoreLimitError,
  SubscriptionExpiredError,
  IndexSnapshot,
  QuerySubscription,
  matchQuery,
  matchQueryPlan,
  matchDocumentPlan,
  normalizeQuery
} from './store.js';
export { tokenize, normalizeTerm, buildPostings } from './tokenizer.js';
