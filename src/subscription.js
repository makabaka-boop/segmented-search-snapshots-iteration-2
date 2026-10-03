import { randomUUID } from 'node:crypto';

/**
 * In-process query subscription.
 *
 * A subscription captures a query and the commit sequence (watermark) at
 * creation time. The store computes change records lazily from its bounded
 * commit feed when polled, so polling is idempotent: repeated polls with the
 * same watermark return identical records. Nothing is persisted; after a
 * process restart the store no longer knows the subscription id and polling
 * must fail so the client re-establishes a fresh snapshot rather than
 * silently missing events.
 */
export class QuerySubscription {
  #store;
  #closed = false;

  constructor(store, { plan, sequence, snapshot }) {
    this.id = `sub_${randomUUID()}`;
    this.plan = plan;
    this.sequence = sequence;
    this.snapshot = snapshot;
    this.#store = store;
  }

  poll(watermark) {
    if (this.#closed) {
      throw Object.assign(new Error('Subscription is closed'), {
        code: 'ERR_SUBSCRIPTION_CLOSED',
        subscriptionId: this.id
      });
    }
    return this.#store._pollSubscription(this, watermark);
  }

  close() {
    if (this.#closed) return Promise.resolve();
    this.#closed = true;
    return this.#store._closeSubscription(this);
  }
}
