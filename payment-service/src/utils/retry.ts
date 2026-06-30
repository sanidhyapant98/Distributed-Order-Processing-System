/**
 * Bounded retry helper.
 *
 * Runs `fn`. If it throws, retries up to `retries` more times with
 * exponential backoff (+ jitter) before giving up and re-throwing the
 * most recent error. This bounds how long / how many times we'll keep
 * hammering a flaky dependency — a transient blip gets a real chance to
 * recover, but we never retry forever.
 */

export interface RetryOptions {
  /** How many extra attempts to make after the first try. Default: 3. */
  retries?: number;
  /** Delay before the first retry, in ms. Doubles each subsequent retry. Default: 200. */
  baseDelayMs?: number;
  /** Upper bound on the backoff delay, in ms, regardless of attempt count. Default: 5000. */
  maxDelayMs?: number;
  /** Called right before each retry's delay. Useful for logging. */
  onRetry?: (error: unknown, attempt: number, delayMs: number) => void;
}

const DEFAULTS = {
  retries: 3,
  baseDelayMs: 200,
  maxDelayMs: 5000,
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function withRetry<T>(
  fn: () => Promise<T>,
  options: RetryOptions = {}
): Promise<T> {
  const retries = options.retries ?? DEFAULTS.retries;
  const baseDelayMs = options.baseDelayMs ?? DEFAULTS.baseDelayMs;
  const maxDelayMs = options.maxDelayMs ?? DEFAULTS.maxDelayMs;

  let attempt = 0;

  for (;;) {
    try {
      return await fn();
    } catch (error) {
      attempt += 1;

      if (attempt > retries) {
        // The bound has been reached — stop retrying and let the caller
        // decide how to handle the permanent failure.
        throw error;
      }

      const exponential = Math.min(baseDelayMs * 2 ** (attempt - 1), maxDelayMs);
      // +/-15% jitter so multiple failing operations don't all retry in lockstep.
      const jitterFactor = 0.85 + Math.random() * 0.3;
      const delayMs = Math.round(exponential * jitterFactor);

      options.onRetry?.(error, attempt, delayMs);

      await sleep(delayMs);
    }
  }
}