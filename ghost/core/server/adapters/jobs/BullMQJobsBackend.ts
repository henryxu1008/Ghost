import errors from '@tryghost/errors';
import { z } from 'zod';
import { Queue, Worker } from 'bullmq';
import type { Job as BullJob, RedisOptions } from 'bullmq';
import { JobsBackendBase } from '@tryghost/adapter-base-jobs';
import type {
  JobProcessor,
  JobEnvelope,
  DispatchEnvelope,
  JobsStartOptions,
  RecurringSchedule,
  JobsShutdownOptions,
} from '@tryghost/adapter-base-jobs';

const logging = require('@tryghost/logging');

// Durable jobs on Redis, selected with adapters:jobs:active = "BullMQJobsBackend"
// and configured under adapters:jobs:BullMQJobsBackend (see defaults.json).
//
// - `connection` is passed through to ioredis. It is deliberately never
//   borrowed from the cache Redis config: the queue needs AOF persistence and
//   maxmemory-policy noeviction, while the cache tolerates eviction.
// - `prefix` namespaces every key. Sites that share one Redis must set distinct
//   prefixes, or they will consume each other's jobs.
// - `concurrency` is per container, per lane. There is no global concurrency.

const DEFAULT_SHUTDOWN_TIMEOUT_MS = 10000;
// A producer waits for its first connection, which never comes when Redis is
// down, so enqueue gives up after this long instead of hanging its caller.
const ENQUEUE_TIMEOUT_MS = 2000;
// Envelopes with no routing share this lane; routing to "default" by name is
// the same lane, not a collision.
const DEFAULT_QUEUE = 'default';

const connectionSchema = z
  .object({
    host: z.string().min(1).optional(),
    port: z.number().int().positive().optional(),
    username: z.string().nullish(),
    password: z.string().nullish(),
    db: z.number().int().nonnegative().optional(),
    tls: z.union([z.boolean(), z.record(z.string(), z.unknown())]).optional(),
  })
  .transform(({ username, password, tls, ...rest }): RedisOptions => {
    const options: RedisOptions = { ...rest };
    if (username !== null && username !== undefined) {
      options.username = username;
    }
    if (password !== null && password !== undefined) {
      options.password = password;
    }
    if (tls === true) {
      options.tls = {};
    } else if (tls) {
      options.tls = tls;
    }
    return options;
  });

const schema = z.object({
  connection: connectionSchema.prefault({}),
  prefix: z.string().min(1).default('ghost'),
  concurrency: z.number().int().positive().default(3),
  lockDuration: z.number().int().positive().default(30000),
  stalledInterval: z.number().int().positive().default(30000),
  maxStalledCount: z.number().int().nonnegative().default(1),
});

type ResolvedConfig = z.output<typeof schema>;

function parseConfig(config: unknown): ResolvedConfig {
  try {
    return schema.parse(config);
  } catch (err) {
    if (!(err instanceof z.ZodError)) {
      throw err;
    }
    const [issue] = err.issues;
    throw new errors.IncorrectUsageError({
      message: `Invalid jobs backend config at "${issue!.path.join('.')}": ${issue!.message}.`,
    });
  }
}

interface Lane {
  queue: Queue;
  worker: Worker;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

// Work only flows between start() and shutdown(): boot starts the service
// before the web app is mounted or any recurring job is scheduled, so an
// enqueue or recurring registration before start() is a boot-ordering bug and
// throws, while an enqueue after shutdown() is a benign shutdown race and is
// dropped.
export default class BullMQJobsBackend extends JobsBackendBase {
  private _processor: JobProcessor | null;
  private _stopped: boolean;
  private _prefix: string;
  private _connection: RedisOptions;
  private _defaultConcurrency: number;
  private _lockDuration: number;
  private _stalledInterval: number;
  private _maxStalledCount: number;
  private _lanes: Map<string, Lane>;
  // Deliveries running now. Shutdown waits on them, within its time budget.
  private _inflight: Set<Promise<void>>;

  constructor(config: unknown = {}) {
    super();
    const resolved = parseConfig(config);
    this._processor = null;
    this._stopped = false;
    this._prefix = resolved.prefix;
    this._connection = resolved.connection;
    this._defaultConcurrency = resolved.concurrency;
    this._lockDuration = resolved.lockDuration;
    this._stalledInterval = resolved.stalledInterval;
    this._maxStalledCount = resolved.maxStalledCount;
    this._lanes = new Map();
    this._inflight = new Set();
  }

  // Producers fail fast when Redis is down, so enqueue rejects promptly and the
  // outbox relay (or the direct path) decides what happens to the job.
  private _producerConnection(): RedisOptions {
    return {
      ...this._connection,
      enableOfflineQueue: false,
      maxRetriesPerRequest: 1,
      retryStrategy: (times) => Math.min(times * 200, 2000),
    };
  }

  // Workers wait for Redis forever and pick up again when it returns.
  private _workerConnection(): RedisOptions {
    return { ...this._connection, maxRetriesPerRequest: null };
  }

  private _makeLane(name: string, concurrency: number): Lane {
    const queue = new Queue(name, {
      connection: this._producerConnection(),
      prefix: this._prefix,
    });
    const worker = new Worker(name, (bullJob) => this._deliver(bullJob), {
      connection: this._workerConnection(),
      prefix: this._prefix,
      concurrency,
      lockDuration: this._lockDuration,
      stalledInterval: this._stalledInterval,
      maxStalledCount: this._maxStalledCount,
    });
    // Without listeners a Redis connection error would be an unhandled 'error' event
    queue.on('error', (err) => logging.error(`Jobs queue "${name}" error`, err));
    worker.on('error', (err) => logging.error(`Jobs worker "${name}" error`, err));
    return { queue, worker };
  }

  start({ processor, queues }: JobsStartOptions): void {
    const declared = new Map<string, number>();
    for (const [name, { concurrency }] of Object.entries(queues ?? {})) {
      // A declaration the backend cannot satisfy must fail loudly here, never
      // silently fall back (see the jobs-base README).
      if (concurrency !== undefined && (!Number.isInteger(concurrency) || concurrency < 1)) {
        throw new errors.IncorrectUsageError({
          message: `Invalid concurrency for declared queue "${name}": ${JSON.stringify(concurrency)}. Expected a positive integer.`,
        });
      }
      declared.set(name, concurrency ?? this._defaultConcurrency);
    }

    // State only changes once every declaration is valid, so a rejected
    // start() never leaves a partially started backend that accepts work.
    this._processor = processor;
    this._stopped = false;
    for (const [name, concurrency] of declared) {
      this._lanes.set(name, this._makeLane(name, concurrency));
    }
    if (!this._lanes.has(DEFAULT_QUEUE)) {
      this._lanes.set(DEFAULT_QUEUE, this._makeLane(DEFAULT_QUEUE, this._defaultConcurrency));
    }
  }

  private _lane(name: string): Lane {
    let lane = this._lanes.get(name);
    if (!lane) {
      // Lanes are created lazily for any queue name no handler declared, at the
      // default concurrency.
      lane = this._makeLane(name, this._defaultConcurrency);
      this._lanes.set(name, lane);
    }
    return lane;
  }

  async enqueue(id: string, envelope: DispatchEnvelope): Promise<void> {
    if (this._stopped) {
      return;
    }
    const { job, routing } = envelope.payload;
    if (!this._processor) {
      throw new errors.IncorrectUsageError({
        message: `Cannot enqueue job "${job.type}" before the jobs backend is started.`,
      });
    }
    const lane = this._lane(routing?.queue ?? DEFAULT_QUEUE);
    // The dispatch id is the job id: the outbox relay resubmits the same id, so
    // BullMQ dedupes for as long as the job key exists.
    await this._withTimeout(
      lane.queue.add(job.type, job, { jobId: id }),
      `Timed out enqueueing job "${job.type}": Redis is unavailable.`,
    );
  }

  private async _withTimeout<T>(promise: Promise<T>, message: string): Promise<T> {
    let timer: NodeJS.Timeout | undefined;
    try {
      return await Promise.race([
        promise,
        new Promise<never>((_resolve, reject) => {
          timer = setTimeout(() => {
            reject(new errors.InternalServerError({ message }));
          }, ENQUEUE_TIMEOUT_MS);
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  }

  private async _deliver(bullJob: BullJob<JobEnvelope>): Promise<void> {
    const delivery = this._processor!(bullJob.data);
    const tracked = delivery.then(
      () => undefined,
      () => undefined,
    );
    this._inflight.add(tracked);
    try {
      await delivery;
    } catch (err) {
      logging.error(`Job "${bullJob.data.type}" delivery failed`, err);
      // Rethrow so BullMQ records the failed attempt
      throw err;
    } finally {
      this._inflight.delete(tracked);
    }
  }

  private async _awaitInflight(): Promise<void> {
    while (this._inflight.size > 0) {
      await Promise.all([...this._inflight]);
    }
  }

  scheduleRecurring(envelope: DispatchEnvelope, _schedule: RecurringSchedule): never {
    if (this._stopped) {
      return undefined as never;
    }
    const { job } = envelope.payload;
    if (!this._processor) {
      throw new errors.IncorrectUsageError({
        message: `Cannot schedule recurring job "${job.type}" before the jobs backend is started.`,
      });
    }
    throw new errors.IncorrectUsageError({
      message: 'BullMQJobsBackend does not support recurring schedules yet.',
    });
  }

  async shutdown(options: JobsShutdownOptions = {}): Promise<void> {
    const timeoutMs = options.timeoutMs ?? DEFAULT_SHUTDOWN_TIMEOUT_MS;
    this._stopped = true;

    const lanes = [...this._lanes.values()];
    this._lanes = new Map();

    // Stop fetching first, then let running deliveries finish. Jobs still
    // waiting stay in Redis and run after the next start.
    const drained = await Promise.race([
      (async () => {
        await Promise.all(lanes.map(({ worker }) => worker.pause(true)));
        await this._awaitInflight();
        return true;
      })().catch(() => false),
      delay(timeoutMs).then(() => false),
    ]);
    // A hung handler's job stays locked; BullMQ's stalled-job handling recovers
    // it once the lock expires, on the next container or boot. Closing with
    // force skips waiting for it.
    await Promise.all(lanes.map(({ worker }) => worker.close(!drained)));
    await Promise.all(lanes.map(({ queue }) => queue.close()));
    this._inflight = new Set();
    this._processor = null;
  }
}
