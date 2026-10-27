import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { Queue } from 'bullmq';
import type { JobEnvelope, DispatchEnvelope } from '@tryghost/adapter-base-jobs';
import { runJobsBackendContractTests } from '@tryghost/adapter-base-jobs/contract-test-suite';
import BullMQJobsBackend from '../../../../core/server/adapters/jobs/BullMQJobsBackend';

const sinon = require('sinon');
const logging = require('@tryghost/logging');

const redisUp = process.env.GHOST_TEST_REDIS_AVAILABLE === '1';
const connection = { host: '127.0.0.1', port: 6379 };
const fileId = crypto.randomBytes(4).toString('hex');
const prefixes: string[] = [];

// Short lock and stall timings so recovery and shutdown fit the test timeout
const fastConfig = {
  lockDuration: 1000,
  stalledInterval: 500,
  maxStalledCount: 1,
};

function newPrefix(): string {
  const prefix = `test-jobs-${fileId}-${prefixes.length}`;
  prefixes.push(prefix);
  return prefix;
}

function makeBackend(config: Record<string, unknown> = {}): BullMQJobsBackend {
  return new BullMQJobsBackend({
    connection,
    prefix: newPrefix(),
    ...fastConfig,
    ...config,
  });
}

function dispatch(job: JobEnvelope, queue?: string): DispatchEnvelope {
  return {
    version: 1,
    payload: queue === undefined ? { job } : { job, routing: { queue } },
  };
}

function inspect(prefix: string, name = 'default'): Queue {
  return new Queue(name, { connection, prefix });
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function waitFor(condition: () => boolean | Promise<boolean>, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (!(await condition())) {
    if (Date.now() > deadline) {
      throw new Error('Timed out waiting for condition');
    }
    await delay(25);
  }
}

interface Scanner {
  scan(cursor: string, ...args: (string | number)[]): Promise<[string, string[]]>;
  del(...keys: string[]): Promise<number>;
}

async function deleteKeys(prefix: string): Promise<void> {
  const queue = new Queue('cleanup', { connection, prefix });
  const client = (await queue.client) as unknown as Scanner;
  let cursor = '0';
  do {
    const [next, keys] = await client.scan(cursor, 'MATCH', `${prefix}:*`, 'COUNT', 500);
    cursor = next;
    if (keys.length > 0) {
      await client.del(...keys);
    }
  } while (cursor !== '0');
  await queue.close();
}

describe.skipIf(!redisUp)('Integration: BullMQJobsBackend', function () {
  const job: JobEnvelope = { type: 'test-job', payload: '{"value":1}' };

  afterAll(async function () {
    for (const prefix of prefixes) {
      await deleteKeys(prefix);
    }
  });

  afterEach(function () {
    sinon.restore();
  });

  // Each backend gets its own prefix: the contract reuses the dispatch id
  // "dispatch-1", which BullMQ would otherwise dedupe across tests.
  runJobsBackendContractTests(() => makeBackend(), {
    describe: (name, fn) => describe(name, fn),
    it,
  });

  it('dedupes an enqueue that reuses a dispatch id', async function () {
    const backend = makeBackend();
    const received: JobEnvelope[] = [];
    await backend.start({
      processor: async (env) => {
        received.push(env);
      },
    });

    await backend.enqueue('dup-1', dispatch(job));
    await waitFor(() => received.length === 1);
    await backend.enqueue('dup-1', dispatch(job));
    await delay(300);
    await backend.shutdown({ timeoutMs: 1000 });

    assert.equal(received.length, 1);
  });

  it('caps concurrency per lane within one process', async function () {
    const running = { serial: 0, default: 0 };
    const max = { serial: 0, default: 0 };
    const release: Array<() => void> = [];
    const backend = makeBackend({ concurrency: 2 });
    await backend.start({
      processor: async (env) => {
        const lane = env.type === 'serial-job' ? 'serial' : 'default';
        running[lane] += 1;
        max[lane] = Math.max(max[lane], running[lane]);
        await new Promise<void>((resolve) => {
          release.push(resolve);
        });
        running[lane] -= 1;
      },
      queues: { serial: { concurrency: 1 } },
    });

    for (let i = 0; i < 4; i += 1) {
      await backend.enqueue(`s${i}`, dispatch({ type: 'serial-job', payload: '{}' }, 'serial'));
      await backend.enqueue(`d${i}`, dispatch({ type: 'plain-job', payload: '{}' }));
    }

    await waitFor(() => max.serial === 1 && max.default === 2);
    await delay(200);
    assert.equal(max.serial, 1);
    assert.equal(max.default, 2);

    const drain = setInterval(() => release.splice(0).forEach((resolve) => resolve()), 10);
    await backend.shutdown({ timeoutMs: 3000 });
    clearInterval(drain);
  });

  it('puts a job routed to an undeclared queue on its own BullMQ queue', async function () {
    const backend = makeBackend();
    const received: JobEnvelope[] = [];
    await backend.start({
      processor: async (env) => {
        received.push(env);
      },
    });

    await backend.enqueue('u1', dispatch(job, 'undeclared'));
    await waitFor(() => received.length === 1);
    await backend.shutdown({ timeoutMs: 1000 });

    const queue = inspect(prefixes[prefixes.length - 1]!, 'undeclared');
    const stored = await queue.getJob('u1');
    await queue.close();
    assert.deepEqual(stored?.data, job);
  });

  it('recovers a job from a stalled worker on another backend with the same prefix', async function () {
    const prefix = newPrefix();
    const a = new BullMQJobsBackend({ connection, prefix, ...fastConfig });
    const b = new BullMQJobsBackend({ connection, prefix, ...fastConfig });
    const started = Promise.withResolvers<void>();
    await a.start({
      processor: () => {
        started.resolve();
        return new Promise<void>(() => {});
      },
    });
    await a.enqueue('stall-1', dispatch(job));
    await started.promise;
    // Force close abandons the job mid-run, as a killed container would
    await a.shutdown({ timeoutMs: 50 });

    const received: JobEnvelope[] = [];
    await b.start({
      processor: async (env) => {
        received.push(env);
      },
    });
    await waitFor(() => received.length === 1, 8000);
    await b.shutdown({ timeoutMs: 1000 });

    assert.deepEqual(received, [job]);
  });

  it('leaves waiting jobs in Redis on shutdown for another backend with the same prefix', async function () {
    const prefix = newPrefix();
    const a = new BullMQJobsBackend({ connection, prefix, ...fastConfig, concurrency: 1 });
    const b = new BullMQJobsBackend({ connection, prefix, ...fastConfig });
    const started = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const ranOnA: JobEnvelope[] = [];
    await a.start({
      processor: async (env) => {
        ranOnA.push(env);
        started.resolve();
        await release.promise;
      },
    });
    const first: JobEnvelope = { type: 'first-job', payload: '{}' };
    const second: JobEnvelope = { type: 'second-job', payload: '{}' };
    await a.enqueue('hold-1', dispatch(first));
    await started.promise;
    // The only slot is busy, so this job is still waiting when shutdown begins
    await a.enqueue('wait-1', dispatch(second));

    const shutdown = a.shutdown({ timeoutMs: 1000 });
    release.resolve();
    await shutdown;

    const received: JobEnvelope[] = [];
    await b.start({
      processor: async (env) => {
        received.push(env);
      },
    });
    await waitFor(() => received.length === 1);
    await b.shutdown({ timeoutMs: 1000 });

    assert.deepEqual(ranOnA, [first]);
    assert.deepEqual(received, [second]);
  });

  it('logs a rejected processor and leaves the job in the failed set', async function () {
    const errorStub = sinon.stub(logging, 'error');
    const backend = makeBackend();
    const attempted = Promise.withResolvers<void>();
    await backend.start({
      processor: async () => {
        attempted.resolve();
        throw new Error('boom');
      },
    });

    await backend.enqueue('fail-1', dispatch(job));
    await attempted.promise;
    const queue = inspect(prefixes[prefixes.length - 1]!);
    await waitFor(async () => (await (await queue.getJob('fail-1'))?.getState()) === 'failed');
    await backend.shutdown({ timeoutMs: 1000 });
    await queue.close();

    assert.match(String(errorStub.firstCall.args[0]), /test-job/);
  });

  describe('recurring schedules', function () {
    const recurring = (type: string): DispatchEnvelope => dispatch({ type, payload: '{}' });

    it('keeps the stored schedule when a later registration has a different cron', async function () {
      const first = makeBackend();
      const prefix = prefixes[prefixes.length - 1]!;
      const second = new BullMQJobsBackend({ connection, prefix, ...fastConfig });
      await first.start({ processor: async () => {} });
      await second.start({ processor: async () => {} });

      await first.scheduleRecurring(recurring('nightly'), { cron: '0 3 * * *' });
      await first.scheduleRecurring(recurring('nightly'), { cron: '0 4 * * *' });
      await second.scheduleRecurring(recurring('nightly'), { cron: '0 5 * * *' });

      const queue = inspect(prefix);
      const stored = await queue.getJobScheduler('nightly');
      await queue.close();
      await first.shutdown({ timeoutMs: 1000 });
      await second.shutdown({ timeoutMs: 1000 });

      assert.equal(stored?.pattern, '0 3 * * *');
    });

    it('stores the configured timezone', async function () {
      const backend = makeBackend({ timezone: 'Europe/London' });
      await backend.start({ processor: async () => {} });
      await backend.scheduleRecurring(recurring('nightly'), { cron: '0 3 * * *' });

      const queue = inspect(prefixes[prefixes.length - 1]!);
      const stored = await queue.getJobScheduler('nightly');
      await queue.close();
      await backend.shutdown({ timeoutMs: 1000 });

      assert.equal(stored?.tz, 'Europe/London');
    });

    it('defaults the timezone to the process timezone', async function () {
      const backend = makeBackend();
      await backend.start({ processor: async () => {} });
      await backend.scheduleRecurring(recurring('nightly'), { cron: '0 3 * * *' });

      const queue = inspect(prefixes[prefixes.length - 1]!);
      const stored = await queue.getJobScheduler('nightly');
      await queue.close();
      await backend.shutdown({ timeoutMs: 1000 });

      assert.equal(stored?.tz, Intl.DateTimeFormat().resolvedOptions().timeZone);
    });
  });

  it('fails enqueue fast when Redis is unreachable and still shuts down', async function () {
    const backend = new BullMQJobsBackend({
      connection: { host: '127.0.0.1', port: 1 },
      prefix: newPrefix(),
    });
    sinon.stub(logging, 'error');
    await backend.start({ processor: async () => {} });

    const outcome = await Promise.race([
      backend.enqueue('x', dispatch(job)).then(
        () => 'accepted',
        () => 'rejected',
      ),
      delay(4000).then(() => 'hung'),
    ]);
    assert.equal(outcome, 'rejected');

    await backend.shutdown({ timeoutMs: 500 });
  });

  it('fails a recurring schedule fast when Redis is unreachable', async function () {
    const backend = new BullMQJobsBackend({
      connection: { host: '127.0.0.1', port: 1 },
      prefix: newPrefix(),
    });
    sinon.stub(logging, 'error');
    await backend.start({ processor: async () => {} });

    const outcome = await Promise.race([
      backend.scheduleRecurring(dispatch(job), { cron: '0 3 * * *' }).then(
        () => 'accepted',
        () => 'rejected',
      ),
      delay(4000).then(() => 'hung'),
    ]);
    assert.equal(outcome, 'rejected');

    await backend.shutdown({ timeoutMs: 500 });
  });
});
