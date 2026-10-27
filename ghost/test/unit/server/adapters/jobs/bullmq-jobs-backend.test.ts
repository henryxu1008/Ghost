import assert from 'node:assert/strict';
import type { DispatchEnvelope } from '@tryghost/adapter-base-jobs';
import errors from '@tryghost/errors';
import BullMQJobsBackend from '../../../../../core/server/adapters/jobs/BullMQJobsBackend';

const defaults = require('../../../../../core/shared/config/defaults.json');

const envelope: DispatchEnvelope = {
  version: 1,
  payload: { job: { type: 'test-job', payload: '{}' } },
};

// These tests never start the backend, so they open no Redis connection.
describe('BullMQJobsBackend', function () {
  it('has the required contract methods', function () {
    const backend = new BullMQJobsBackend();
    for (const fn of backend.requiredFns) {
      assert.equal(typeof backend[fn], 'function', `${fn} is a function`);
    }
  });

  it('accepts the checked-in default config', function () {
    assert.doesNotThrow(() => new BullMQJobsBackend(defaults.adapters.jobs.BullMQJobsBackend));
  });

  for (const [field, value] of [
    ['concurrency', 0],
    ['concurrency', '3'],
    ['prefix', ''],
    ['prefix', 42],
    ['lockDuration', -1],
    ['stalledInterval', 1.5],
    ['maxStalledCount', -1],
    ['timezone', 'Not/AZone'],
    ['timezone', 42],
    ['connection', { port: 0 }],
    ['connection', { port: '6379' }],
    ['connection', { host: '' }],
    ['connection', { db: -1 }],
    ['connection', { tls: 'yes' }],
  ] as const) {
    it(`rejects an invalid ${field} config ${JSON.stringify(value)}`, function () {
      assert.throws(
        () => new BullMQJobsBackend({ [field]: value }),
        (err: Error) =>
          err instanceof errors.IncorrectUsageError &&
          /Invalid jobs backend/.test(err.message) &&
          err.message.includes(field),
      );
    });
  }

  it('throws on an enqueue before start instead of silently losing the job', async function () {
    await assert.rejects(
      async () => new BullMQJobsBackend().enqueue('d1', envelope),
      /before the jobs backend is started/,
    );
  });

  it('throws on a recurring schedule before start', async function () {
    await assert.rejects(
      async () => new BullMQJobsBackend().scheduleRecurring(envelope, { cron: '* * * * *' }),
      /before the jobs backend is started/,
    );
  });

  it('rejects an invalid declared queue concurrency at start instead of silently ignoring it', function () {
    assert.throws(
      () =>
        new BullMQJobsBackend().start({
          processor: async () => {},
          queues: { bad: { concurrency: 0 } },
        }),
      /Invalid concurrency for declared queue "bad"/,
    );
  });
});
