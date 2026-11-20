import request from 'supertest';
import { createApp } from '../src/app';
import { setupTestDB, teardownTestDB, registerWorkspace } from './setup';
import { Opportunity } from '../src/models/Opportunity';
import { OpportunityStageTransition } from '../src/models/OpportunityStageTransition';
import { BulkJob } from '../src/models/BulkJob';
import { WorkerEngine } from '../src/services/worker.service';
import { config } from '../src/config';

const app = createApp();

async function seedDeals(wsId: string, count: number, stageId = 'stage-source') {
  await Opportunity.insertMany(
    Array.from({ length: count }).map((_, i) => ({
      workspaceId: wsId,
      name: `Deal ${i + 1}`,
      monetaryValue: 100 + i,
      stageId,
      ownerId: 'rep-1',
      status: 'open',
    })),
    { ordered: false }
  );
}

async function submit(wsId: string, body: any, headers: Record<string, string> = {}) {
  return request(app).post('/bulk-jobs').set('X-Workspace-Id', wsId).set(headers).send(body);
}

const move = { filter: { stageId: 'stage-source' }, targetStageId: 'stage-destination' };

describe('Hardening: crash consistency, fencing, retries, validation', () => {
  const original = { chunkSize: config.chunkSize, lease: config.leaseDurationMs, pacing: config.chunkPacingMs };

  beforeAll(async () => {
    await setupTestDB();
    config.chunkSize = 20;
    config.leaseDurationMs = 50;
    config.chunkPacingMs = 0;
  });

  // Workers claim the oldest eligible job globally, so leftover jobs would leak between tests
  beforeEach(async () => {
    await BulkJob.deleteMany({});
  });

  afterAll(async () => {
    config.chunkSize = original.chunkSize;
    config.leaseDurationMs = original.lease;
    config.chunkPacingMs = original.pacing;
    await teardownTestDB();
  });

  describe.each(['after_bulk_write', 'after_transitions'] as const)(
    'crash between writes (%s)',
    (crashPoint) => {
      it('keeps transitions and counters exact after recovery', async () => {
        const wsId = `ws-crash-${crashPoint}`;
        await registerWorkspace(wsId, ['stage-source', 'stage-destination']);
        await seedDeals(wsId, 60);

        const res = await submit(wsId, move);
        expect(res.status).toBe(201);
        const jobId = res.body.id;

        // Dies after moving the first chunk but before the cursor/counters were committed
        const dying = new WorkerEngine('dies-mid-chunk');
        dying.crashAt = crashPoint;
        await dying.processNextJob();

        const mid = await BulkJob.findById(jobId);
        expect(mid?.status).toBe('processing');
        expect(mid?.cursor).toBeNull();
        expect(mid?.processedCount).toBe(0);
        // The chunk was physically moved even though the job recorded nothing
        expect(await Opportunity.countDocuments({ workspaceId: wsId, stageId: 'stage-destination' })).toBe(20);

        await new Promise((r) => setTimeout(r, 70));
        await new WorkerEngine('recovery').processNextJob();

        const done = await BulkJob.findById(jobId);
        expect(done?.status).toBe('completed');
        expect(done?.updatedCount).toBe(60);
        expect(done?.skippedCount).toBe(0);
        expect(done?.processedCount).toBe(60);
        expect(done?.active).toBe(false);

        // Exactly one transition per moved deal: none lost, none duplicated
        const transitions = await OpportunityStageTransition.find({ jobId }).lean();
        expect(transitions.length).toBe(60);
        expect(new Set(transitions.map((t) => t.opportunityId.toString())).size).toBe(60);
        expect(await Opportunity.countDocuments({ workspaceId: wsId, stageId: 'stage-destination' })).toBe(60);
      });
    }
  );

  it('fences a stale worker so it cannot overwrite the new lease owner state', async () => {
    const wsId = 'ws-fencing';
    await registerWorkspace(wsId, ['stage-source', 'stage-destination']);
    await seedDeals(wsId, 40);
    const res = await submit(wsId, move);
    const jobId = res.body.id;

    const stale = new WorkerEngine('stale-worker');
    const claimed = await stale.claimNextJob();
    expect(claimed?._id.toString()).toBe(jobId);

    // Another worker takes over the lease while the stale one is still running
    await BulkJob.updateOne({ _id: jobId }, { $set: { workerId: 'new-owner' } });

    await stale.processJob(claimed!);

    const job = await BulkJob.findById(jobId);
    expect(job?.workerId).toBe('new-owner');
    expect(job?.status).toBe('processing');
    expect(job?.processedCount).toBe(0);
    expect(job?.cursor).toBeNull();
  });

  it('enforces one processing job per workspace atomically under concurrent claims', async () => {
    const wsId = 'ws-atomic-cap';
    await registerWorkspace(wsId, ['stage-source', 'stage-destination', 'stage-other', 'stage-other-2']);
    await seedDeals(wsId, 5);
    await seedDeals(wsId, 5, 'stage-other');
    await submit(wsId, move);
    await submit(wsId, { filter: { stageId: 'stage-other' }, targetStageId: 'stage-other-2' });

    const claims = await Promise.all(
      Array.from({ length: 6 }).map((_, i) => new WorkerEngine(`racer-${i}`).claimNextJob())
    );

    expect(claims.filter(Boolean).length).toBe(1);
    expect(await BulkJob.countDocuments({ workspaceId: wsId, status: 'processing' })).toBe(1);
  });

  it('retries a transient failure with backoff, resuming instead of restarting', async () => {
    const wsId = 'ws-retry';
    await registerWorkspace(wsId, ['stage-source', 'stage-destination']);
    await seedDeals(wsId, 40);
    const jobId = (await submit(wsId, move)).body.id;

    const realFind = Opportunity.find.bind(Opportunity);
    let reads = 0;
    const failing = jest.spyOn(Opportunity, 'find').mockImplementation(((...args: any[]) => {
      reads++;
      // reads: 1 repair, 2 chunk-1 read, 3 chunk-1 verify, 4 chunk-2 read
      if (reads === 4) throw new Error('MongoNetworkError: connection dropped');
      return (realFind as any)(...args);
    }) as any);

    const worker = new WorkerEngine('retry-worker');
    await worker.processNextJob();
    failing.mockRestore();

    const after = await BulkJob.findById(jobId);
    expect(after?.status).toBe('queued');
    expect(after?.attempts).toBe(1);
    expect(after?.nextAttemptAt!.getTime()).toBeGreaterThan(Date.now());
    expect(after?.processedCount).toBe(20);

    // Backoff not elapsed: nobody picks it up
    expect(await new WorkerEngine('too-early').claimNextJob()).toBeNull();

    await BulkJob.updateOne({ _id: jobId }, { $set: { nextAttemptAt: new Date(Date.now() - 1) } });
    await new WorkerEngine('retry-2').processNextJob();

    const done = await BulkJob.findById(jobId);
    expect(done?.status).toBe('completed');
    expect(done?.updatedCount).toBe(40);
    expect(await OpportunityStageTransition.countDocuments({ jobId })).toBe(40);
  });

  it('marks a job failed after max consecutive failed attempts instead of retrying forever', async () => {
    const wsId = 'ws-fail';
    await registerWorkspace(wsId, ['stage-source', 'stage-destination']);
    await seedDeals(wsId, 10);
    const jobId = (await submit(wsId, move)).body.id;

    const failing = jest.spyOn(Opportunity, 'find').mockImplementation((() => {
      throw new Error('permanent failure');
    }) as any);

    for (let i = 0; i < config.maxJobAttempts; i++) {
      await BulkJob.updateOne({ _id: jobId }, { $set: { nextAttemptAt: null } });
      await new WorkerEngine(`failer-${i}`).processNextJob();
    }
    failing.mockRestore();

    const job = await BulkJob.findById(jobId);
    expect(job?.status).toBe('failed');
    expect(job?.active).toBe(false);
    expect(job?.errorMessage).toContain('permanent failure');
  });

  it('completes immediately when nothing matches and never touches later arrivals', async () => {
    const wsId = 'ws-empty';
    await registerWorkspace(wsId, ['stage-source', 'stage-destination']);

    const res = await submit(wsId, move);
    expect(res.status).toBe(201);
    expect(res.body.status).toBe('completed');

    await seedDeals(wsId, 5); // arrive after submission
    expect(await new WorkerEngine('idle').processNextJob()).toBe(false);
    expect(await Opportunity.countDocuments({ workspaceId: wsId, stageId: 'stage-source' })).toBe(5);
  });

  it('rejects unknown target/source stages and operator-injection in filters', async () => {
    const wsId = 'ws-validation';
    await registerWorkspace(wsId, ['stage-source', 'stage-destination']);

    const badTarget = await submit(wsId, { filter: { stageId: 'stage-source' }, targetStageId: 'stage-typo' });
    expect(badTarget.status).toBe(400);

    const badSource = await submit(wsId, { filter: { stageId: 'stage-nope' }, targetStageId: 'stage-destination' });
    expect(badSource.status).toBe(400);

    const injected = await submit(wsId, {
      filter: { stageId: 'stage-source', ownerId: { $ne: null } },
      targetStageId: 'stage-destination',
    });
    expect(injected.status).toBe(400);

    const unknownWs = await submit('ws-does-not-exist', move);
    expect(unknownWs.status).toBe(404);
  });

  it('dedupes implicit requests only while active, and client keys permanently', async () => {
    const wsId = 'ws-dedupe-lifecycle';
    await registerWorkspace(wsId, ['stage-source', 'stage-destination']);
    await seedDeals(wsId, 3);

    const first = await submit(wsId, move);
    const replay = await submit(wsId, move);
    expect(replay.status).toBe(200);
    expect(replay.body.id).toBe(first.body.id);

    await new WorkerEngine('finisher').processNextJob();

    // Same filter after completion is a genuinely new request (new deals may have arrived)
    await seedDeals(wsId, 2);
    const again = await submit(wsId, move);
    expect(again.status).toBe(201);
    expect(again.body.id).not.toBe(first.body.id);
    await new WorkerEngine('finisher-2').processNextJob();

    // Explicit client keys stay idempotent forever, and cannot be reused for a different request
    await seedDeals(wsId, 1);
    const keyed = await submit(wsId, move, { 'Idempotency-Key': 'key-1' });
    await new WorkerEngine('finisher-3').processNextJob();
    const keyedReplay = await submit(wsId, move, { 'Idempotency-Key': 'key-1' });
    expect(keyedReplay.status).toBe(200);
    expect(keyedReplay.body.id).toBe(keyed.body.id);

    const mismatch = await submit(
      wsId,
      { filter: { stageId: 'stage-source', ownerId: 'someone' }, targetStageId: 'stage-destination' },
      { 'Idempotency-Key': 'key-1' }
    );
    expect(mismatch.status).toBe(409);
  });
});
