import request from 'supertest';
import { createApp } from '../src/app';
import { setupTestDB, teardownTestDB, registerWorkspace } from './setup';
import { Opportunity } from '../src/models/Opportunity';
import { OpportunityStageTransition } from '../src/models/OpportunityStageTransition';
import { BulkJob } from '../src/models/BulkJob';
import { WorkerEngine } from '../src/services/worker.service';
import { config } from '../src/config';

const app = createApp();

describe('Requirement 5: Crash Recovery & Resumability', () => {
  const originalChunkSize = config.chunkSize;

  beforeAll(async () => {
    await setupTestDB();
    await registerWorkspace('ws-crash-recovery', ['stage-source','stage-destination']);
    // Set chunk size to 20 for precise multi-chunk crash testing
    config.chunkSize = 20;
    config.leaseDurationMs = 50; // short lease for testing
  });

  afterAll(async () => {
    config.chunkSize = originalChunkSize;
    config.leaseDurationMs = 30000;
    await teardownTestDB();
  });

  it('should resume an interrupted job from cursor without duplicating transitions', async () => {
    const wsId = 'ws-crash-recovery';
    const totalDeals = 60; // 3 chunks of 20

    // 1. Seed 60 deals
    const docs = Array.from({ length: totalDeals }).map((_, i) => ({
      workspaceId: wsId,
      name: `Crash Test Deal ${i + 1}`,
      monetaryValue: 1000 + i,
      stageId: 'stage-source',
      ownerId: 'rep-worker',
      status: 'open',
    }));
    await Opportunity.insertMany(docs, { ordered: false });

    // 2. Submit Bulk Job
    const jobRes = await request(app)
      .post('/bulk-jobs')
      .set('X-Workspace-Id', wsId)
      .send({
        filter: { stageId: 'stage-source' },
        targetStageId: 'stage-destination',
      });

    expect(jobRes.status).toBe(201);
    const jobId = jobRes.body.id;

    // 3. Worker 1 starts, processes 1 chunk (20 records), and crashes!
    const crashingWorker = new WorkerEngine('worker-will-die');
    crashingWorker.crashAfterRecords = 20;

    await crashingWorker.processNextJob();

    // 4. Verify intermediate crashed state
    const intermediateJob = await BulkJob.findById(jobId);
    expect(intermediateJob).not.toBeNull();
    expect(intermediateJob?.status).toBe('processing');
    expect(intermediateJob?.processedCount).toBe(20);
    expect(intermediateJob?.updatedCount).toBe(20);
    expect(intermediateJob?.cursor).not.toBeNull();

    // Exactly 20 transitions recorded so far
    const intermediateTransitions = await OpportunityStageTransition.countDocuments({
      jobId,
    });
    expect(intermediateTransitions).toBe(20);

    // Wait for lease to expire (50ms)
    await new Promise((r) => setTimeout(r, 60));

    // 5. Recovery Worker comes online and claims the stalled job
    const recoveryWorker = new WorkerEngine('recovery-worker-hero');
    await recoveryWorker.processNextJob();

    // 6. Verify final completed state
    const completedJob = await BulkJob.findById(jobId);
    expect(completedJob?.status).toBe('completed');
    expect(completedJob?.processedCount).toBe(60);
    expect(completedJob?.updatedCount).toBe(60);
    expect(completedJob?.skippedCount).toBe(0);

    // 7. CRITICAL VERIFICATION:
    // Total transitions created must be EXACTLY 60 (zero duplicate transitions for records 1..20!)
    const allTransitions = await OpportunityStageTransition.find({ jobId });
    expect(allTransitions.length).toBe(60);

    // Verify each opportunity has exactly one transition
    const opportunityIds = allTransitions.map((t) => t.opportunityId.toString());
    const uniqueOpportunityIds = new Set(opportunityIds);
    expect(uniqueOpportunityIds.size).toBe(60);

    // Verify all 60 deals are now in stage-destination
    const remainingInSource = await Opportunity.countDocuments({
      workspaceId: wsId,
      stageId: 'stage-source',
    });
    expect(remainingInSource).toBe(0);

    const inDestination = await Opportunity.countDocuments({
      workspaceId: wsId,
      stageId: 'stage-destination',
    });
    expect(inDestination).toBe(60);
  });
});
