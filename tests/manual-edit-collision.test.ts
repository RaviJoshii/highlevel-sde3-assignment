import request from 'supertest';
import { createApp } from '../src/app';
import { setupTestDB, teardownTestDB, registerWorkspace } from './setup';
import { Opportunity } from '../src/models/Opportunity';
import { OpportunityStageTransition } from '../src/models/OpportunityStageTransition';
import { WorkerEngine } from '../src/services/worker.service';
import { opportunityService } from '../src/services/opportunity.service';

const app = createApp();

describe('Requirement 1: Manual Edit Handling & Collision Safety (A->B->A)', () => {
  beforeAll(async () => {
    await setupTestDB();
    await registerWorkspace('ws-collision-test', ['stage-A','stage-TARGET']);
  });

  afterAll(async () => {
    await teardownTestDB();
  });

  it('should preserve manual edits and skip deals even if moved A -> B -> A', async () => {
    const wsId = 'ws-collision-test';

    // 1. Create 3 deals in stage-A
    const opp1 = await Opportunity.create({
      workspaceId: wsId,
      name: 'Untouched Deal',
      monetaryValue: 10000,
      stageId: 'stage-A',
      ownerId: 'rep-1',
      status: 'open',
      lastManualStageChangeAt: null,
      version: 1,
    });

    const opp2ManualMove = await Opportunity.create({
      workspaceId: wsId,
      name: 'Deal Moved A -> B',
      monetaryValue: 20000,
      stageId: 'stage-A',
      ownerId: 'rep-1',
      status: 'open',
      lastManualStageChangeAt: null,
      version: 1,
    });

    const opp3CycleABA = await Opportunity.create({
      workspaceId: wsId,
      name: 'Deal Moved A -> B -> A',
      monetaryValue: 30000,
      stageId: 'stage-A',
      ownerId: 'rep-1',
      status: 'open',
      lastManualStageChangeAt: null,
      version: 1,
    });

    // 2. Submit Bulk Job: Move all stage-A deals to stage-TARGET
    const jobRes = await request(app)
      .post('/bulk-jobs')
      .set('X-Workspace-Id', wsId)
      .send({
        filter: { stageId: 'stage-A' },
        targetStageId: 'stage-TARGET',
      });

    expect(jobRes.status).toBe(201);
    const jobId = jobRes.body.id;

    // Small delay so timestamps are strictly greater than snapshotCreatedAt
    await new Promise((r) => setTimeout(r, 20));

    // 3. User manually moves opp2: stage-A -> stage-B
    await opportunityService.moveStage({
      workspaceId: wsId,
      opportunityId: opp2ManualMove._id.toString(),
      toStageId: 'stage-B',
      triggeredBy: 'manual',
    });

    // 4. User manually moves opp3: stage-A -> stage-B, then back to stage-A (A -> B -> A)
    await opportunityService.moveStage({
      workspaceId: wsId,
      opportunityId: opp3CycleABA._id.toString(),
      toStageId: 'stage-B',
      triggeredBy: 'manual',
    });
    await opportunityService.moveStage({
      workspaceId: wsId,
      opportunityId: opp3CycleABA._id.toString(),
      toStageId: 'stage-A',
      triggeredBy: 'manual',
    });

    // Verify current state before worker runs:
    // opp3 is currently in stage-A!
    const opp3BeforeWorker = await Opportunity.findById(opp3CycleABA._id);
    expect(opp3BeforeWorker?.stageId).toBe('stage-A');

    // 5. Worker processes the bulk job
    const worker = new WorkerEngine('collision-worker');
    await worker.processNextJob();

    // 6. Verify job report
    const checkRes = await request(app)
      .get(`/bulk-jobs/${jobId}`)
      .set('X-Workspace-Id', wsId);

    expect(checkRes.status).toBe(200);
    expect(checkRes.body.status).toBe('completed');
    // opp1 should be updated (1 updated)
    // opp2 and opp3 should both be skipped! (2 skipped)
    expect(checkRes.body.updatedCount).toBe(1);
    expect(checkRes.body.skippedCount).toBe(2);

    // 7. Verify documents in MongoDB
    const freshOpp1 = await Opportunity.findById(opp1._id);
    expect(freshOpp1?.stageId).toBe('stage-TARGET'); // Successfully moved by bulk job

    const freshOpp2 = await Opportunity.findById(opp2ManualMove._id);
    expect(freshOpp2?.stageId).toBe('stage-B'); // Preserved manual move

    const freshOpp3 = await Opportunity.findById(opp3CycleABA._id);
    expect(freshOpp3?.stageId).toBe('stage-A'); // CRITICAL: Preserved in stage-A! NOT overwritten to stage-TARGET!

    // 8. Verify transitions for opp3: Only manual transitions exist, NO bulk_job transition
    const opp3Transitions = await OpportunityStageTransition.find({
      opportunityId: opp3CycleABA._id,
    });
    expect(opp3Transitions.length).toBe(2);
    expect(opp3Transitions.every((t) => t.triggeredBy === 'manual')).toBe(true);
  });
});
