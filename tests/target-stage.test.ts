import request from 'supertest';
import { createApp } from '../src/app';
import { setupTestDB, teardownTestDB, registerWorkspace } from './setup';
import { Opportunity } from '../src/models/Opportunity';
import { OpportunityStageTransition } from '../src/models/OpportunityStageTransition';
import { WorkerEngine } from '../src/services/worker.service';

const app = createApp();

describe('Requirement 2: Target Stage Validation', () => {
  beforeAll(async () => {
    await setupTestDB();
    await registerWorkspace('ws-target-validation', ['stage-A','stage-B']);
  });

  afterAll(async () => {
    await teardownTestDB();
  });

  it('should reject bulk job request if source stage equals target stage', async () => {
    const res = await request(app)
      .post('/bulk-jobs')
      .set('X-Workspace-Id', 'ws-target-validation')
      .send({
        filter: { stageId: 'stage-A' },
        targetStageId: 'stage-A',
      });

    expect(res.status).toBe(400);
    expect(res.body.error).toContain('Target stage cannot be identical to source stage');
  });

  it('should skip deals already in target stage and avoid unnecessary updates or transitions', async () => {
    // Create 10 deals in stage-A
    const oppsA = await Opportunity.insertMany([
      {
        workspaceId: 'ws-target-validation',
        name: 'Deal A1',
        monetaryValue: 1000,
        stageId: 'stage-A',
        ownerId: 'rep-1',
        status: 'open',
      },
      {
        workspaceId: 'ws-target-validation',
        name: 'Deal A2',
        monetaryValue: 1000,
        stageId: 'stage-A',
        ownerId: 'rep-1',
        status: 'open',
      },
    ]);

    // Create a job targeting stage-B
    const res = await request(app)
      .post('/bulk-jobs')
      .set('X-Workspace-Id', 'ws-target-validation')
      .send({
        filter: { stageId: 'stage-A' },
        targetStageId: 'stage-B',
      });

    expect(res.status).toBe(201);
    const jobId = res.body.id;

    // Simulate concurrent move: Deal A1 gets moved to stage-B before worker processes it
    await Opportunity.updateOne(
      { _id: oppsA[0]._id },
      { $set: { stageId: 'stage-B', updatedAt: new Date() } }
    );

    // Run worker
    const worker = new WorkerEngine('test-worker-target');
    await worker.processNextJob();

    // Check progress
    const checkRes = await request(app)
      .get(`/bulk-jobs/${jobId}`)
      .set('X-Workspace-Id', 'ws-target-validation');

    expect(checkRes.status).toBe(200);
    expect(checkRes.body.status).toBe('completed');
    // 1 updated (Deal A2), 1 skipped (Deal A1)
    expect(checkRes.body.updatedCount).toBe(1);
    expect(checkRes.body.skippedCount).toBe(1);

    // Verify only Deal A2 got a bulk_job transition
    const transitions = await OpportunityStageTransition.find({
      jobId,
    });
    expect(transitions.length).toBe(1);
    expect(transitions[0].opportunityId.toString()).toBe(oppsA[1]._id.toString());
  });
});
