import request from 'supertest';
import { createApp } from '../src/app';
import { setupTestDB, teardownTestDB } from './setup';
import { Opportunity } from '../src/models/Opportunity';
import { BulkJob } from '../src/models/BulkJob';
import { WorkerEngine } from '../src/services/worker.service';

const app = createApp();

describe('Requirement 5: Well-behaved & Workspace Isolation', () => {
  beforeAll(async () => {
    await setupTestDB();

    // Create 100 deals in ws-tenant-A
    const docsA = Array.from({ length: 100 }).map((_, i) => ({
      workspaceId: 'ws-tenant-A',
      name: `Tenant A Deal ${i}`,
      monetaryValue: 5000,
      stageId: 'stage-A1',
      ownerId: 'rep-A',
      status: 'open',
    }));
    await Opportunity.insertMany(docsA, { ordered: false });

    // Create 100 deals in ws-tenant-B
    const docsB = Array.from({ length: 100 }).map((_, i) => ({
      workspaceId: 'ws-tenant-B',
      name: `Tenant B Deal ${i}`,
      monetaryValue: 5000,
      stageId: 'stage-B1',
      ownerId: 'rep-B',
      status: 'open',
    }));
    await Opportunity.insertMany(docsB, { ordered: false });
  });

  afterAll(async () => {
    await teardownTestDB();
  });

  it('should enforce per-workspace concurrency cap to prevent tenant starvation', async () => {
    // Submit 2 jobs for ws-tenant-A
    const resA1 = await request(app)
      .post('/bulk-jobs')
      .set('X-Workspace-Id', 'ws-tenant-A')
      .set('Idempotency-Key', 'job-A1')
      .send({
        filter: { stageId: 'stage-A1', ownerId: 'rep-A' },
        targetStageId: 'stage-A2',
      });

    const resA2 = await request(app)
      .post('/bulk-jobs')
      .set('X-Workspace-Id', 'ws-tenant-A')
      .set('Idempotency-Key', 'job-A2')
      .send({
        filter: { stageId: 'stage-A1', ownerId: 'rep-A' },
        targetStageId: 'stage-A3',
      });

    // Submit 1 job for ws-tenant-B
    const resB1 = await request(app)
      .post('/bulk-jobs')
      .set('X-Workspace-Id', 'ws-tenant-B')
      .set('Idempotency-Key', 'job-B1')
      .send({
        filter: { stageId: 'stage-B1' },
        targetStageId: 'stage-B2',
      });

    const worker = new WorkerEngine('isolation-worker');

    // Worker claims next job -> should claim Job A1
    const claimedJob1 = await worker.claimNextJob();
    expect(claimedJob1).not.toBeNull();
    expect(claimedJob1?.workspaceId).toBe('ws-tenant-A');

    // Worker attempts to claim another job while Job A1 is active:
    // It should SKIP Job A2 (due to per-workspace concurrency cap) and claim Job B1!
    const claimedJob2 = await worker.claimNextJob();
    expect(claimedJob2).not.toBeNull();
    expect(claimedJob2?.workspaceId).toBe('ws-tenant-B');
  });

  it('should allow concurrent interactive requests to succeed while bulk job is active', async () => {
    // Interactive GET request
    const getRes = await request(app)
      .get('/opportunities?stage=stage-B1&page=1&limit=10')
      .set('X-Workspace-Id', 'ws-tenant-B');

    expect(getRes.status).toBe(200);
    expect(getRes.body.data.length).toBe(10);
    expect(getRes.body.total).toBe(100);

    // Interactive POST create request
    const createRes = await request(app)
      .post('/opportunities')
      .set('X-Workspace-Id', 'ws-tenant-B')
      .send({
        name: 'Urgent Interactive Deal',
        monetaryValue: 99000,
        stageId: 'stage-B1',
        ownerId: 'rep-B',
      });

    expect(createRes.status).toBe(201);
    expect(createRes.body.name).toBe('Urgent Interactive Deal');
  });
});
