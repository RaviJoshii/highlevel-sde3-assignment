import request from 'supertest';
import { createApp } from '../src/app';
import { setupTestDB, teardownTestDB } from './setup';
import { Opportunity } from '../src/models/Opportunity';
import { BulkJob } from '../src/models/BulkJob';

const app = createApp();

describe('Requirement 4: Idempotency & Duplicate Request Prevention', () => {
  beforeAll(async () => {
    await setupTestDB();

    // Create 10 sample opportunities
    for (let i = 0; i < 10; i++) {
      await Opportunity.create({
        workspaceId: 'ws-idempotency',
        name: `Deal ${i}`,
        monetaryValue: 5000,
        stageId: 'stage-1',
        ownerId: 'rep-alice',
        status: 'open',
      });
    }
  });

  afterAll(async () => {
    await teardownTestDB();
  });

  it('should create only one job when client sends identical requests sequentially', async () => {
    const payload = {
      filter: { stageId: 'stage-1' },
      targetStageId: 'stage-2',
    };
    const idempotencyKey = 'client-req-uuid-12345';

    // First request: Creates job (201 Created)
    const res1 = await request(app)
      .post('/bulk-jobs')
      .set('X-Workspace-Id', 'ws-idempotency')
      .set('Idempotency-Key', idempotencyKey)
      .send(payload);

    expect(res1.status).toBe(201);
    expect(res1.body.idempotentReplay).toBe(false);
    const originalJobId = res1.body.id;

    // Retry request (e.g. timeout retry from client): Returns existing job (200 OK)
    const res2 = await request(app)
      .post('/bulk-jobs')
      .set('X-Workspace-Id', 'ws-idempotency')
      .set('Idempotency-Key', idempotencyKey)
      .send(payload);

    expect(res2.status).toBe(200);
    expect(res2.body.idempotentReplay).toBe(true);
    expect(res2.body.id).toBe(originalJobId);

    // Verify only ONE job exists in the database
    const jobs = await BulkJob.find({
      workspaceId: 'ws-idempotency',
      idempotencyKey,
    });
    expect(jobs.length).toBe(1);
  });

  it('should handle concurrent identical requests safely without creating duplicate jobs', async () => {
    const payload = {
      filter: { stageId: 'stage-1', ownerId: 'rep-alice' },
      targetStageId: 'stage-3',
    };
    const idempotencyKey = 'concurrent-req-key-999';

    // Fire 5 identical requests concurrently
    const requests = Array.from({ length: 5 }).map(() =>
      request(app)
        .post('/bulk-jobs')
        .set('X-Workspace-Id', 'ws-idempotency')
        .set('Idempotency-Key', idempotencyKey)
        .send(payload)
    );

    const responses = await Promise.all(requests);

    // All should succeed (200 or 201)
    responses.forEach((res) => {
      expect([200, 201]).toContain(res.status);
    });

    // Exactly one should be 201 (created), and the rest 200 (replay)
    const createdCount = responses.filter((r) => r.status === 201).length;
    const replayCount = responses.filter((r) => r.status === 200).length;

    expect(createdCount).toBe(1);
    expect(replayCount).toBe(4);

    // All must reference the exact same job ID
    const jobIds = new Set(responses.map((r) => r.body.id));
    expect(jobIds.size).toBe(1);

    // Total jobs in MongoDB must be 1
    const totalJobs = await BulkJob.countDocuments({
      workspaceId: 'ws-idempotency',
      idempotencyKey,
    });
    expect(totalJobs).toBe(1);
  });

  it('should automatically deduplicate via deterministic SHA-256 fingerprint when no header is sent', async () => {
    const payload = {
      filter: { stageId: 'stage-1', status: 'open' },
      targetStageId: 'stage-4',
    };

    const res1 = await request(app)
      .post('/bulk-jobs')
      .set('X-Workspace-Id', 'ws-idempotency')
      .send(payload);

    expect(res1.status).toBe(201);
    expect(res1.body.idempotentReplay).toBe(false);

    // Identical payload without header
    const res2 = await request(app)
      .post('/bulk-jobs')
      .set('X-Workspace-Id', 'ws-idempotency')
      .send(payload);

    expect(res2.status).toBe(200);
    expect(res2.body.idempotentReplay).toBe(true);
    expect(res2.body.id).toBe(res1.body.id);
  });
});
