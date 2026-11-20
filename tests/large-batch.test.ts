import request from 'supertest';
import { createApp } from '../src/app';
import { setupTestDB, teardownTestDB, registerWorkspace } from './setup';
import { Opportunity } from '../src/models/Opportunity';
import { BulkJob } from '../src/models/BulkJob';
import { config } from '../src/config';

const app = createApp();

describe('Requirement 3: Large Batch Handling (50k Limit)', () => {
  beforeAll(async () => {
    await setupTestDB();
    await registerWorkspace('ws-oversized', ['stage-oversized','stage-normal','stage-target']);

    // Insert 50,005 opportunities into stage-oversized
    console.log('[Test Setup] Generating 50,005 opportunities for batch limit test...');
    const totalToInsert = 50005;
    const batchSize = 10000;

    for (let i = 0; i < totalToInsert; i += batchSize) {
      const currentBatch = Math.min(batchSize, totalToInsert - i);
      const docs = Array.from({ length: currentBatch }).map((_, idx) => ({
        workspaceId: 'ws-oversized',
        name: `Oversized Deal ${i + idx}`,
        monetaryValue: 1000,
        stageId: 'stage-oversized',
        ownerId: 'rep-tester',
        status: 'open',
        lastManualStageChangeAt: null,
        version: 1,
        createdAt: new Date(),
        updatedAt: new Date(),
      }));
      await Opportunity.collection.insertMany(docs as any, { ordered: false });
    }

    // Insert 100 opportunities into stage-normal
    const normalDocs = Array.from({ length: 100 }).map((_, idx) => ({
      workspaceId: 'ws-oversized',
      name: `Normal Deal ${idx}`,
      monetaryValue: 1000,
      stageId: 'stage-normal',
      ownerId: 'rep-tester',
      status: 'open',
      lastManualStageChangeAt: null,
      version: 1,
      createdAt: new Date(),
      updatedAt: new Date(),
    }));
    await Opportunity.collection.insertMany(normalDocs as any, { ordered: false });
  }, 90000);

  afterAll(async () => {
    await teardownTestDB();
  });

  it('should gracefully reject requests exceeding 50,000 matching deals with HTTP 422', async () => {
    const res = await request(app)
      .post('/bulk-jobs')
      .set('X-Workspace-Id', 'ws-oversized')
      .send({
        filter: { stageId: 'stage-oversized' },
        targetStageId: 'stage-target',
      });

    expect(res.status).toBe(422);
    expect(res.body.error).toContain('exceeds maximum allowed limit of 50,000');
    expect(res.body.matchingCount).toBe(50005);
    expect(res.body.maxLimit).toBe(50000);

    // Verify NO job was queued in the database
    const jobs = await BulkJob.find({
      workspaceId: 'ws-oversized',
      'filter.stageId': 'stage-oversized',
    });
    expect(jobs.length).toBe(0);
  });

  it('should accept and queue requests with <= 50,000 matching deals', async () => {
    const res = await request(app)
      .post('/bulk-jobs')
      .set('X-Workspace-Id', 'ws-oversized')
      .send({
        filter: { stageId: 'stage-normal' },
        targetStageId: 'stage-target',
      });

    expect(res.status).toBe(201);
    expect(res.body.status).toBe('queued');
    expect(res.body.totalCount).toBe(100);
  });
});
