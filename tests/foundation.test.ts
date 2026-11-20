import request from 'supertest';
import { createApp } from '../src/app';
import { setupTestDB, teardownTestDB } from './setup';
import { Opportunity } from '../src/models/Opportunity';
import { OpportunityStageTransition } from '../src/models/OpportunityStageTransition';

const app = createApp();

describe('Part 1 - Foundation APIs', () => {
  beforeAll(async () => {
    await setupTestDB();
  });

  afterAll(async () => {
    await teardownTestDB();
  });

  describe('Tenant Scoping & Authentication', () => {
    it('should reject requests without X-Workspace-Id header', async () => {
      const res = await request(app).get('/opportunities?stage=stage-1');
      expect(res.status).toBe(400);
      expect(res.body.error).toContain('X-Workspace-Id header is required');
    });
  });

  describe('Create Opportunity', () => {
    it('should create an opportunity within workspace', async () => {
      const res = await request(app)
        .post('/opportunities')
        .set('X-Workspace-Id', 'ws-test-1')
        .send({
          name: 'Acme Mega Deal',
          monetaryValue: 50000,
          stageId: 'stage-1',
          ownerId: 'rep-alice',
        });

      expect(res.status).toBe(201);
      expect(res.body._id).toBeDefined();
      expect(res.body.workspaceId).toBe('ws-test-1');
      expect(res.body.stageId).toBe('stage-1');
      expect(res.body.version).toBe(1);
    });

    it('should fail when required fields are missing', async () => {
      const res = await request(app)
        .post('/opportunities')
        .set('X-Workspace-Id', 'ws-test-1')
        .send({
          name: 'Incomplete Deal',
        });

      expect(res.status).toBe(400);
    });
  });

  describe('Move Single Opportunity Stage', () => {
    it('should move stage and record transition audit trail', async () => {
      // 1. Create opportunity
      const opp = await Opportunity.create({
        workspaceId: 'ws-test-1',
        name: 'Deal to Move',
        monetaryValue: 10000,
        stageId: 'stage-1',
        status: 'open',
        ownerId: 'rep-bob',
        version: 1,
      });

      // 2. Move stage
      const res = await request(app)
        .patch(`/opportunities/${opp._id}/stage`)
        .set('X-Workspace-Id', 'ws-test-1')
        .send({ toStageId: 'stage-2' });

      expect(res.status).toBe(200);
      expect(res.body.opportunity.stageId).toBe('stage-2');
      expect(res.body.opportunity.version).toBe(2);
      expect(res.body.opportunity.lastManualStageChangeAt).toBeDefined();

      // 3. Verify transition was recorded
      const transitions = await OpportunityStageTransition.find({
        opportunityId: opp._id,
      });
      expect(transitions.length).toBe(1);
      expect(transitions[0].fromStageId).toBe('stage-1');
      expect(transitions[0].toStageId).toBe('stage-2');
      expect(transitions[0].triggeredBy).toBe('manual');
    });

    it('should skip update if opportunity is already in target stage', async () => {
      const opp = await Opportunity.create({
        workspaceId: 'ws-test-1',
        name: 'Already In Target',
        monetaryValue: 10000,
        stageId: 'stage-3',
        status: 'open',
        ownerId: 'rep-bob',
        version: 1,
      });

      const res = await request(app)
        .patch(`/opportunities/${opp._id}/stage`)
        .set('X-Workspace-Id', 'ws-test-1')
        .send({ toStageId: 'stage-3' });

      expect(res.status).toBe(200);
      expect(res.body.skipped).toBe(true);
      expect(res.body.reason).toContain('Already in target stage');

      // Verify no transition was recorded
      const transitions = await OpportunityStageTransition.find({
        opportunityId: opp._id,
      });
      expect(transitions.length).toBe(0);
    });
  });

  describe('List Opportunities with Pagination', () => {
    it('should return paginated list of opportunities for a stage', async () => {
      // Create 5 opportunities in stage-4
      for (let i = 1; i <= 5; i++) {
        await Opportunity.create({
          workspaceId: 'ws-test-1',
          name: `Paginated Deal ${i}`,
          monetaryValue: i * 1000,
          stageId: 'stage-4',
          status: 'open',
          ownerId: 'rep-alice',
        });
      }

      const res = await request(app)
        .get('/opportunities?stage=stage-4&page=1&limit=3')
        .set('X-Workspace-Id', 'ws-test-1');

      expect(res.status).toBe(200);
      expect(res.body.data.length).toBe(3);
      expect(res.body.total).toBe(5);
      expect(res.body.totalPages).toBe(2);
      expect(res.body.page).toBe(1);
    });
  });
});
