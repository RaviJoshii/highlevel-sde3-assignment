import { Types } from 'mongoose';
import { connectDB, disconnectDB } from '../config/db';
import { Workspace } from '../models/Workspace';
import { Opportunity, IOpportunity, OpportunityStatus } from '../models/Opportunity';
import { OpportunityStageTransition } from '../models/OpportunityStageTransition';
import { BulkJob } from '../models/BulkJob';

const STAGES = [
  { stageId: 'stage-1', name: 'New Lead', order: 1, weight: 0.22 }, // ~110,000
  { stageId: 'stage-2', name: 'Contacted', order: 2, weight: 0.16 }, // ~80,000
  { stageId: 'stage-3', name: 'Engaged', order: 3, weight: 0.12 }, // ~60,000
  { stageId: 'stage-4', name: 'Qualified', order: 4, weight: 0.10 }, // ~50,000
  { stageId: 'stage-5', name: 'Demo Scheduled', order: 5, weight: 0.08 }, // ~40,000
  { stageId: 'stage-6', name: 'Proposal Sent', order: 6, weight: 0.08 }, // ~40,000
  { stageId: 'stage-7', name: 'In Review', order: 7, weight: 0.06 }, // ~30,000
  { stageId: 'stage-8', name: 'Negotiation', order: 8, weight: 0.05 }, // ~25,000
  { stageId: 'stage-9', name: 'Contract Sent', order: 9, weight: 0.04 }, // ~20,000
  { stageId: 'stage-10', name: 'Closing', order: 10, weight: 0.03 }, // ~15,000
  { stageId: 'stage-11', name: 'Closed Won', order: 11, weight: 0.03 }, // ~15,000
  { stageId: 'stage-12', name: 'Closed Lost', order: 12, weight: 0.03 }, // ~15,000
];

const OWNERS = ['rep-alice', 'rep-bob', 'rep-charlie', 'rep-david', 'rep-eva'];
const STATUSES: OpportunityStatus[] = ['open', 'won', 'lost', 'abandoned'];

function getWeightedStage(): string {
  const rand = Math.random();
  let cumulative = 0;
  for (const s of STAGES) {
    cumulative += s.weight;
    if (rand <= cumulative) return s.stageId;
  }
  return STAGES[0].stageId;
}

export async function seedDatabase(options?: { largeCount?: number; smallCount?: number }) {
  const largeCount = options?.largeCount ?? 500000;
  const smallCount = options?.smallCount ?? 2500;

  console.log(`[Seed] Starting seed process...`);
  console.log(`[Seed] Large workspace target: ${largeCount.toLocaleString()} deals`);
  console.log(`[Seed] 5 small workspaces target: ${smallCount.toLocaleString()} deals each`);

  await connectDB();

  // Clean collections
  console.log('[Seed] Cleaning old records...');
  await Promise.all([
    Workspace.deleteMany({}),
    Opportunity.deleteMany({}),
    OpportunityStageTransition.deleteMany({}),
    BulkJob.deleteMany({}),
  ]);

  // Ensure indexes are built
  console.log('[Seed] Building indexes...');
  await Promise.all([
    Opportunity.syncIndexes(),
    BulkJob.syncIndexes(),
    OpportunityStageTransition.syncIndexes(),
  ]);

  // 1. Create Workspaces
  console.log('[Seed] Creating workspace metadata...');
  const workspaces = [
    {
      _id: 'ws-large-1',
      name: 'Enterprise Global Corp',
      stages: STAGES.map(({ stageId, name, order }) => ({ stageId, name, order })),
    },
    ...[1, 2, 3, 4, 5].map((i) => ({
      _id: `ws-small-${i}`,
      name: `Startup Ventures ${i}`,
      stages: STAGES.map(({ stageId, name, order }) => ({ stageId, name, order })),
    })),
  ];

  await Workspace.insertMany(workspaces);

  // Time window: spread over ~18 months (~548 days)
  const nowMs = Date.now();
  const eighteenMonthsMs = 548 * 24 * 60 * 60 * 1000;

  // 2. Seed Large Workspace in chunks
  console.log(`[Seed] Generating ${largeCount.toLocaleString()} deals for ws-large-1...`);
  const batchSize = 10000;
  let seededSoFar = 0;
  const startTime = Date.now();

  while (seededSoFar < largeCount) {
    const currentBatchSize = Math.min(batchSize, largeCount - seededSoFar);
    const batch: Partial<IOpportunity>[] = [];

    for (let i = 0; i < currentBatchSize; i++) {
      const globalIndex = seededSoFar + i + 1;
      const createdMs = nowMs - Math.floor(Math.random() * eighteenMonthsMs);
      const createdAt = new Date(createdMs);
      const stageId = getWeightedStage();

      batch.push({
        workspaceId: 'ws-large-1',
        name: `Deal #${globalIndex} - Acme Enterprise Solution`,
        monetaryValue: Math.floor(Math.random() * 95000) + 5000,
        stageId,
        status: stageId === 'stage-11' ? 'won' : stageId === 'stage-12' ? 'lost' : 'open',
        ownerId: OWNERS[Math.floor(Math.random() * OWNERS.length)],
        lastManualStageChangeAt: null,
        version: 1,
        createdAt,
        updatedAt: createdAt,
      });
    }

    await Opportunity.collection.insertMany(batch as any, { ordered: false });
    seededSoFar += currentBatchSize;

    if (seededSoFar % 50000 === 0 || seededSoFar === largeCount) {
      const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(1);
      const rate = Math.round(seededSoFar / Math.max(0.1, (Date.now() - startTime) / 1000));
      console.log(
        `[Seed] ws-large-1: ${seededSoFar.toLocaleString()} / ${largeCount.toLocaleString()} deals (${elapsedSec}s, ${rate.toLocaleString()} deals/sec)`
      );
    }
  }

  // 3. Seed 5 Small Workspaces
  console.log('[Seed] Generating data for 5 small workspaces...');
  for (let w = 1; w <= 5; w++) {
    const wsId = `ws-small-${w}`;
    const batch: Partial<IOpportunity>[] = [];

    for (let i = 0; i < smallCount; i++) {
      const createdMs = nowMs - Math.floor(Math.random() * eighteenMonthsMs);
      const createdAt = new Date(createdMs);
      const stageId = getWeightedStage();

      batch.push({
        workspaceId: wsId,
        name: `${wsId} - Deal #${i + 1}`,
        monetaryValue: Math.floor(Math.random() * 20000) + 1000,
        stageId,
        status: stageId === 'stage-11' ? 'won' : stageId === 'stage-12' ? 'lost' : 'open',
        ownerId: OWNERS[Math.floor(Math.random() * OWNERS.length)],
        lastManualStageChangeAt: null,
        version: 1,
        createdAt,
        updatedAt: createdAt,
      });
    }

    await Opportunity.collection.insertMany(batch as any, { ordered: false });
    console.log(`[Seed] ${wsId}: ${smallCount.toLocaleString()} deals seeded.`);
  }

  const totalTimeSec = ((Date.now() - startTime) / 1000).toFixed(1);
  const totalCount = await Opportunity.countDocuments();
  console.log(
    `[Seed] SUCCESS! Total opportunities in DB: ${totalCount.toLocaleString()} in ${totalTimeSec} seconds.`
  );
}

if (require.main === module) {
  seedDatabase()
    .then(() => disconnectDB())
    .catch((err) => {
      console.error('[Seed] Failed:', err);
      process.exit(1);
    });
}
