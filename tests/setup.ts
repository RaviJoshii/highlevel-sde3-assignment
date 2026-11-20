import { connectDB, disconnectDB } from '../src/config/db';
import { Opportunity } from '../src/models/Opportunity';
import { BulkJob } from '../src/models/BulkJob';
import { OpportunityStageTransition } from '../src/models/OpportunityStageTransition';
import { Workspace } from '../src/models/Workspace';

export async function setupTestDB() {
  await connectDB();
  await Promise.all([
    Opportunity.deleteMany({}),
    BulkJob.deleteMany({}),
    OpportunityStageTransition.deleteMany({}),
    Workspace.deleteMany({}),
  ]);
  // syncIndexes drops indexes left over from older schema versions (e.g. the non-partial idempotency index)
  await Promise.all([
    Opportunity.syncIndexes(),
    BulkJob.syncIndexes(),
    OpportunityStageTransition.syncIndexes(),
  ]);
}

export async function teardownTestDB() {
  await disconnectDB();
}

/** Bulk jobs validate stages against the workspace pipeline, so tests must register one. */
export async function registerWorkspace(workspaceId: string, stageIds: string[]) {
  await Workspace.updateOne(
    { _id: workspaceId },
    {
      $set: {
        name: workspaceId,
        stages: stageIds.map((stageId, i) => ({ stageId, name: stageId, order: i + 1 })),
      },
    },
    { upsert: true }
  );
}
