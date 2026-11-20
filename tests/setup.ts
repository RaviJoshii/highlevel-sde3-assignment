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
  await Promise.all([
    Opportunity.createIndexes(),
    BulkJob.createIndexes(),
    OpportunityStageTransition.createIndexes(),
  ]);
}

export async function teardownTestDB() {
  await disconnectDB();
}
