import os from 'os';
import { performance } from 'perf_hooks';
import { connectDB, disconnectDB } from '../config/db';
import { Opportunity } from '../models/Opportunity';
import { BulkJob } from '../models/BulkJob';
import { OpportunityStageTransition } from '../models/OpportunityStageTransition';
import { bulkJobService } from '../services/bulk-job.service';
import { WorkerEngine } from '../services/worker.service';
import { opportunityService } from '../services/opportunity.service';
import { config } from '../config';

interface LatencyStats {
  count: number;
  min: number;
  max: number;
  mean: number;
  p50: number;
  p95: number;
  p99: number;
}

function calculatePercentiles(latencies: number[]): LatencyStats {
  if (latencies.length === 0) {
    return { count: 0, min: 0, max: 0, mean: 0, p50: 0, p95: 0, p99: 0 };
  }
  const sorted = [...latencies].sort((a, b) => a - b);
  const sum = sorted.reduce((acc, v) => acc + v, 0);
  const p = (pct: number) => {
    const idx = Math.min(sorted.length - 1, Math.floor((pct / 100) * sorted.length));
    return parseFloat(sorted[idx].toFixed(2));
  };

  return {
    count: sorted.length,
    min: parseFloat(sorted[0].toFixed(2)),
    max: parseFloat(sorted[sorted.length - 1].toFixed(2)),
    mean: parseFloat((sum / sorted.length).toFixed(2)),
    p50: p(50),
    p95: p(95),
    p99: p(99),
  };
}

async function runBenchmarks() {
  console.log('================================================================');
  console.log('       SDE-3 BULK STAGE MOVE BENCHMARK & MEASUREMENT SUITE       ');
  console.log('================================================================');
  console.log(`Hardware: ${os.cpus()[0]?.model} (${os.cpus().length} vCPUs), ${(os.totalmem() / 1024 / 1024 / 1024).toFixed(1)} GB RAM`);
  console.log(`OS: ${os.type()} ${os.release()} (${os.arch()})`);
  console.log(`Node.js: ${process.version}`);
  console.log('================================================================\n');

  await connectDB();

  // 1. Prepare target 50,000 dataset for Benchmark 1
  console.log('[Setup] Preparing 50,000 test deals in stage-bench-1 for ws-large-1...');
  await Opportunity.deleteMany({ workspaceId: 'ws-large-1', stageId: { $in: ['stage-bench-1', 'stage-bench-target'] } });
  await OpportunityStageTransition.deleteMany({ workspaceId: 'ws-large-1' });
  await BulkJob.deleteMany({ workspaceId: 'ws-large-1' });

  const total50k = 50000;
  const seedBatchSize = 10000;
  for (let i = 0; i < total50k; i += seedBatchSize) {
    const batch = Array.from({ length: seedBatchSize }).map((_, idx) => ({
      workspaceId: 'ws-large-1',
      name: `Benchmark Deal ${i + idx + 1}`,
      monetaryValue: 15000 + (idx % 1000),
      stageId: 'stage-bench-1',
      ownerId: 'rep-alice',
      status: 'open',
      lastManualStageChangeAt: null,
      version: 1,
      createdAt: new Date(),
      updatedAt: new Date(),
    }));
    await Opportunity.collection.insertMany(batch as any, { ordered: false });
  }
  console.log('[Setup] 50,000 deals seeded successfully into stage-bench-1.\n');

  // Also seed small workspace for multi-tenant isolation benchmark
  console.log('[Setup] Preparing 2,500 test deals in ws-small-1...');
  await Opportunity.deleteMany({ workspaceId: 'ws-small-1' });
  const smallBatch = Array.from({ length: 2500 }).map((_, idx) => ({
    workspaceId: 'ws-small-1',
    name: `Small Workspace Deal ${idx + 1}`,
    monetaryValue: 5000,
    stageId: 'stage-bench-1',
    ownerId: 'rep-bob',
    status: 'open',
    lastManualStageChangeAt: null,
    version: 1,
    createdAt: new Date(),
    updatedAt: new Date(),
  }));
  await Opportunity.collection.insertMany(smallBatch as any, { ordered: false });
  console.log('[Setup] 2,500 deals seeded in ws-small-1.\n');

  // -------------------------------------------------------------
  // BENCHMARK 1 & 2: Bulk Move of 50,000 + Interactive Latency
  // -------------------------------------------------------------
  console.log('>>> RUNNING BENCHMARK 1 & 2: Bulk Move of 50,000 Deals with Concurrent Interactive Load');

  // Submit Bulk Job
  const createStart = performance.now();
  const { job } = await bulkJobService.createJob({
    workspaceId: 'ws-large-1',
    filter: { stageId: 'stage-bench-1' },
    targetStageId: 'stage-bench-target',
    idempotencyKey: 'bench-job-50k-run-1',
  });
  const createEnd = performance.now();
  console.log(`[API] Bulk job submitted in ${(createEnd - createStart).toFixed(1)}ms. Total matching count: ${job.totalCount}`);

  // Setup worker
  const worker = new WorkerEngine('benchmark-worker-1');
  const sameWsLatencies: number[] = [];
  const diffWsLatencies: number[] = [];

  let isBulkJobRunning = true;

  // Background interactive load generator
  const runInteractiveLoad = async () => {
    while (isBulkJobRunning) {
      // 1. Interactive request in SAME workspace (ws-large-1)
      const t1 = performance.now();
      await opportunityService.listOpportunities({
        workspaceId: 'ws-large-1',
        stageId: 'stage-bench-1',
        page: 1,
        limit: 20,
      });
      const t2 = performance.now();
      sameWsLatencies.push(t2 - t1);

      // 2. Interactive request in DIFFERENT workspace (ws-small-1)
      const t3 = performance.now();
      await opportunityService.listOpportunities({
        workspaceId: 'ws-small-1',
        stageId: 'stage-bench-1',
        page: 1,
        limit: 20,
      });
      const t4 = performance.now();
      diffWsLatencies.push(t4 - t3);

      // Brief pause between interactive queries
      await new Promise((r) => setTimeout(r, 20));
    }
  };

  const interactivePromise = runInteractiveLoad();

  // Execute the 50,000 bulk move
  const bulkStart = performance.now();
  await worker.processJob(job);
  const bulkEnd = performance.now();

  isBulkJobRunning = false;
  await interactivePromise;

  const totalWallClockSec = (bulkEnd - bulkStart) / 1000;
  const throughput = Math.round(50000 / totalWallClockSec);

  const completedJob = await bulkJobService.getJob('ws-large-1', job._id.toString());
  const sameStats = calculatePercentiles(sameWsLatencies);
  const diffStats = calculatePercentiles(diffWsLatencies);

  console.log('\n--- BENCHMARK 1 RESULTS: Bulk Move 50,000 ---');
  console.log(`Total Opportunities: ${completedJob?.totalCount.toLocaleString()}`);
  console.log(`Successfully Updated: ${completedJob?.updatedCount.toLocaleString()}`);
  console.log(`Skipped: ${completedJob?.skippedCount}`);
  console.log(`Total Wall-Clock Time: ${totalWallClockSec.toFixed(2)} seconds`);
  console.log(`Sustained Throughput: ${throughput.toLocaleString()} deals/second`);

  console.log('\n--- BENCHMARK 2 RESULTS: Interactive Request Latency During Bulk Run ---');
  console.log('Same Workspace (ws-large-1):');
  console.log(`  Requests: ${sameStats.count}`);
  console.log(`  Mean:     ${sameStats.mean} ms`);
  console.log(`  p50:      ${sameStats.p50} ms`);
  console.log(`  p95:      ${sameStats.p95} ms`);
  console.log(`  p99:      ${sameStats.p99} ms`);
  console.log(`  Max:      ${sameStats.max} ms`);

  console.log('Different Workspace (ws-small-1):');
  console.log(`  Requests: ${diffStats.count}`);
  console.log(`  Mean:     ${diffStats.mean} ms`);
  console.log(`  p50:      ${diffStats.p50} ms`);
  console.log(`  p95:      ${diffStats.p95} ms`);
  console.log(`  p99:      ${diffStats.p99} ms`);
  console.log(`  Max:      ${diffStats.max} ms`);

  // -------------------------------------------------------------
  // BENCHMARK 3: Kill and Resume
  // -------------------------------------------------------------
  console.log('\n>>> RUNNING BENCHMARK 3: Kill and Resume Resiliency');

  // Seed 20,000 deals in stage-kill-test
  console.log('[Setup] Seeding 20,000 deals for Kill & Resume test...');
  await Opportunity.deleteMany({ workspaceId: 'ws-large-1', stageId: { $in: ['stage-kill-source', 'stage-kill-dest'] } });
  const killBatch = Array.from({ length: 20000 }).map((_, idx) => ({
    workspaceId: 'ws-large-1',
    name: `Kill Test Deal ${idx + 1}`,
    monetaryValue: 25000,
    stageId: 'stage-kill-source',
    ownerId: 'rep-alice',
    status: 'open',
    lastManualStageChangeAt: null,
    version: 1,
    createdAt: new Date(),
    updatedAt: new Date(),
  }));
  await Opportunity.collection.insertMany(killBatch as any, { ordered: false });

  const { job: killJob } = await bulkJobService.createJob({
    workspaceId: 'ws-large-1',
    filter: { stageId: 'stage-kill-source' },
    targetStageId: 'stage-kill-dest',
    idempotencyKey: 'bench-kill-resume-job',
  });

  const killStart = performance.now();

  // Worker 1 starts and dies after 10,000 records
  const dyingWorker = new WorkerEngine('worker-will-be-killed');
  dyingWorker.crashAfterRecords = 10000;
  console.log('[Kill/Resume] Worker 1 started. Processing 10,000 records before simulated termination...');
  await dyingWorker.processJob(killJob);

  const killedState = await bulkJobService.getJob('ws-large-1', killJob._id.toString());
  console.log(`[Kill/Resume] Worker 1 terminated. Processed so far: ${killedState?.processedCount.toLocaleString()} / 20,000. Cursor saved: ${killedState?.cursor}`);

  // Simulating lease expiration for recovery worker
  await BulkJob.updateOne(
    { _id: killJob._id },
    { $set: { leaseExpiresAt: new Date(Date.now() - 1000) } }
  );

  // Worker 2 (Recovery Worker) claims and completes the job
  console.log('[Kill/Resume] Recovery Worker 2 started. Resuming job from committed cursor...');
  const recoveryWorker = new WorkerEngine('recovery-worker-2');
  const claimed = await recoveryWorker.claimNextJob();
  if (claimed) {
    await recoveryWorker.processJob(claimed);
  }

  const killEnd = performance.now();
  const killTotalSec = (killEnd - killStart) / 1000;

  const finalJob = await bulkJobService.getJob('ws-large-1', killJob._id.toString());
  const finalDestCount = await Opportunity.countDocuments({ workspaceId: 'ws-large-1', stageId: 'stage-kill-dest' });
  const finalTransitionCount = await OpportunityStageTransition.countDocuments({ jobId: killJob._id });

  console.log('\n--- BENCHMARK 3 RESULTS: Kill and Resume ---');
  console.log(`Total Deals:               ${finalJob?.totalCount.toLocaleString()}`);
  console.log(`Processed Count:           ${finalJob?.processedCount.toLocaleString()}`);
  console.log(`Updated Count:             ${finalJob?.updatedCount.toLocaleString()}`);
  console.log(`Deals in Target Stage:     ${finalDestCount.toLocaleString()}`);
  console.log(`Transition Records:        ${finalTransitionCount.toLocaleString()}`);
  console.log(`Total Wall-Clock Time:     ${killTotalSec.toFixed(2)} seconds`);
  console.log(`Proof of Correctness:      ${finalDestCount === 20000 && finalTransitionCount === 20000 ? 'VERIFIED 100% CORRECT (0 duplicate transitions)' : 'FAILED'}`);

  console.log('\n================================================================');
  console.log('                   BENCHMARK SUITE FINISHED                     ');
  console.log('================================================================');

  await disconnectDB();
}

if (require.main === module) {
  runBenchmarks().catch((err) => {
    console.error('Benchmark failed:', err);
    process.exit(1);
  });
}
