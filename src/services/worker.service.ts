import crypto from 'crypto';
import { Types } from 'mongoose';
import { BulkJob, IBulkJob } from '../models/BulkJob';
import { Opportunity } from '../models/Opportunity';
import { OpportunityStageTransition } from '../models/OpportunityStageTransition';
import { bulkJobService } from './bulk-job.service';
import { config } from '../config';

export class WorkerEngine {
  public workerId: string;
  private isRunning: boolean = false;
  private pollTimer: NodeJS.Timeout | null = null;
  private activeJobs: Set<string> = new Set();

  // Test hook to simulate crash / abrupt termination after processing N records
  public crashAfterRecords: number | null = null;
  public onChunkProcessed?: (job: IBulkJob, chunkCount: number) => void;

  constructor(workerId?: string) {
    this.workerId = workerId || `worker-${crypto.randomBytes(4).toString('hex')}`;
  }

  start(intervalMs: number = 1000): void {
    if (this.isRunning) return;
    this.isRunning = true;
    this.pollLoop(intervalMs);
  }

  stop(): void {
    this.isRunning = false;
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
  }

  private pollLoop(intervalMs: number): void {
    if (!this.isRunning) return;

    this.processNextJob()
      .catch((err) => {
        console.error(`[Worker ${this.workerId}] Error in job execution loop:`, err);
      })
      .finally(() => {
        if (this.isRunning) {
          this.pollTimer = setTimeout(() => this.pollLoop(intervalMs), intervalMs);
        }
      });
  }

  /**
   * Finds and atomically acquires the next eligible job.
   * Handles:
   * 1. Newly queued jobs ('queued')
   * 2. Interrupted/crashed jobs ('processing' with expired lease) -> Crash Recovery!
   * 3. Tenant fairness: skips jobs for workspaces already running a job on this worker
   */
  async claimNextJob(): Promise<IBulkJob | null> {
    const now = new Date();

    // Query candidate jobs
    const candidates = await BulkJob.find({
      $or: [
        { status: 'queued' },
        { status: 'processing', leaseExpiresAt: { $lt: now } }, // Crashed worker recovery
      ],
    })
      .sort({ createdAt: 1 })
      .limit(10)
      .lean();

    for (const candidate of candidates) {
      // Per-workspace concurrency isolation: don't process multiple concurrent bulk jobs for the same tenant
      if (this.activeJobs.has(candidate.workspaceId)) {
        continue;
      }

      // Check if another worker is already processing a job for this workspace
      const otherActiveForWorkspace = await BulkJob.findOne({
        workspaceId: candidate.workspaceId,
        status: 'processing',
        leaseExpiresAt: { $gte: now },
        _id: { $ne: candidate._id },
      });

      if (otherActiveForWorkspace) {
        continue;
      }

      // Atomically claim lease on this job
      const claimedJob = await BulkJob.findOneAndUpdate(
        {
          _id: candidate._id,
          $or: [
            { status: 'queued' },
            { status: 'processing', leaseExpiresAt: { $lt: now } },
          ],
        },
        {
          $set: {
            status: 'processing',
            workerId: this.workerId,
            leaseExpiresAt: new Date(Date.now() + config.leaseDurationMs),
            lastHeartbeatAt: now,
            startedAt: candidate.startedAt || now,
          },
        },
        { new: true }
      );

      if (claimedJob) {
        return claimedJob;
      }
    }

    return null;
  }

  /**
   * Main job processing routine.
   */
  async processJob(job: IBulkJob): Promise<void> {
    const jobIdStr = job._id.toString();
    this.activeJobs.add(job.workspaceId);

    // Heartbeat timer to periodically renew lease
    const heartbeatTimer = setInterval(async () => {
      try {
        await BulkJob.updateOne(
          { _id: job._id, workerId: this.workerId, status: 'processing' },
          {
            $set: {
              lastHeartbeatAt: new Date(),
              leaseExpiresAt: new Date(Date.now() + config.leaseDurationMs),
            },
          }
        );
      } catch (hbErr) {
        console.error(`[Worker ${this.workerId}] Failed to renew lease for job ${jobIdStr}:`, hbErr);
      }
    }, config.heartbeatIntervalMs);

    try {
      await this.executeJobChunks(job);
    } catch (err: any) {
      if (err.message === 'SIMULATED_WORKER_CRASH') {
        console.warn(`[Worker ${this.workerId}] Simulated worker crash triggered on job ${jobIdStr}`);
        // Worker was "killed": lease will naturally expire and another worker/process will recover it
        return;
      }

      console.error(`[Worker ${this.workerId}] Job ${jobIdStr} failed with unrecoverable error:`, err);
      await BulkJob.updateOne(
        { _id: job._id },
        {
          $set: {
            status: 'failed',
            failedAt: new Date(),
            errorMessage: err.message || 'Unknown processing error',
            leaseExpiresAt: null,
          },
        }
      );
    } finally {
      clearInterval(heartbeatTimer);
      this.activeJobs.delete(job.workspaceId);
    }
  }

  /**
   * Chunks through matching opportunities using high-efficiency keyset pagination (_id > cursor).
   */
  private async executeJobChunks(job: IBulkJob): Promise<void> {
    let currentCursor: Types.ObjectId | null = job.cursor || null;
    let recordsProcessedThisSession = 0;

    const baseFilter = bulkJobService.buildFilterQuery(job.workspaceId, job.filter);

    while (true) {
      // Build keyset pagination query
      const chunkQuery: any = { ...baseFilter };

      if (currentCursor) {
        chunkQuery._id = { $gt: currentCursor };
      }

      // Respect snapshot upper bound
      if (job.snapshotMaxId) {
        chunkQuery._id = {
          ...(chunkQuery._id || {}),
          $lte: job.snapshotMaxId,
        };
      }

      // Fetch chunk using covered index: { workspaceId: 1, stageId: 1, _id: 1 }
      const opportunities = await Opportunity.find(chunkQuery)
        .sort({ _id: 1 })
        .limit(config.chunkSize)
        .lean();

      if (opportunities.length === 0) {
        // No more records matching snapshot in this stage -> Job complete!
        // Reconcile any records from initial snapshot that left the filter set
        const currentJobState = await BulkJob.findById(job._id).lean();
        const updated = currentJobState?.updatedCount || 0;
        const currentSkipped = currentJobState?.skippedCount || 0;
        const total = currentJobState?.totalCount || 0;

        const unaccounted = Math.max(0, total - (updated + currentSkipped));
        const finalSkipped = currentSkipped + unaccounted;
        const finalProcessed = Math.max(total, updated + finalSkipped);

        await BulkJob.updateOne(
          { _id: job._id },
          {
            $set: {
              status: 'completed',
              completedAt: new Date(),
              leaseExpiresAt: null,
              skippedCount: finalSkipped,
              processedCount: finalProcessed,
            },
          }
        );
        break;
      }

      const bulkOps: any[] = [];
      const transitionDocs: any[] = [];
      let chunkSkippedCount = 0;
      const now = new Date();

      for (const opp of opportunities) {
        // REQUIREMENT 1: Manual Edit Handling
        // Preserves manual stage changes and skips deals manually moved, even if moved A->B->A.
        const wasManuallyEdited =
          opp.lastManualStageChangeAt &&
          new Date(opp.lastManualStageChangeAt).getTime() >
            new Date(job.snapshotCreatedAt).getTime();

        if (wasManuallyEdited) {
          chunkSkippedCount++;
          continue;
        }

        // REQUIREMENT 2: Target Stage Validation
        // Avoids unnecessary updates by skipping deals already in the target stage.
        if (opp.stageId === job.targetStageId) {
          chunkSkippedCount++;
          continue;
        }

        // Prepare atomic update with collision guard
        bulkOps.push({
          updateOne: {
            filter: {
              _id: opp._id,
              workspaceId: job.workspaceId,
              stageId: opp.stageId,
              version: opp.version,
              // Ensure no concurrent manual move slipped in since reading this chunk
              $or: [
                { lastManualStageChangeAt: opp.lastManualStageChangeAt || null },
                { lastManualStageChangeAt: { $exists: false } },
              ],
            },
            update: {
              $set: {
                stageId: job.targetStageId,
                updatedAt: now,
              },
              $inc: { version: 1 },
            },
          },
        });

        transitionDocs.push({
          workspaceId: job.workspaceId,
          opportunityId: opp._id,
          fromStageId: opp.stageId,
          toStageId: job.targetStageId,
          triggeredBy: 'bulk_job',
          jobId: job._id,
          createdAt: now,
        });
      }

      let chunkUpdatedCount = 0;

      if (bulkOps.length > 0) {
        const bulkResult = await Opportunity.bulkWrite(bulkOps, { ordered: false });
        chunkUpdatedCount = bulkResult.modifiedCount || 0;

        // If any records were modified concurrently during chunk execution, adjust skipped count
        const missedConcurrently = bulkOps.length - chunkUpdatedCount;
        if (missedConcurrently > 0) {
          chunkSkippedCount += missedConcurrently;
        }

        // Only record transitions for opportunities that actually updated
        // For precision under concurrent conflict:
        if (chunkUpdatedCount > 0) {
          // If all updated, insert all transitions
          if (chunkUpdatedCount === bulkOps.length) {
            await OpportunityStageTransition.insertMany(transitionDocs, { ordered: false });
          } else {
            // Partial chunk conflict: insert transitions only for verified updated IDs
            const successfullyUpdatedOpps = await Opportunity.find({
              _id: { $in: bulkOps.map((op) => op.updateOne.filter._id) },
              stageId: job.targetStageId,
            })
              .select('_id')
              .lean();

            const updatedIdSet = new Set(
              successfullyUpdatedOpps.map((o) => o._id.toString())
            );

            const verifiedTransitions = transitionDocs.filter((t) =>
              updatedIdSet.has(t.opportunityId.toString())
            );

            if (verifiedTransitions.length > 0) {
              await OpportunityStageTransition.insertMany(verifiedTransitions, {
                ordered: false,
              });
            }
          }
        }
      }

      const lastOppIdInChunk = opportunities[opportunities.length - 1]._id as Types.ObjectId;
      currentCursor = lastOppIdInChunk;
      recordsProcessedThisSession += opportunities.length;

      // ATOMIC PROGRESS COMMIT: Real committed state survives restarts
      const updatedJobDoc = await BulkJob.findOneAndUpdate(
        { _id: job._id },
        {
          $set: {
            cursor: lastOppIdInChunk,
            lastHeartbeatAt: new Date(),
            leaseExpiresAt: new Date(Date.now() + config.leaseDurationMs),
          },
          $inc: {
            processedCount: opportunities.length,
            updatedCount: chunkUpdatedCount,
            skippedCount: chunkSkippedCount,
          },
        },
        { new: true }
      );

      if (this.onChunkProcessed && updatedJobDoc) {
        this.onChunkProcessed(updatedJobDoc, opportunities.length);
      }

      // Check simulated crash hook for testing resumability
      if (
        this.crashAfterRecords !== null &&
        recordsProcessedThisSession >= this.crashAfterRecords
      ) {
        throw new Error('SIMULATED_WORKER_CRASH');
      }

      // REQUIREMENT 5: Well-behaved pacing delay
      // Yields CPU & database I/O to interactive user requests
      if (config.chunkPacingMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, config.chunkPacingMs));
      }
    }
  }

  /**
   * One iteration of claiming and executing a job.
   */
  async processNextJob(): Promise<boolean> {
    const job = await this.claimNextJob();
    if (!job) {
      return false;
    }
    await this.processJob(job);
    return true;
  }
}

export const defaultWorker = new WorkerEngine();
