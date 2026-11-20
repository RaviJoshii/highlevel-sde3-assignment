import crypto from 'crypto';
import { Types } from 'mongoose';
import { BulkJob, IBulkJob } from '../models/BulkJob';
import { Opportunity } from '../models/Opportunity';
import { OpportunityStageTransition } from '../models/OpportunityStageTransition';
import { bulkJobService } from './bulk-job.service';
import { config } from '../config';

/** Thrown when this worker no longer owns the job's lease (another worker took over). */
class LeaseLostError extends Error {
  constructor() {
    super('LEASE_LOST');
  }
}

export type CrashPoint = 'after_bulk_write' | 'after_transitions';

export class WorkerEngine {
  public workerId: string;
  private isRunning: boolean = false;
  private pollTimer: NodeJS.Timeout | null = null;

  // Test hooks to simulate crash / abrupt termination
  public crashAfterRecords: number | null = null;
  public crashAt: CrashPoint | null = null;
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
   * 1. Newly queued jobs, and retried jobs whose backoff has elapsed
   * 2. Crashed jobs ('processing' with expired lease) -> crash recovery
   * 3. Tenant isolation: the partial unique index `one_processing_job_per_workspace`
   *    makes the "one running job per workspace" cap atomic across all workers
   */
  async claimNextJob(): Promise<IBulkJob | null> {
    const now = new Date();

    // Workspaces with a live job are excluded in the query so a backlog from one
    // tenant can never fill the candidate window and hide other tenants' jobs.
    const busyWorkspaces = await BulkJob.distinct('workspaceId', {
      status: 'processing',
      leaseExpiresAt: { $gte: now },
    });

    const candidates = await BulkJob.find({
      workspaceId: { $nin: busyWorkspaces },
      $or: [
        { status: 'queued', $or: [{ nextAttemptAt: null }, { nextAttemptAt: { $lte: now } }] },
        { status: 'processing', leaseExpiresAt: { $lt: now } },
      ],
    })
      .sort({ createdAt: 1, _id: 1 })
      .limit(10)
      .lean();

    const blockedWorkspaces = new Set<string>();

    for (const candidate of candidates) {
      if (blockedWorkspaces.has(candidate.workspaceId)) continue;

      const isRecovery = candidate.status === 'processing';

      // A job that keeps killing its workers must not be recovered forever
      if (isRecovery && (candidate.attempts || 0) >= config.maxJobAttempts) {
        await BulkJob.updateOne(
          { _id: candidate._id, status: 'processing', leaseExpiresAt: { $lt: now } },
          {
            $set: {
              status: 'failed',
              active: false,
              failedAt: now,
              leaseExpiresAt: null,
              errorMessage: 'Exceeded max attempts: worker repeatedly failed while processing this job',
            },
          }
        );
        continue;
      }

      try {
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
              nextAttemptAt: null,
            },
            ...(isRecovery ? { $inc: { attempts: 1 } } : {}),
          },
          { new: true }
        );

        if (claimedJob) return claimedJob;
      } catch (err: any) {
        // Another job for this workspace is already 'processing' (unique partial index)
        if (err.code === 11000) {
          blockedWorkspaces.add(candidate.workspaceId);
          continue;
        }
        throw err;
      }
    }

    return null;
  }

  /**
   * Main job processing routine.
   */
  async processJob(job: IBulkJob): Promise<void> {
    const jobIdStr = job._id.toString();

    // Heartbeat timer to periodically renew lease
    const heartbeatTimer = setInterval(async () => {
      try {
        await BulkJob.updateOne(this.ownerFilter(job), {
          $set: {
            lastHeartbeatAt: new Date(),
            leaseExpiresAt: new Date(Date.now() + config.leaseDurationMs),
          },
        });
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

      if (err instanceof LeaseLostError) {
        console.warn(`[Worker ${this.workerId}] Lost lease on job ${jobIdStr}; abandoning it`);
        return;
      }

      await this.handleFailure(job, err);
    } finally {
      clearInterval(heartbeatTimer);
    }
  }

  /**
   * Retry with exponential backoff + jitter. Progress (cursor/counters) is kept, so a retry
   * resumes where the failed attempt stopped. After maxJobAttempts consecutive failures
   * without progress the job becomes 'failed' instead of retrying forever.
   */
  private async handleFailure(job: IBulkJob, err: any): Promise<void> {
    const message = err?.message || 'Unknown processing error';
    console.error(`[Worker ${this.workerId}] Job ${job._id} attempt failed:`, err);

    try {
      const counted = await BulkJob.findOneAndUpdate(
        this.ownerFilter(job),
        { $inc: { attempts: 1 }, $set: { errorMessage: message } },
        { new: true }
      );
      if (!counted) return; // lost the lease; the new owner decides

      if (counted.attempts >= config.maxJobAttempts) {
        await BulkJob.updateOne(this.ownerFilter(job), {
          $set: {
            status: 'failed',
            active: false,
            failedAt: new Date(),
            leaseExpiresAt: null,
          },
        });
        return;
      }

      const backoff = config.retryBaseDelayMs * 2 ** (counted.attempts - 1);
      const delay = backoff + Math.floor(Math.random() * (backoff / 2 + 1));
      await BulkJob.updateOne(this.ownerFilter(job), {
        $set: {
          status: 'queued',
          workerId: null,
          leaseExpiresAt: null,
          nextAttemptAt: new Date(Date.now() + delay),
        },
      });
    } catch (dbErr) {
      // Database unreachable: leave the job 'processing'. Its lease expires and it is recovered.
      console.error(`[Worker ${this.workerId}] Could not record failure for job ${job._id}:`, dbErr);
    }
  }

  private ownerFilter(job: IBulkJob) {
    return { _id: job._id, workerId: this.workerId, status: 'processing' as const };
  }

  /** Every progress write is fenced on lease ownership so a stale worker cannot corrupt state. */
  private async commit(job: IBulkJob, update: any): Promise<IBulkJob> {
    const updated = await BulkJob.findOneAndUpdate(this.ownerFilter(job), update, { new: true });
    if (!updated) throw new LeaseLostError();
    return updated;
  }

  /**
   * Records transitions. The unique {jobId, opportunityId} index makes this replay-safe:
   * duplicate-key errors from an earlier (crashed) attempt are ignored.
   */
  private async recordTransitions(job: IBulkJob, opportunityIds: Types.ObjectId[], at: Date) {
    if (opportunityIds.length === 0) return;

    const docs = opportunityIds.map((opportunityId) => ({
      workspaceId: job.workspaceId,
      opportunityId,
      fromStageId: job.filter.stageId,
      toStageId: job.targetStageId,
      triggeredBy: 'bulk_job',
      jobId: job._id,
      createdAt: at,
    }));

    try {
      await OpportunityStageTransition.collection.insertMany(docs, { ordered: false });
    } catch (err: any) {
      const writeErrors: any[] = err.writeErrors || [];
      const onlyDuplicates =
        writeErrors.length > 0 && writeErrors.every((e) => (e.code ?? e.err?.code) === 11000);
      if (!onlyDuplicates) throw err;
    }
  }

  /**
   * Crash repair. Deals are stamped with lastBulkJobId in the same write that moves them, so any
   * deal stamped by this job beyond the committed cursor was moved by a chunk whose transitions /
   * counters were never committed. Backfill those and re-derive the counters from the truth.
   */
  private async repairUncommittedChunk(job: IBulkJob, cursor: Types.ObjectId | null): Promise<void> {
    const query: any = { workspaceId: job.workspaceId, lastBulkJobId: job._id };
    if (cursor) query._id = { $gt: cursor };

    const orphans = await Opportunity.find(query).select('_id').lean();
    if (orphans.length === 0) return;

    await this.recordTransitions(job, orphans.map((o) => o._id as Types.ObjectId), new Date());

    const updatedCount = await OpportunityStageTransition.countDocuments({ jobId: job._id });
    await this.commit(job, {
      $set: {
        updatedCount,
        processedCount: updatedCount + (job.skippedCount || 0),
      },
    });
  }

  /**
   * Chunks through matching opportunities using high-efficiency keyset pagination (_id > cursor).
   */
  private async executeJobChunks(job: IBulkJob): Promise<void> {
    let currentCursor: Types.ObjectId | null = job.cursor || null;
    let recordsProcessedThisSession = 0;

    const baseFilter = bulkJobService.buildFilterQuery(job.workspaceId, job.filter);

    if (job.snapshotMaxId) {
      await this.repairUncommittedChunk(job, currentCursor);
    }

    while (true) {
      // A job whose snapshot matched nothing has no upper bound: never scan the live set
      const opportunities = job.snapshotMaxId
        ? await Opportunity.find({
            ...baseFilter,
            _id: {
              ...(currentCursor ? { $gt: currentCursor } : {}),
              $lte: job.snapshotMaxId,
            },
          })
            .sort({ _id: 1 })
            .limit(config.chunkSize)
            .lean()
        : [];

      if (opportunities.length === 0) {
        await this.completeJob(job);
        return;
      }

      const bulkOps: any[] = [];
      const now = new Date();

      for (const opp of opportunities) {
        // Manual edit handling: a deal touched by a user after submission is never overwritten,
        // even if it was moved A -> B -> A and therefore matches the filter again.
        const wasManuallyEdited =
          opp.lastManualStageChangeAt &&
          new Date(opp.lastManualStageChangeAt).getTime() >
            new Date(job.snapshotCreatedAt).getTime();

        if (wasManuallyEdited) continue;

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
                lastBulkJobId: job._id,
                updatedAt: now,
              },
              $inc: { version: 1 },
            },
          },
        });
      }

      let movedIds: Types.ObjectId[] = [];

      if (bulkOps.length > 0) {
        await Opportunity.bulkWrite(bulkOps, { ordered: false });

        if (this.crashAt === 'after_bulk_write') throw new Error('SIMULATED_WORKER_CRASH');

        // The stamp (not stageId) identifies what *this job* moved: a deal a user concurrently
        // moved into the target stage carries no stamp and gets no bulk_job transition.
        const moved = await Opportunity.find({
          _id: { $in: bulkOps.map((op) => op.updateOne.filter._id) },
          workspaceId: job.workspaceId,
          lastBulkJobId: job._id,
        })
          .select('_id')
          .lean();
        movedIds = moved.map((m) => m._id as Types.ObjectId);

        await this.recordTransitions(job, movedIds, now);

        if (this.crashAt === 'after_transitions') throw new Error('SIMULATED_WORKER_CRASH');
      }

      const chunkUpdatedCount = movedIds.length;
      const chunkSkippedCount = opportunities.length - chunkUpdatedCount;
      const lastOppIdInChunk = opportunities[opportunities.length - 1]._id as Types.ObjectId;
      currentCursor = lastOppIdInChunk;
      recordsProcessedThisSession += opportunities.length;

      // Atomic progress commit (cursor + counters together). A successful chunk also clears the
      // consecutive-failure counter.
      const updatedJobDoc = await this.commit(job, {
        $set: {
          cursor: lastOppIdInChunk,
          lastHeartbeatAt: new Date(),
          leaseExpiresAt: new Date(Date.now() + config.leaseDurationMs),
          attempts: 0,
        },
        $inc: {
          processedCount: opportunities.length,
          updatedCount: chunkUpdatedCount,
          skippedCount: chunkSkippedCount,
        },
      });

      if (this.onChunkProcessed) {
        this.onChunkProcessed(updatedJobDoc, opportunities.length);
      }

      if (
        this.crashAfterRecords !== null &&
        recordsProcessedThisSession >= this.crashAfterRecords
      ) {
        throw new Error('SIMULATED_WORKER_CRASH');
      }

      // Chunk pacing: yields CPU & database I/O to interactive requests
      if (config.chunkPacingMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, config.chunkPacingMs));
      }
    }
  }

  /**
   * Final counters come from committed data (transitions), not from in-memory or incremental
   * state. Snapshot members that left the filter set (e.g. a user moved them) count as skipped.
   */
  private async completeJob(job: IBulkJob): Promise<void> {
    const updated = await OpportunityStageTransition.countDocuments({ jobId: job._id });
    const current = await BulkJob.findOne(this.ownerFilter(job)).lean();
    if (!current) throw new LeaseLostError();

    const total = current.totalCount || 0;
    const unaccounted = Math.max(0, total - (updated + (current.skippedCount || 0)));
    const finalSkipped = (current.skippedCount || 0) + unaccounted;

    await this.commit(job, {
      $set: {
        status: 'completed',
        active: false,
        completedAt: new Date(),
        leaseExpiresAt: null,
        updatedCount: updated,
        skippedCount: finalSkipped,
        processedCount: Math.max(total, updated + finalSkipped),
      },
    });
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
