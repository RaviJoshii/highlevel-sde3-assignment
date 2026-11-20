import crypto from 'crypto';
import { Types } from 'mongoose';
import { BulkJob, IBulkJob, IBulkJobFilter } from '../models/BulkJob';
import { Opportunity } from '../models/Opportunity';
import { config } from '../config';

export interface CreateBulkJobDTO {
  workspaceId: string;
  filter: IBulkJobFilter;
  targetStageId: string;
  idempotencyKey?: string;
}

export class BulkJobService {
  /**
   * Generates a deterministic idempotency key if the client didn't supply one.
   */
  generateFingerprint(workspaceId: string, filter: IBulkJobFilter, targetStageId: string): string {
    const canonicalFilter = {
      stageId: filter.stageId,
      ownerId: filter.ownerId || null,
      status: filter.status || null,
      minValue: filter.minValue !== undefined ? filter.minValue : null,
      maxValue: filter.maxValue !== undefined ? filter.maxValue : null,
      startDate: filter.startDate ? new Date(filter.startDate).toISOString() : null,
      endDate: filter.endDate ? new Date(filter.endDate).toISOString() : null,
      targetStageId,
    };
    return crypto
      .createHash('sha256')
      .update(`${workspaceId}:${JSON.stringify(canonicalFilter)}`)
      .digest('hex');
  }

  /**
   * Builds the MongoDB query condition from the bulk job filter.
   */
  buildFilterQuery(workspaceId: string, filter: IBulkJobFilter): any {
    const query: any = {
      workspaceId,
      stageId: filter.stageId,
    };

    if (filter.ownerId) {
      query.ownerId = filter.ownerId;
    }

    if (filter.status) {
      query.status = filter.status;
    }

    if (filter.minValue !== undefined || filter.maxValue !== undefined) {
      query.monetaryValue = {};
      if (filter.minValue !== undefined) {
        query.monetaryValue.$gte = filter.minValue;
      }
      if (filter.maxValue !== undefined) {
        query.monetaryValue.$lte = filter.maxValue;
      }
    }

    if (filter.startDate || filter.endDate) {
      query.createdAt = {};
      if (filter.startDate) {
        query.createdAt.$gte = new Date(filter.startDate);
      }
      if (filter.endDate) {
        query.createdAt.$lte = new Date(filter.endDate);
      }
    }

    return query;
  }

  /**
   * Creates or returns an existing bulk stage move job.
   * Enforces:
   * 1. Target stage validation (source != target)
   * 2. Large batch handling (gracefully rejects > 50,000 matching deals)
   * 3. Idempotent deduplication (duplicate identical requests create only one job)
   */
  async createJob(dto: CreateBulkJobDTO): Promise<{ job: IBulkJob; isExisting: boolean }> {
    const { workspaceId, filter, targetStageId } = dto;

    if (!filter || !filter.stageId) {
      const err: any = new Error('filter.stageId is required');
      err.statusCode = 400;
      throw err;
    }

    if (!targetStageId) {
      const err: any = new Error('targetStageId is required');
      err.statusCode = 400;
      throw err;
    }

    // User Requirement 2: Target stage validation
    if (filter.stageId === targetStageId) {
      const err: any = new Error('Target stage cannot be identical to source stage');
      err.statusCode = 400;
      throw err;
    }

    const idempotencyKey =
      dto.idempotencyKey && dto.idempotencyKey.trim() !== ''
        ? dto.idempotencyKey.trim()
        : this.generateFingerprint(workspaceId, filter, targetStageId);

    // Check if an existing job with this idempotency key is already registered
    const existingJob = await BulkJob.findOne({ workspaceId, idempotencyKey });
    if (existingJob) {
      return { job: existingJob, isExisting: true };
    }

    // Snapshot query evaluation and User Requirement 3: Large Batch Handling
    const filterQuery = this.buildFilterQuery(workspaceId, filter);
    const matchingCount = await Opportunity.countDocuments(filterQuery);

    if (matchingCount > config.maxBatchSize) {
      const err: any = new Error(
        `Batch size exceeds maximum allowed limit of ${config.maxBatchSize.toLocaleString()} matching deals (found ${matchingCount.toLocaleString()})`
      );
      err.statusCode = 422;
      err.matchingCount = matchingCount;
      err.maxLimit = config.maxBatchSize;
      throw err;
    }

    // Find upper-bound ID at snapshot time for deterministic snapshot semantics
    const latestDoc = await Opportunity.findOne(filterQuery)
      .sort({ _id: -1 })
      .select('_id')
      .lean();

    const snapshotMaxId = latestDoc ? (latestDoc._id as Types.ObjectId) : null;
    const now = new Date();

    try {
      const newJob = await BulkJob.create({
        workspaceId,
        idempotencyKey,
        status: 'queued',
        filter,
        targetStageId,
        totalCount: matchingCount,
        processedCount: 0,
        updatedCount: 0,
        skippedCount: 0,
        failedCount: 0,
        cursor: null,
        snapshotMaxId,
        snapshotCreatedAt: now,
      });

      return { job: newJob, isExisting: false };
    } catch (err: any) {
      // Handle race condition: concurrent duplicate request created job between findOne and create
      if (err.code === 11000) {
        const raceJob = await BulkJob.findOne({ workspaceId, idempotencyKey });
        if (raceJob) {
          return { job: raceJob, isExisting: true };
        }
      }
      throw err;
    }
  }

  /**
   * Retrieves real committed job status.
   */
  async getJob(workspaceId: string, jobId: string): Promise<IBulkJob | null> {
    if (!Types.ObjectId.isValid(jobId)) {
      const err: any = new Error('Invalid Job ID');
      err.statusCode = 400;
      throw err;
    }

    return BulkJob.findOne({
      _id: new Types.ObjectId(jobId),
      workspaceId,
    });
  }
}

export const bulkJobService = new BulkJobService();
