import crypto from 'crypto';
import { Types } from 'mongoose';
import { BulkJob, IBulkJob, IBulkJobFilter } from '../models/BulkJob';
import { Opportunity } from '../models/Opportunity';
import { Workspace } from '../models/Workspace';
import { config } from '../config';

export interface CreateBulkJobDTO {
  workspaceId: string;
  filter: IBulkJobFilter;
  targetStageId: string;
  idempotencyKey?: string;
}

const VALID_STATUSES = ['open', 'won', 'lost', 'abandoned'];

function badRequest(message: string, statusCode = 400): Error {
  const err: any = new Error(message);
  err.statusCode = statusCode;
  return err;
}

export class BulkJobService {
  /**
   * Rejects non-primitive / malformed filter values so request bodies cannot inject
   * query operators (e.g. ownerId: { $ne: null }) into the Mongo query.
   */
  validateFilter(filter: IBulkJobFilter): void {
    const isNonEmptyString = (v: unknown) => typeof v === 'string' && v.trim() !== '';
    const isValidDate = (v: unknown) =>
      (typeof v === 'string' || typeof v === 'number' || v instanceof Date) &&
      !Number.isNaN(new Date(v as any).getTime());

    if (!isNonEmptyString(filter.stageId)) throw badRequest('filter.stageId must be a non-empty string');
    if (filter.ownerId !== undefined && !isNonEmptyString(filter.ownerId)) {
      throw badRequest('filter.ownerId must be a non-empty string');
    }
    if (filter.status !== undefined && !VALID_STATUSES.includes(filter.status as string)) {
      throw badRequest(`filter.status must be one of ${VALID_STATUSES.join(', ')}`);
    }
    for (const key of ['minValue', 'maxValue'] as const) {
      const v = filter[key];
      if (v !== undefined && (typeof v !== 'number' || !Number.isFinite(v))) {
        throw badRequest(`filter.${key} must be a finite number`);
      }
    }
    if (
      filter.minValue !== undefined &&
      filter.maxValue !== undefined &&
      filter.minValue > filter.maxValue
    ) {
      throw badRequest('filter.minValue cannot exceed filter.maxValue');
    }
    for (const key of ['startDate', 'endDate'] as const) {
      const v = filter[key];
      if (v !== undefined && !isValidDate(v)) throw badRequest(`filter.${key} must be a valid date`);
    }
  }

  private async findExisting(
    workspaceId: string,
    idempotencyKey: string | null,
    fingerprint: string
  ): Promise<IBulkJob | null> {
    if (idempotencyKey) {
      const byKey = await BulkJob.findOne({ workspaceId, idempotencyKey });
      if (byKey) {
        if (byKey.fingerprint !== fingerprint) {
          throw badRequest('Idempotency-Key was already used with a different request', 409);
        }
        return byKey;
      }
    }
    // Identical request still in flight: replay returns the same job
    return BulkJob.findOne({ workspaceId, fingerprint, active: true });
  }

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
   * 3. Idempotent deduplication: a client Idempotency-Key dedupes permanently; without one,
   *    an identical request dedupes while a matching job is still queued/processing
   */
  async createJob(dto: CreateBulkJobDTO): Promise<{ job: IBulkJob; isExisting: boolean }> {
    const { workspaceId, filter, targetStageId } = dto;

    if (!filter || typeof filter !== 'object') throw badRequest('filter is required');
    this.validateFilter(filter);

    if (typeof targetStageId !== 'string' || targetStageId.trim() === '') {
      throw badRequest('targetStageId is required');
    }

    // Target stage validation (source != target)
    if (filter.stageId === targetStageId) {
      throw badRequest('Target stage cannot be identical to source stage');
    }

    // Both stages must belong to this workspace's pipeline
    const workspace = await Workspace.findById(workspaceId).lean();
    if (!workspace) throw badRequest('Workspace not found', 404);
    const stageIds = new Set(workspace.stages.map((st) => st.stageId));
    if (!stageIds.has(targetStageId)) {
      throw badRequest(`targetStageId '${targetStageId}' does not exist in this workspace pipeline`);
    }
    if (!stageIds.has(filter.stageId)) {
      throw badRequest(`filter.stageId '${filter.stageId}' does not exist in this workspace pipeline`);
    }

    const fingerprint = this.generateFingerprint(workspaceId, filter, targetStageId);
    const clientKey =
      typeof dto.idempotencyKey === 'string' && dto.idempotencyKey.trim() !== ''
        ? dto.idempotencyKey.trim()
        : null;

    const existingJob = await this.findExisting(workspaceId, clientKey, fingerprint);
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

    // Nothing matches: complete immediately instead of leaving an unbounded job for the worker
    const nothingToDo = matchingCount === 0;

    try {
      const newJob = await BulkJob.create({
        workspaceId,
        idempotencyKey: clientKey,
        fingerprint,
        active: !nothingToDo,
        status: nothingToDo ? 'completed' : 'queued',
        completedAt: nothingToDo ? now : null,
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
        const raceJob = await this.findExisting(workspaceId, clientKey, fingerprint);
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
