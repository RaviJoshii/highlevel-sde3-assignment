import { Types } from 'mongoose';
import { Opportunity, IOpportunity, OpportunityStatus } from '../models/Opportunity';
import { OpportunityStageTransition } from '../models/OpportunityStageTransition';

export interface CreateOpportunityDTO {
  workspaceId: string;
  name: string;
  monetaryValue: number;
  stageId: string;
  status?: OpportunityStatus;
  ownerId: string;
}

export interface MoveStageDTO {
  workspaceId: string;
  opportunityId: string;
  toStageId: string;
  triggeredBy?: 'manual' | 'bulk_job';
  jobId?: string;
}

export interface ListOpportunitiesQuery {
  workspaceId: string;
  stageId: string;
  page?: number;
  limit?: number;
}

export class OpportunityService {
  async createOpportunity(dto: CreateOpportunityDTO): Promise<IOpportunity> {
    const opp = new Opportunity({
      workspaceId: dto.workspaceId,
      name: dto.name,
      monetaryValue: dto.monetaryValue,
      stageId: dto.stageId,
      status: dto.status || 'open',
      ownerId: dto.ownerId,
      lastManualStageChangeAt: null,
      version: 1,
    });
    return opp.save();
  }

  async moveStage(dto: MoveStageDTO): Promise<{
    opportunity: IOpportunity;
    transition?: any;
    skipped?: boolean;
    reason?: string;
  }> {
    const { workspaceId, opportunityId, toStageId, triggeredBy = 'manual', jobId } = dto;

    if (!Types.ObjectId.isValid(opportunityId)) {
      const err: any = new Error('Invalid opportunity ID');
      err.statusCode = 400;
      throw err;
    }

    const opp = await Opportunity.findOne({
      _id: new Types.ObjectId(opportunityId),
      workspaceId,
    });

    if (!opp) {
      const err: any = new Error('Opportunity not found');
      err.statusCode = 404;
      throw err;
    }

    // Target stage validation: skip if already in target stage
    if (opp.stageId === toStageId) {
      return {
        opportunity: opp,
        skipped: true,
        reason: 'Already in target stage',
      };
    }

    const fromStageId = opp.stageId;
    const now = new Date();

    // Atomic update with optimistic version check
    const updateQuery: any = {
      $set: {
        stageId: toStageId,
        updatedAt: now,
      },
      $inc: { version: 1 },
    };

    if (triggeredBy === 'manual') {
      updateQuery.$set.lastManualStageChangeAt = now;
    }

    const updatedOpp = await Opportunity.findOneAndUpdate(
      {
        _id: opp._id,
        workspaceId,
        version: opp.version,
      },
      updateQuery,
      { new: true }
    );

    if (!updatedOpp) {
      const err: any = new Error('Conflict: Opportunity was modified concurrently');
      err.statusCode = 409;
      throw err;
    }

    // Record transition audit trail
    const transition = await OpportunityStageTransition.create({
      workspaceId,
      opportunityId: updatedOpp._id,
      fromStageId,
      toStageId,
      triggeredBy,
      jobId: jobId ? new Types.ObjectId(jobId) : null,
      createdAt: now,
    });

    return {
      opportunity: updatedOpp,
      transition,
      skipped: false,
    };
  }

  async listOpportunities(query: ListOpportunitiesQuery) {
    const { workspaceId, stageId, page = 1, limit = 50 } = query;
    const safeLimit = Math.min(Math.max(limit, 1), 100);
    const skip = (Math.max(page, 1) - 1) * safeLimit;

    const filter = { workspaceId, stageId };

    const [data, total] = await Promise.all([
      Opportunity.find(filter)
        .sort({ _id: -1 })
        .skip(skip)
        .limit(safeLimit)
        .lean(),
      Opportunity.countDocuments(filter),
    ]);

    return {
      data,
      total,
      page: Math.max(page, 1),
      limit: safeLimit,
      totalPages: Math.ceil(total / safeLimit),
    };
  }
}

export const opportunityService = new OpportunityService();
