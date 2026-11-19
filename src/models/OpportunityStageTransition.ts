import mongoose, { Schema, Document, Types } from 'mongoose';

export type TransitionTrigger = 'manual' | 'bulk_job';

export interface IOpportunityStageTransition extends Document {
  _id: Types.ObjectId;
  workspaceId: string;
  opportunityId: Types.ObjectId;
  fromStageId: string;
  toStageId: string;
  triggeredBy: TransitionTrigger;
  jobId?: Types.ObjectId | null;
  createdAt: Date;
}

const OpportunityStageTransitionSchema = new Schema<IOpportunityStageTransition>(
  {
    workspaceId: { type: String, required: true, index: true },
    opportunityId: { type: Schema.Types.ObjectId, ref: 'Opportunity', required: true, index: true },
    fromStageId: { type: String, required: true },
    toStageId: { type: String, required: true },
    triggeredBy: {
      type: String,
      required: true,
      enum: ['manual', 'bulk_job'],
    },
    jobId: { type: Schema.Types.ObjectId, ref: 'BulkJob', default: null, index: true },
  },
  {
    timestamps: { createdAt: true, updatedAt: false },
  }
);

OpportunityStageTransitionSchema.index({ workspaceId: 1, opportunityId: 1, createdAt: -1 });

export const OpportunityStageTransition = mongoose.model<IOpportunityStageTransition>(
  'OpportunityStageTransition',
  OpportunityStageTransitionSchema
);
