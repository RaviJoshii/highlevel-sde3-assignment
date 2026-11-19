import mongoose, { Schema, Document, Types } from 'mongoose';

export type OpportunityStatus = 'open' | 'won' | 'lost' | 'abandoned';

export interface IOpportunity extends Document {
  _id: Types.ObjectId;
  workspaceId: string;
  name: string;
  monetaryValue: number;
  stageId: string;
  status: OpportunityStatus;
  ownerId: string;
  lastManualStageChangeAt?: Date | null;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

const OpportunitySchema = new Schema<IOpportunity>(
  {
    workspaceId: { type: String, required: true, index: true },
    name: { type: String, required: true },
    monetaryValue: { type: Number, required: true, default: 0 },
    stageId: { type: String, required: true, index: true },
    status: {
      type: String,
      required: true,
      enum: ['open', 'won', 'lost', 'abandoned'],
      default: 'open',
      index: true,
    },
    ownerId: { type: String, required: true, index: true },
    lastManualStageChangeAt: { type: Date, default: null },
    version: { type: Number, default: 1 },
  },
  {
    timestamps: true,
  }
);

// High-performance keyset cursor index: workspace + stage + _id
OpportunitySchema.index({ workspaceId: 1, stageId: 1, _id: 1 });

// Multi-attribute filter index for complex stage moves
OpportunitySchema.index({
  workspaceId: 1,
  stageId: 1,
  ownerId: 1,
  status: 1,
  monetaryValue: 1,
  createdAt: 1,
  _id: 1,
});

// Tenant point lookup index
OpportunitySchema.index({ workspaceId: 1, _id: 1 });

export const Opportunity = mongoose.model<IOpportunity>('Opportunity', OpportunitySchema);
