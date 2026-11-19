import mongoose, { Schema, Document, Types } from 'mongoose';
import { OpportunityStatus } from './Opportunity';

export type BulkJobStatus = 'queued' | 'processing' | 'completed' | 'failed' | 'rejected';

export interface IBulkJobFilter {
  stageId: string;
  ownerId?: string;
  status?: OpportunityStatus;
  minValue?: number;
  maxValue?: number;
  startDate?: Date;
  endDate?: Date;
}

export interface IBulkJob extends Document {
  _id: Types.ObjectId;
  workspaceId: string;
  idempotencyKey: string;
  status: BulkJobStatus;
  filter: IBulkJobFilter;
  targetStageId: string;
  totalCount: number;
  processedCount: number;
  updatedCount: number;
  skippedCount: number;
  failedCount: number;
  cursor?: Types.ObjectId | null;
  snapshotMaxId?: Types.ObjectId | null;
  snapshotCreatedAt: Date;
  workerId?: string | null;
  leaseExpiresAt?: Date | null;
  lastHeartbeatAt?: Date | null;
  startedAt?: Date | null;
  completedAt?: Date | null;
  failedAt?: Date | null;
  errorMessage?: string | null;
  createdAt: Date;
  updatedAt: Date;
}

const BulkJobFilterSchema = new Schema<IBulkJobFilter>(
  {
    stageId: { type: String, required: true },
    ownerId: { type: String },
    status: { type: String, enum: ['open', 'won', 'lost', 'abandoned'] },
    minValue: { type: Number },
    maxValue: { type: Number },
    startDate: { type: Date },
    endDate: { type: Date },
  },
  { _id: false }
);

const BulkJobSchema = new Schema<IBulkJob>(
  {
    workspaceId: { type: String, required: true, index: true },
    idempotencyKey: { type: String, required: true },
    status: {
      type: String,
      required: true,
      enum: ['queued', 'processing', 'completed', 'failed', 'rejected'],
      default: 'queued',
      index: true,
    },
    filter: { type: BulkJobFilterSchema, required: true },
    targetStageId: { type: String, required: true },
    totalCount: { type: Number, default: 0 },
    processedCount: { type: Number, default: 0 },
    updatedCount: { type: Number, default: 0 },
    skippedCount: { type: Number, default: 0 },
    failedCount: { type: Number, default: 0 },
    cursor: { type: Schema.Types.ObjectId, default: null },
    snapshotMaxId: { type: Schema.Types.ObjectId, default: null },
    snapshotCreatedAt: { type: Date, required: true, default: Date.now },
    workerId: { type: String, default: null },
    leaseExpiresAt: { type: Date, default: null, index: true },
    lastHeartbeatAt: { type: Date, default: null },
    startedAt: { type: Date, default: null },
    completedAt: { type: Date, default: null },
    failedAt: { type: Date, default: null },
    errorMessage: { type: String, default: null },
  },
  {
    timestamps: true,
  }
);

// Idempotency: Workspace + IdempotencyKey must be globally unique
BulkJobSchema.index({ workspaceId: 1, idempotencyKey: 1 }, { unique: true });

// Scavenger & Worker claiming index
BulkJobSchema.index({ status: 1, leaseExpiresAt: 1 });

// Tenant concurrency check index
BulkJobSchema.index({ workspaceId: 1, status: 1 });

export const BulkJob = mongoose.model<IBulkJob>('BulkJob', BulkJobSchema);
