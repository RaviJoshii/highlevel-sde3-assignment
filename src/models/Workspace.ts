import mongoose, { Schema } from 'mongoose';

export interface IPipelineStage {
  stageId: string;
  name: string;
  order: number;
}

export interface IWorkspace {
  _id: string; // Tenant workspace ID (e.g. 'ws-large-1')
  name: string;
  stages: IPipelineStage[];
  createdAt?: Date;
  updatedAt?: Date;
}

const PipelineStageSchema = new Schema<IPipelineStage>(
  {
    stageId: { type: String, required: true },
    name: { type: String, required: true },
    order: { type: Number, required: true },
  },
  { _id: false }
);

const WorkspaceSchema = new Schema<IWorkspace>(
  {
    _id: { type: String, required: true },
    name: { type: String, required: true },
    stages: { type: [PipelineStageSchema], required: true },
  },
  {
    timestamps: true,
  }
);

export const Workspace = mongoose.model<IWorkspace>('Workspace', WorkspaceSchema);
