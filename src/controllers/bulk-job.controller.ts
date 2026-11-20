import { Request, Response, NextFunction } from 'express';
import { bulkJobService } from '../services/bulk-job.service';

export class BulkJobController {
  async createJob(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const workspaceId = req.workspaceId!;
      const { filter, targetStageId } = req.body;
      const idempotencyKey = req.header('Idempotency-Key') || req.body.idempotencyKey;

      if (!filter || !targetStageId) {
        res.status(400).json({ error: 'filter and targetStageId are required' });
        return;
      }

      const { job, isExisting } = await bulkJobService.createJob({
        workspaceId,
        filter,
        targetStageId,
        idempotencyKey,
      });

      // 201 for freshly created job, 200 for idempotent duplicate request
      const statusCode = isExisting ? 200 : 201;

      res.status(statusCode).json({
        id: job._id,
        workspaceId: job.workspaceId,
        status: job.status,
        totalCount: job.totalCount,
        processedCount: job.processedCount,
        updatedCount: job.updatedCount,
        skippedCount: job.skippedCount,
        filter: job.filter,
        targetStageId: job.targetStageId,
        idempotentReplay: isExisting,
        createdAt: job.createdAt,
      });
    } catch (err) {
      next(err);
    }
  }

  async getJob(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const workspaceId = req.workspaceId!;
      const id = Array.isArray(req.params.id) ? req.params.id[0] : req.params.id;

      const job = await bulkJobService.getJob(workspaceId, id);

      if (!job) {
        res.status(404).json({ error: 'Bulk job not found' });
        return;
      }

      const now = new Date().getTime();
      let liveness = 'healthy';

      if (job.status === 'processing') {
        const lastHb = job.lastHeartbeatAt ? new Date(job.lastHeartbeatAt).getTime() : 0;
        // If no heartbeat for > 15s, mark as slow/stalled
        if (now - lastHb > 15000) {
          liveness = 'stalled_recovering';
        }
      }

      const progressPercent =
        job.totalCount > 0
          ? Math.min(100, Math.round((job.processedCount / job.totalCount) * 100))
          : 100;

      res.status(200).json({
        id: job._id,
        workspaceId: job.workspaceId,
        status: job.status,
        liveness,
        totalCount: job.totalCount,
        processedCount: job.processedCount,
        updatedCount: job.updatedCount,
        skippedCount: job.skippedCount,
        failedCount: job.failedCount,
        progressPercent,
        cursor: job.cursor,
        startedAt: job.startedAt,
        completedAt: job.completedAt,
        lastHeartbeatAt: job.lastHeartbeatAt,
        errorMessage: job.errorMessage,
      });
    } catch (err) {
      next(err);
    }
  }
}

export const bulkJobController = new BulkJobController();
