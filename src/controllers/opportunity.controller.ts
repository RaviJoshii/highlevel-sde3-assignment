import { Request, Response, NextFunction } from 'express';
import { opportunityService } from '../services/opportunity.service';

export class OpportunityController {
  async create(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const workspaceId = req.workspaceId!;
      const { name, monetaryValue, stageId, status, ownerId } = req.body;

      if (!name || monetaryValue === undefined || !stageId || !ownerId) {
        res.status(400).json({
          error: 'Missing required fields: name, monetaryValue, stageId, and ownerId are required',
        });
        return;
      }

      const opp = await opportunityService.createOpportunity({
        workspaceId,
        name,
        monetaryValue: Number(monetaryValue),
        stageId,
        status,
        ownerId,
      });

      res.status(201).json(opp);
    } catch (err) {
      next(err);
    }
  }

  async moveStage(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const workspaceId = req.workspaceId!;
      const id = Array.isArray(req.params.id) ? req.params.id[0] : req.params.id;
      const { toStageId } = req.body;

      if (!toStageId) {
        res.status(400).json({ error: 'toStageId is required' });
        return;
      }

      const result = await opportunityService.moveStage({
        workspaceId,
        opportunityId: id,
        toStageId,
        triggeredBy: 'manual',
      });

      res.status(200).json(result);
    } catch (err) {
      next(err);
    }
  }

  async list(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const workspaceId = req.workspaceId!;
      const { stage, page, limit } = req.query;

      if (!stage || typeof stage !== 'string') {
        res.status(400).json({ error: 'stage query parameter is required' });
        return;
      }

      const result = await opportunityService.listOpportunities({
        workspaceId,
        stageId: stage,
        page: page ? parseInt(page as string, 10) : 1,
        limit: limit ? parseInt(limit as string, 10) : 50,
      });

      res.status(200).json(result);
    } catch (err) {
      next(err);
    }
  }
}

export const opportunityController = new OpportunityController();
