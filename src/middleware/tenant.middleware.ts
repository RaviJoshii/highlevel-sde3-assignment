import { Request, Response, NextFunction } from 'express';

declare global {
  namespace Express {
    interface Request {
      workspaceId?: string;
    }
  }
}

export function tenantMiddleware(req: Request, res: Response, next: NextFunction): void {
  const workspaceId = req.header('X-Workspace-Id');

  if (!workspaceId || workspaceId.trim() === '') {
    res.status(400).json({
      error: 'X-Workspace-Id header is required for tenant isolation',
    });
    return;
  }

  req.workspaceId = workspaceId.trim();
  next();
}
