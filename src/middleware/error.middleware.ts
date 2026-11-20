import { Request, Response, NextFunction } from 'express';

export function errorHandler(
  err: any,
  _req: Request,
  res: Response,
  _next: NextFunction
): void {
  const statusCode = err.statusCode || (err.name === 'ValidationError' ? 400 : 500);
  res.status(statusCode).json({
    error: err.message || 'Internal Server Error',
    code: err.code || undefined,
    matchingCount: err.matchingCount !== undefined ? err.matchingCount : undefined,
    maxLimit: err.maxLimit !== undefined ? err.maxLimit : undefined,
  });
}
