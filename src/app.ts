import express from 'express';
import { tenantMiddleware } from './middleware/tenant.middleware';
import { errorHandler } from './middleware/error.middleware';
import { opportunityRoutes } from './routes/opportunity.routes';
import { bulkJobRoutes } from './routes/bulk-job.routes';

export function createApp(): express.Application {
  const app = express();

  app.use(express.json());

  // Health check endpoint (exempt from tenant middleware)
  app.get('/health', (_req, res) => {
    res.status(200).json({ status: 'ok', timestamp: new Date().toISOString() });
  });

  // Tenant-scoped routes
  app.use('/opportunities', tenantMiddleware, opportunityRoutes);
  app.use('/bulk-jobs', tenantMiddleware, bulkJobRoutes);

  // Global error handler
  app.use(errorHandler);

  return app;
}
