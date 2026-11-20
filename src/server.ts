import { createApp } from './app';
import { connectDB, disconnectDB } from './config/db';
import { WorkerEngine } from './services/worker.service';
import { config } from './config';

async function bootstrap() {
  await connectDB();
  console.log(`[Database] Connected to MongoDB at ${config.mongoUri}`);

  // Start background worker loop
  const workers = Array.from({ length: Math.max(1, config.workerConcurrency) }, () => new WorkerEngine());
  workers.forEach((w) => w.start(500));
  console.log(`[Worker] Started ${workers.length} worker(s): ${workers.map((w) => w.workerId).join(', ')}`);

  const app = createApp();
  const server = app.listen(config.port, () => {
    console.log(`[Server] Opportunities Service running on http://localhost:${config.port}`);
  });

  const shutdown = async (signal: string) => {
    console.log(`\n[Server] Received ${signal}. Gracefully shutting down...`);
    workers.forEach((w) => w.stop());
    server.close(async () => {
      await disconnectDB();
      console.log('[Server] Successfully shut down.');
      process.exit(0);
    });
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

if (require.main === module) {
  bootstrap().catch((err) => {
    console.error('[Server] Fatal startup error:', err);
    process.exit(1);
  });
}
