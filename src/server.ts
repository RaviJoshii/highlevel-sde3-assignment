import { createApp } from './app';
import { connectDB, disconnectDB } from './config/db';
import { defaultWorker } from './services/worker.service';
import { config } from './config';

async function bootstrap() {
  await connectDB();
  console.log(`[Database] Connected to MongoDB at ${config.mongoUri}`);

  // Start background worker loop
  defaultWorker.start(500);
  console.log(`[Worker] Started background worker engine: ${defaultWorker.workerId}`);

  const app = createApp();
  const server = app.listen(config.port, () => {
    console.log(`[Server] Opportunities Service running on http://localhost:${config.port}`);
  });

  const shutdown = async (signal: string) => {
    console.log(`\n[Server] Received ${signal}. Gracefully shutting down...`);
    defaultWorker.stop();
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
