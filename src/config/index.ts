import dotenv from 'dotenv';
dotenv.config();

export const config = {
  port: parseInt(process.env.PORT || '3000', 10),
  mongoUri: process.env.MONGODB_URI || 'mongodb://127.0.0.1:27017/opportunities_crm',
  nodeEnv: process.env.NODE_ENV || 'development',
  chunkSize: parseInt(process.env.CHUNK_SIZE || '500', 10),
  chunkPacingMs: parseInt(process.env.CHUNK_PACING_MS || '20', 10),
  maxBatchSize: parseInt(process.env.MAX_BATCH_SIZE || '50000', 10),
  leaseDurationMs: parseInt(process.env.LEASE_DURATION_MS || '30000', 10),
  heartbeatIntervalMs: parseInt(process.env.HEARTBEAT_INTERVAL_MS || '5000', 10),
  maxJobAttempts: parseInt(process.env.MAX_JOB_ATTEMPTS || '5', 10),
  retryBaseDelayMs: parseInt(process.env.RETRY_BASE_DELAY_MS || '1000', 10),
  workerConcurrency: parseInt(process.env.WORKER_CONCURRENCY || '2', 10),
};
