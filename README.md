# CRM Opportunities Bulk Stage Move Engine

A high-throughput, resilient, and multi-tenant background engine for moving up to 50,000 CRM deal opportunities in a single action. Built with Node.js, TypeScript, Express, and MongoDB (Mongoose).

---

## Quick Start (One Command Run)

To bring up the MongoDB database, run the complete automated test suite, seed initial datasets, and start the service on a clean machine:

```bash
docker compose up
```

Once up, the API is available at:
`http://localhost:3000`

---

## Local Development (Without Docker)

### Prerequisites
- Node.js (v20+ or v24)
- npm (v10+)
- MongoDB instance running locally on `localhost:27017`

### 1. Install Dependencies
```bash
npm install
```

### 2. Environment Configuration
Create a `.env` file (or use default `.env.example`):
```bash
PORT=3000
MONGODB_URI=mongodb://127.0.0.1:27017/opportunities_crm
NODE_ENV=development
CHUNK_SIZE=500
CHUNK_PACING_MS=20
MAX_BATCH_SIZE=50000
LEASE_DURATION_MS=30000
HEARTBEAT_INTERVAL_MS=5000
WORKER_CONCURRENCY=2
```

### 3. Run Automated Tests
Runs all unit and integration test suites against the real MongoDB instance:
```bash
npm test
```

### 4. Seed Database
Generates 1 large workspace (`ws-large-1`) with 500,000 opportunities across 12 stages spread over ~18 months, plus 5 small workspaces:
```bash
npm run seed
```

### 5. Run Performance Benchmarks
Executes the comprehensive benchmark suite (50k bulk move throughput, concurrent interactive request latency, kill & resume verification):
```bash
npm run benchmark
```

### 6. Start the Service
```bash
# Build TypeScript
npm run build

# Start production server with background worker engine
npm start

# Or start in development mode with ts-node
npm run dev
```

---

## Core Deliverables & Implemented Features

### 1. Manual Edit Handling (Preserves $A \to B \to A$)
- Tracks `lastManualStageChangeAt` and `version` on each opportunity document.
- If a user manually changes an opportunity's stage while a bulk job is running—even if they move it $A \to B \to A$ so the stage appears unchanged—the worker identifies the manual intervention, skips the record, preserves the human edit, and increments `skippedCount`.

### 2. Target Stage Validation
- Validates upfront that source and target stages are not identical.
- In-flight check skips any deals that are already in the target stage, avoiding redundant database writes and transition log bloat.

### 3. Large Batch Handling (50,000 Limit)
- Evaluates matching document counts at job submission time.
- Requests matching $>50,000$ opportunities are rejected immediately with HTTP 422:
  `"Batch size exceeds maximum allowed limit of 50,000 matching deals"`
  without consuming worker queue resources.

### 4. Idempotency & Duplicate Request Prevention
- Supports explicit client `Idempotency-Key` headers or automatic deterministic SHA-256 fingerprinting.
- Client keys are permanent (partial unique index on `{ workspaceId, idempotencyKey }`); the implicit fingerprint dedupes while a job is queued/processing (partial unique index on `{ workspaceId, fingerprint }` where `active`). Key reuse with a different payload returns 409.
- Filters are validated (primitive types only) and both stages must exist in the workspace pipeline.
- Replayed or concurrent duplicate requests safely return the existing job handle with HTTP 200 without launching duplicate jobs.

### 5. Crash Recovery & Resumability
- Keyset cursor chunking (`_id > cursor`) advances in batches of 500 records.
- Progress, cursor, and metrics are committed to MongoDB at each chunk boundary, fenced on lease ownership.
- Moved deals are stamped with `lastBulkJobId`; after a crash, missing transitions are backfilled and counters re-derived (unique `{jobId, opportunityId}` index makes it replay-safe).
- Transient failures retry with exponential backoff (`MAX_JOB_ATTEMPTS`, `RETRY_BASE_DELAY_MS`); exhausted jobs become `failed`.
- If a worker dies mid-job (e.g. at 30,000 of 50,000), a recovery worker claims the expired lease and resumes processing from record 30,001. Previously completed records are never re-processed and 0 duplicate transitions are created.

### 6. Multi-Tenant Isolation & Pacing
- Enforces a per-workspace concurrency cap (only 1 `processing` bulk job per tenant) with a partial unique index, so it is atomic across workers.
- 20ms chunk pacing delay yields database I/O and CPU to interactive requests, maintaining sub-20ms p95 latency for interactive operations.

---

## API Documentation

All endpoints require the `X-Workspace-Id` header for tenant isolation.

### Part 1: Foundation Endpoints

#### 1. Create Opportunity
```http
POST /opportunities
X-Workspace-Id: ws-large-1
Content-Type: application/json

{
  "name": "Acme Enterprise Software",
  "monetaryValue": 45000,
  "stageId": "stage-1",
  "status": "open",
  "ownerId": "rep-alice"
}
```

#### 2. Move Opportunity Stage (Single)
```http
PATCH /opportunities/:id/stage
X-Workspace-Id: ws-large-1
Content-Type: application/json

{
  "toStageId": "stage-2"
}
```

#### 3. List Opportunities in Stage
```http
GET /opportunities?stage=stage-1&page=1&limit=50
X-Workspace-Id: ws-large-1
```

---

### Part 2: Bulk Stage Move Endpoints

#### 1. Submit Bulk Move Job
```http
POST /bulk-jobs
X-Workspace-Id: ws-large-1
Idempotency-Key: 7b8f9e01-1234-5678-abcd-000000000001
Content-Type: application/json

{
  "filter": {
    "stageId": "stage-1",
    "ownerId": "rep-alice",
    "status": "open",
    "minValue": 10000,
    "maxValue": 50000
  },
  "targetStageId": "stage-2"
}
```
**Response (201 Created / 200 OK for Idempotent Replay):**
```json
{
  "id": "673f1a2b8e9f1a2b3c4d5e6f",
  "workspaceId": "ws-large-1",
  "status": "queued",
  "totalCount": 4200,
  "processedCount": 0,
  "updatedCount": 0,
  "skippedCount": 0,
  "filter": { "stageId": "stage-1", "ownerId": "rep-alice" },
  "targetStageId": "stage-2",
  "idempotentReplay": false,
  "createdAt": "2026-11-20T13:30:00.000Z"
}
```

#### 2. Poll Job Progress
```http
GET /bulk-jobs/:id
X-Workspace-Id: ws-large-1
```
**Response (200 OK):**
```json
{
  "id": "673f1a2b8e9f1a2b3c4d5e6f",
  "workspaceId": "ws-large-1",
  "status": "processing",
  "liveness": "healthy",
  "totalCount": 4200,
  "processedCount": 2500,
  "updatedCount": 2490,
  "skippedCount": 10,
  "failedCount": 0,
  "progressPercent": 60,
  "cursor": "673f1a2c8e9f1a2b3c4d5e8a",
  "startedAt": "2026-11-20T13:30:01.000Z",
  "completedAt": null,
  "lastHeartbeatAt": "2026-11-20T13:30:03.500Z",
  "errorMessage": null
}
```

---

## Scope Boundaries (What Is and Is Not Implemented)

### What IS Implemented
- Complete Part 1 Foundation APIs (`create`, `moveStage`, `listOpportunities` with pagination and tenant isolation).
- Full Part 2 Bulk Stage Move background job engine with lease locks and heartbeats.
- All 5 core properties:
  - Idempotency with persistent compound unique index.
  - Keyset cursor pagination surviving restarts.
  - Committed observable progress reporting.
  - Manual edit collision handling including cyclic $A \to B \to A$ moves.
  - Pacing and tenant isolation mechanism.
- 50,000 batch limit validation.
- Comprehensive test suite covering all failure modes and hard scenarios.
- High-volume seeding script for 500,000 opportunities.
- Full benchmark measurement script and reports.

### What is Explicitly OUT OF SCOPE (as defined in take-home brief)
- Frontend user interfaces or boards.
- User authentication / JWT tokens (tenant scoping handled via `X-Workspace-Id`).
- Downstream automation consumers / webhook deliveries.
- Custom fields, tasks, notes, or contact associations.
- Kubernetes deployment manifests.
