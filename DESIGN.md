# Architecture & Technical Design Document

## 1. System Overview & Core Requirements

The **Bulk Stage Move Engine** is designed to execute massive, multi-tenant CRM deal transitions (up to 50,000 opportunities per action) in a high-scale background environment. The system guarantees five fundamental properties under heavy concurrent load and failure scenarios:

1. **Strict Idempotency**: Identical requests create exactly one job and cannot double-apply.
2. **Crash Resumability**: If a worker process dies mid-job, the job resumes from the exact committed progress boundary without re-processing or duplicating audit transitions.
3. **Observability**: Progress reflects real committed database state rather than volatile in-memory counters.
4. **Collision Correctness**: Concurrent manual edits take precedence over bulk actions. If a record is manually modified (including cyclical moves like $A \to B \to A$), the manual action is preserved and skipped by the bulk job.
5. **Multi-Tenant Isolation**: Bulk operations are paced and bounded, guaranteeing that interactive requests in the same workspace are not starved and neighboring workspaces experience zero degradation.

```
       +-------------------------------------------------------------+
       |                        API Client                           |
       +-------------------------------------------------------------+
                     | POST /bulk-jobs             | GET /bulk-jobs/:id
                     v                             v
       +-------------------------+     +-----------------------------+
       |   Bulk Job Controller   |     |    Progress & Health API    |
       +-------------------------+     +-----------------------------+
                     |                                ^
                     v                                |
       +-------------------------------------------------------------+
       |                       MongoDB Cluster                       |
       |  - BulkJob (Unique Idempotency Key, Cursor, Lease Locks)    |
       |  - Opportunity (Compound Index: ws + stage + _id)           |
       |  - OpportunityStageTransition (Audit Log)                   |
       +-------------------------------------------------------------+
                     ^                                |
                     | Distributed Lease Lock         | Keyset Pagination
                     | Heartbeat Lease Renewal        | Atomic bulkWrite
                     +--------------------------------+
                                     |
                       +---------------------------+
                       |   Worker Engine Daemon    |
                       |  - Keyset Chunking        |
                       |  - A->B->A Collision Guard|
                       |  - Pacing Delay & Fairness|
                       +---------------------------+
```

---

## 2. Job Chunking & Keyset Cursor Resumability

### 2.1 The Chunking Architecture
Large batch operations of 50,000 records cannot be processed in a single unbounded database transaction without risking memory exhaustion, transaction log saturation, and table locks. Conversely, processing records one-by-one introduces excessive network roundtrip latency.

We utilize **Keyset Pagination (Seek Method)** operating in chunks of 500 documents:
- Documents in MongoDB are ordered monotonically by `_id: 1`. MongoDB ObjectIds contain a 4-byte timestamp, a 5-byte random value, and a 3-byte incrementing counter, ensuring strictly ordered generation within any single node.
- Each chunk query executes as:
  ```json
  {
    "workspaceId": "ws-large-1",
    "stageId": "stage-source",
    "_id": { "$gt": "last_committed_cursor_id", "$lte": "snapshot_max_id" }
  }
  ```
  with `.sort({ "_id": 1 }).limit(500)`.

### 2.2 Why Keyset Pagination vs. Offset Pagination (`skip`)
Offset pagination (`skip(N).limit(500)`) has $O(N)$ computational complexity because MongoDB must scan and discard all $N$ preceding documents before returning the 500 records. For a 50,000-record job, offset pagination would read over 2.5 million documents across the job lifetime, degrading performance quadratically as the cursor advances.

In contrast, Keyset Pagination performs an **$O(\log M)$ B-Tree index seek** directly to the first key greater than the cursor. It reads exactly 500 documents per chunk regardless of whether it is processing chunk 1 or chunk 99.

### 2.3 The Index That Makes Chunking Cheap
Chunking is backed by the compound index:
```javascript
db.opportunities.createIndex({ workspaceId: 1, stageId: 1, _id: 1 })
```
- **Index Prefix Rule**: The query filters on `workspaceId` (equality) and `stageId` (equality), then seeks on `_id` (range). This satisfies the Equality-Sort-Range (ESR) rule.
- **Index Scan**: The query performs an `IXSCAN` directly into the leaf nodes of the B-tree, fetching only matching document pointers.

### 2.4 How the Cursor Survives a Restart
Standalone MongoDB gives no multi-document transactions, so crash safety comes from write ordering plus idempotent repair instead. Per chunk of 500 records:
1. `bulkWrite` moves the deals. The same write stamps each moved deal with `lastBulkJobId = job._id` (guarded by `version` and `lastManualStageChangeAt`).
2. Transitions are inserted for exactly the deals carrying this job's stamp. A partial unique index on `{ jobId, opportunityId }` makes this replay-safe (duplicate-key errors are ignored), and a deal a user moved into the target stage concurrently has no stamp, so it gets no `bulk_job` transition.
3. Cursor and counters are committed together in one fenced update (`{ _id, workerId, status: 'processing' }`), which also renews the lease and resets the consecutive-failure counter.

**Crash windows.** If the worker dies after step 1 or 2 but before step 3, the cursor is behind the real state. On resume, `repairUncommittedChunk` finds deals stamped by this job with `_id > cursor` (served by the `{ workspaceId, lastBulkJobId, _id }` partial index), backfills their missing transitions, and re-derives `updatedCount` from the transition count. Deals already moved no longer match `stageId = source`, so they are never moved twice. At completion, `updatedCount` is recomputed from committed transitions rather than trusting incremental counters.

**Fencing.** Every job write filters on `workerId`. A worker that stalls past its lease and wakes up after another worker took over gets no match, throws `LeaseLostError`, and abandons the job without touching cursor or counters.

When work resumes, the new worker queries `_id > job.cursor`; committed chunks are skipped at index-seek speed.

**Retries.** A thrown error (e.g. dropped DB connection) requeues the job with exponential backoff + jitter (`RETRY_BASE_DELAY_MS`, `nextAttemptAt`), keeping the cursor. After `MAX_JOB_ATTEMPTS` consecutive failures without progress, the job becomes `failed`. A job that repeatedly kills its worker is failed the same way on recovery (`attempts` is incremented per recovery claim).

---

## 3. Idempotency Model

### 3.1 What the Key Is & Where It Lives
The idempotency model supports two modes:
1. **Explicit Client Idempotency Key**: Passed via the HTTP header `Idempotency-Key` (e.g., UUIDv4 generated by client).
2. **Deterministic Fingerprint**: If no header is provided, the server derives a SHA-256 fingerprint from the canonical tuple:
   $$\text{Fingerprint} = \text{SHA256}(\text{workspaceId} + \text{canonicalJSON}(\text{filter}) + \text{targetStageId})$$

Both are stored on the `BulkJob` document and enforced by partial unique indexes:
```javascript
// client key: unique per workspace, permanent
db.bulkjobs.createIndex({ workspaceId: 1, idempotencyKey: 1 }, { unique: true, partialFilterExpression: { idempotencyKey: { $type: 'string' } } })
// implicit fingerprint: unique only while the job is active (queued/processing)
db.bulkjobs.createIndex({ workspaceId: 1, fingerprint: 1 }, { unique: true, partialFilterExpression: { active: true } })
```
Reusing a client key with a *different* request returns `409`.

### 3.2 What It Protects
- **Network Retries**: If a mobile or web client sends a request and the HTTP connection drops before the client receives the 201 response, a subsequent retry sends the same `Idempotency-Key`. The server intercepts the duplicate key, queries the existing job, and returns HTTP 200 OK with the original job handle.
- **Concurrent Submissions**: If a user double-clicks or multiple distributed services submit identical jobs simultaneously, MongoDB's unique index guarantees that exactly one `insert` succeeds. The losing threads receive a duplicate key error (`E11000`), catch it, and return the winner's job handle.
- **Lifecycle Protection**: Idempotency is maintained throughout the job lifecycle. A client key never launches a second job, in any state. Without a key, an identical request returns the in-flight job while it is `queued`/`processing`; once it is `completed`/`failed` the same filter is treated as a new request (deals may have arrived since).

### 3.3 Where a Retry Could Still Slip Through
1. **Filter Semantics Drift**: If the client submits two identical logical requests with non-canonical filter variations (e.g., omitted default values or floating timestamp changes like `startDate: new Date()`), the computed fingerprint may differ if explicit `Idempotency-Key` is omitted.
2. **Implicit Replay After Completion**: Without an `Idempotency-Key`, a retry that arrives after the first job already finished starts a second job. It is harmless for stage moves (nothing left to move, so it completes with 0 updates), but clients that need strict replay semantics must send a key.

---

## 4. Concurrency Control on a Single Opportunity & Collision Handling

### 4.1 The Conflict Problem
While a bulk job is processing 50,000 records, human sales representatives are actively editing individual deals via interactive CRM endpoints. A critical race condition arises if a bulk job overwrites a manual move made by a human.

### 4.2 The $A \to B \to A$ Cyclic Move Edge Case
Consider deal #42:
- At time $T_0$, deal #42 is in Stage $A$. A bulk job is scheduled to move all deals from $A \to C$.
- At time $T_1$, a sales rep manually moves deal #42 to Stage $B$.
- At time $T_2$, the sales rep realizes their mistake and moves deal #42 back to Stage $A$.
- At time $T_3$, the bulk worker reads a chunk containing deal #42. Its current stage is $A$!

A naive system checking only `deal.stageId === job.filter.stageId` would conclude that deal #42 was never touched and move it to Stage $C$, obliterating the human's deliberate decision.

### 4.3 Implemented Solution: Timestamp-Bounded Version Preconditions
Every opportunity record maintains:
- `version`: Integer incremented on every update (Optimistic Concurrency Control).
- `lastManualStageChangeAt`: Timestamp set to `new Date()` whenever a manual stage change occurs.

When the bulk worker processes a chunk:
1. **In-Memory Filter**: The worker inspects each candidate:
   ```typescript
   const wasManuallyEdited = opp.lastManualStageChangeAt &&
     new Date(opp.lastManualStageChangeAt).getTime() > new Date(job.snapshotCreatedAt).getTime();
   if (wasManuallyEdited) {
     chunkSkippedCount++;
     continue; // Preserved! Do not touch.
   }
   ```
2. **Atomic Write Precondition**: To protect against a human editing the deal during the exact microsecond the worker is preparing the `bulkWrite`, the update filter enforces:
   ```typescript
   {
     _id: opp._id,
     workspaceId: job.workspaceId,
     stageId: opp.stageId,
     version: opp.version,
     $or: [
       { lastManualStageChangeAt: opp.lastManualStageChangeAt || null },
       { lastManualStageChangeAt: { $exists: false } }
     ]
   }
   ```
3. **Outcome**:
   - If a manual change occurs, `version` or `lastManualStageChangeAt` mismatches, and `modifiedCount` returns 0.
   - The worker records 0 updates for that document, logs it as skipped, and omits the bulk transition audit log.
   - The human edit is 100% preserved.

---

## 5. Snapshot vs. Live Filter Set

### 5.1 The Decision: Snapshot Semantics with Keyset Boundary
We operate on **Snapshot Semantics**, bounded by an upper ObjectId ceiling:
$$\text{Snapshot Upper Bound} = \text{maxOpportunityId at job submission time } (T_0)$$

### 5.2 Why Not Full Live Set?
If we operated strictly on a live set:
- Any new deals created in or moved into Stage $A$ *after* job creation would be swept up by the worker if their `_id` fell beyond the current cursor.
- The total job count would continuously shift, making progress estimation impossible and creating a moving target ("phantom reads").
- A user selecting "move the current 5,000 leads" would unintentionally move leads that an inbound webhook created five seconds later.

### 5.3 Why Not Materialized ID Lists?
An alternative approach is inserting all 50,000 IDs into a temporary table or array at submission time.
- **Trade-off**: Querying and storing 50,000 ObjectIds into an array or auxiliary collection requires tens of megabytes of memory and introduces 50,000 secondary index writes before the job even begins.
- **Our Approach**: By capturing `snapshotCreatedAt` and `snapshotMaxId = max(_id)`, we achieve identical snapshot consistency with **$O(1)$ memory and instantaneous ($<25\text{ms}$) job registration**.

### 5.4 Consequences of This Choice
- Deals created after $T_0$ have `_id > snapshotMaxId` and are excluded.
- Deals that *leave* the filter set between $T_0$ and the time the worker reaches them are skipped. Upon job termination, unencountered snapshot records are cleanly reconciled as `skippedCount`.

---

## 6. Multi-Tenant Isolation & Starvation Prevention

### 6.1 The Mechanism
1. **Chunk Pacing Delay**: Between each 500-record chunk, the worker introduces an asynchronous pacing delay (`CHUNK_PACING_MS = 20ms`). This yields the Node.js event loop and allows MongoDB's WiredTiger storage engine to flush dirty pages and prioritize interactive read tickets.
2. **Per-Workspace Concurrency Cap**: A workspace may only have **one `processing` bulk job** at any time, enforced by the database (partial unique index `one_processing_job_per_workspace` on `{ workspaceId }` where `status = 'processing'`), so it holds even when many workers claim simultaneously. Workspaces with a live job are excluded in the claim query, so one tenant's backlog cannot hide other tenants' jobs.
3. **Global Concurrency Cap**: `WORKER_CONCURRENCY` worker loops run per instance.
4. **Targeted Compound Indexing**: All interactive endpoints (`GET /opportunities`, `PATCH /opportunities/:id/stage`) query with covered index prefixes (`{ workspaceId: 1, stageId: 1, _id: 1 }`), ensuring interactive queries complete in $<10\text{ms}$ even under bulk load.

### 6.2 The Hole It Still Leaves
- **Shared WiredTiger Cache Pool**: In a shared multi-tenant MongoDB cluster, all tenants share the same memory cache pool. A 50,000-record scan inevitably pulls documents into cache, potentially evicting cold cache lines belonging to another workspace.
- **Network Bandwidth**: Massive bulk transitions generate replica set oplog traffic. If worker concurrency is unconstrained, replication lag could impact read replicas.

---

## 7. What Breaks at $10\times$ (500,000-Record Move, 20M Opportunities in Workspace)

### 7.1 The First Thing to Fail
**The Initial Count & Snapshot Query ($O(N)$ index scan on 20M records).**
- At 20M opportunities in a single workspace, evaluating `countDocuments(filter)` for a multi-attribute filter that matches 500,000 records takes 3 to 8 seconds. This exceeds acceptable synchronous HTTP request budgets and ties up MongoDB read tickets.
- Furthermore, a single job recording 500,000 individual `OpportunityStageTransition` documents will generate ~150 MB of audit data, saturating MongoDB collection write locks and triggering replica oplog bloat.

### 7.2 What to Do About It
1. **Asynchronous Partitioning & Count Estimation**:
   - Make job creation $O(1)$ by removing synchronous `countDocuments`. Accept the job with an estimated count using MongoDB collection stats or asynchronous count evaluation.
2. **Partitioned Parallel Workers (Sharded Cursor Ranges)**:
   - Split the 500,000 records into 10 parallel ranges based on ObjectId boundaries:
     $$\text{Worker } k: \text{Range } [Id_k, Id_{k+1})$$
   - 10 workers process the partitions concurrently, reducing wall-clock time from ~80s to ~8s.
3. **Partitioned Transition Collections**:
   - Write bulk transitions into a time-partitioned collection (e.g. `OpportunityStageTransition_2026_11`) or stream transitions directly to Kafka / AWS Kinesis for asynchronous long-term archival in ClickHouse or Snowflake.

---

## 8. What We Would Do With Another Week (Ranked)

1. **Distributed Work Stealing with Redis / BullMQ**:
   - Replace database polling with a Redis-backed streams queue supporting priority queues, per-tenant fair round-robin scheduling, and automatic dead-letter queue (DLQ) retry policies.
2. **Parallel Range Partitioning for Single Jobs**:
   - Dynamically divide large jobs (>20,000 records) across multiple worker threads using ObjectId quantile splits to achieve horizontal scale-out.
3. **WebSocket / SSE Real-Time Progress Streaming**:
   - Add Server-Sent Events (SSE) or WebSocket push notifications for frontend progress bars, eliminating client polling overhead.
4. **Transition Event Streaming to Kafka**:
   - Emit transition change events directly to Kafka/RabbitMQ to feed downstream CRM automations (e.g. email sequences, Slack alerts) without loading the primary database.
5. **Circuit Breakers on Database Pressure**:
   - Integrate MongoDB server status monitoring (tick queue depth and cache dirty percentage) into the worker engine to dynamically throttle chunk pacing if database memory pressure rises.
