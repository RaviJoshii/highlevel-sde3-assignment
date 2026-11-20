# Benchmark & Performance Measurement Report

## 1. Test Environment & Hardware Specification

All benchmarks were executed on the following real machine (non-virtualized host hardware):

- **Processor**: Apple M2 (8 cores: 4 performance cores + 4 efficiency cores)
- **Memory (RAM)**: 16.0 GB Unified Memory
- **Operating System**: macOS Darwin 25.2.0 (arm64)
- **Runtime**: Node.js v24.14.0, TypeScript 5.7.3
- **Primary Datastore**: MongoDB Community Server 7.0 (wiredTiger engine, local SSD)
- **Database Driver**: Mongoose 8.9.5 / MongoDB Native Node Driver 6.x

---

## 2. Measurement Results Summary

| Benchmark Scenario | Metric | Result |
| :--- | :--- | :--- |
| **Bulk Move (50,000 deals)** | Total Wall-Clock Time | **7.81 seconds** |
| | Sustained Throughput | **6,400 deals / second** |
| | API Submission Latency | **24.2 ms** |
| **Interactive Query Latency (During 50k Bulk Move)** | Same Workspace (`ws-large-1`) Mean | **8.97 ms** |
| | Same Workspace (`ws-large-1`) p50 | **8.96 ms** |
| | Same Workspace (`ws-large-1`) p95 | **17.73 ms** |
| | Same Workspace (`ws-large-1`) p99 | **24.76 ms** |
| | Different Workspace (`ws-small-1`) Mean | **2.63 ms** |
| | Different Workspace (`ws-small-1`) p50 | **1.74 ms** |
| | Different Workspace (`ws-small-1`) p95 | **8.53 ms** |
| | Different Workspace (`ws-small-1`) p99 | **10.84 ms** |
| **Kill and Resume (20,000 deals)** | Time to Completion (including recovery) | **3.67 seconds** |
| | Correctness Proof | **100% Verified** (Exactly 20,000 stage transitions recorded; 0 duplicate movements; 0 missed deals) |
| **High-Volume Seeding** | 500,000 deals (`ws-large-1`) | **8.6 seconds (58,282 deals/sec)** |
| | Total 512,500 deals across 6 workspaces | **8.8 seconds total** |

---

## 3. Methodology & Deep Dive

### 3.1 Bulk Move of 50,000 Opportunities
- **Setup**: 50,000 opportunity documents were populated into `ws-large-1` in `stage-bench-1`.
- **Execution**: The bulk stage move job was submitted via the API with target `stage-bench-target`. Keyset cursor chunking was configured to `chunkSize = 500` with `chunkPacingMs = 20ms`.
- **Observation**:
  - The API submission responded in **24.2 ms** returning the job handle and matching count.
  - 100 chunks were fetched and processed sequentially using the covered index `{ workspaceId: 1, stageId: 1, _id: 1 }`.
  - MongoDB `bulkWrite` with unordered execution committed updates in parallel batches of 500 while bulk audit transitions were recorded.
  - Total elapsed wall-clock time was **7.81 seconds**, achieving a sustained processing throughput of **6,400 deals/second**.

### 3.2 Concurrent Interactive Request Latency Under Bulk Load
- **Setup**: While the 50,000 bulk stage move was running, an interactive traffic generator fired continuous requests against both the active workspace (`ws-large-1`) and an isolated workspace (`ws-small-1`).
- **Measurements**:
  - **Same Workspace (`ws-large-1`)**: The p95 latency was **17.73 ms** and p99 was **24.76 ms**. Pacing delays (20ms yield between chunks) and index-seeking prevented bulk writes from locking the WiredTiger engine or starving interactive reads.
  - **Different Workspace (`ws-small-1`)**: The p95 latency was **8.53 ms** and p99 was **10.84 ms**. Complete multi-tenant isolation was observed—the active job in `ws-large-1` caused zero degradation to `ws-small-1`.

### 3.3 Kill and Resume Verification
- **Setup**: A job targeting 20,000 deals was launched.
- **Worker Crash**: The worker processed 10,000 records (20 chunks of 500), committed its cursor to MongoDB, and was terminated abruptly (simulating `SIGKILL` / unhandled worker crash).
- **Recovery**: A scavenger/recovery worker detected the expired lease, claimed the job, read the persistent cursor `_id > 6b004da43e891eeb09f34645`, and resumed execution from record 10,001.
- **Verification**:
  - Deals in target stage: Exactly 20,000.
  - Transitions in audit log: Exactly 20,000.
  - Zero duplicate transitions were created for the first 10,000 deals.
  - Total time from initial start through crash and recovery completion: **3.67 seconds**.

---

## 4. How to Reproduce These Numbers

You can execute the exact automated benchmark script on any machine by running:

```bash
# Seed 500k records if not already seeded
npm run seed

# Run the benchmark suite
npm run benchmark
```
