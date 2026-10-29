# Durable work queue

Build a SQLite job queue. It must handle worker crashes and repeated requests without losing jobs or accepting work from an expired lease.

Implement `/app/queue.py` using Python 3.12 and its standard library. Run it as `python /app/queue.py --db /path/queue.db`. Read one JSON object per stdin line and flush one JSON response per line. Keep serving after a rejected request. Stdout is only for responses; stderr is for diagnostics. Your entire implementation must be in this file.

The starter reads and writes JSON lines. Implement the queue using the rules below. You may replace the starter.

## Data and responses

Every request has `op` and `tenant`. Tenants, job IDs, and worker names are nonempty strings of at most 100 characters. They are opaque, case-sensitive identifiers. Jobs are identified by `(tenant, id)`; tenants are isolated. Extra request fields are ignored.

Successful responses are `{"ok":true,"result":...}`. Rejections are `{"ok":false,"error":"CODE"}` with `invalid`, `conflict`, `not_found`, or `stale`. Use exactly these response keys. Malformed JSON, unknown operations, and invalid fields return `invalid`. A rejected request must not change durable state, including events.

Times are caller-supplied integers in seconds, from 0 through 1,000,000,000. No wall clock is used. Requests can supply different times; do not maintain a global clock or advance time on reads. Boolean values are not integers. Lease durations are integers from 1 through 3600. Priorities range from -100 through 100. Maximum attempts range from 1 through 10.

A job object has exactly these fields: `id`, `tenant`, `payload`, `priority`, `run_at`, `max_attempts`, `depends_on`, `state`, `attempts`, `token`, `worker`, `lease_until`. `payload` is any JSON value. `depends_on` is a sorted list of IDs. New jobs have state `queued`, attempts and token 0, and worker and lease_until null. Other states are `leased`, `done`, `dead`, and `cancelled`. Returning a job means its state after that operation.

## Operations

`submit`: requires a nonempty `jobs` array of at most 1000 objects. Each needs `id` and `payload`. Defaults: priority 0, run_at 0, max_attempts 3, depends_on []. Dependencies are unique IDs in the same tenant, already present or in this batch, and must form an acyclic graph. Self dependencies, repeated IDs in a batch, invalid fields, missing dependencies, and cycles return `invalid`.

The whole batch is atomic. Repeating an ID with the same original specification succeeds without changing its current state or adding an event. Compare payloads as canonical JSON (sorted object keys, compact separators); integer 1, float 1.0, and boolean true are distinct. Compare dependency lists as sets. A different specification for an existing ID returns `conflict`, rolling back the whole batch. Original run_at remains part of this comparison even when retries change the current run_at. Return the jobs in input order. For requests with both invalid input and conflicts, either applicable code is acceptable.

`claim`: requires worker, now, and lease_sec. In one transaction, first reap this tenant's expired leases (`lease_until <= now`) in job creation order. A reaped job becomes `dead` if attempts has reached max_attempts; otherwise it becomes queued with run_at = old lease_until + retry_delay. Clear its worker and lease_until. Append an `expired` event either way. Then pick at most one queued job with run_at <= now whose dependencies are all done. Order by priority descending, current run_at ascending, then creation order. Change it to leased, increment attempts and token, and set worker and lease_until = now + lease_sec. Append `claimed`. Return that job, or null when nothing is eligible. Creation order is the order of first successful inserts, including input order within a batch. Idempotent submissions do not change it.

`ack`, `nack`, and `extend`: require id, worker, token, and now. Token is an integer from 1 through 1,000,000,000. The job must exist in this tenant or return `not_found`. It must be leased to this worker with this token and lease_until > now; otherwise return `stale` with no side effects. An expired lease is stale even before a claim reaps it.

Ack sets state to done, clears worker and lease_until, and appends `completed`. Nack sets state to dead when attempts has reached max_attempts, otherwise queued with run_at = now + retry_delay. Clear worker and lease_until and append `failed`. Retry_delay is `min(60, 2 ** (attempts - 1))` after the failed or expired attempt; attempts only increases on claim. Both return the updated job.

Extend also requires lease_sec and sets lease_until = max(current lease_until, now + lease_sec), so it cannot shorten a lease. Append `extended` even if the deadline stays unchanged. Return the updated job. Tokens never decrease or reset, including between failed attempts. Neither ack, nack, nor extend reaps other jobs.

`cancel`: requires id. Missing jobs return `not_found`. Queued or leased jobs become cancelled, clear worker and lease_until, and append `cancelled`. Cancelling a cancelled job succeeds without an event. Cancelling done or dead jobs returns `conflict`. Return the job. Dependents of dead or cancelled jobs remain queued but cannot be claimed; there is no automatic cascading cancellation.

`get`: requires id. Return the job or `not_found`; no expiry processing.

`events`: optional after (integer 0..2**63-1, default 0) and limit (integer 1..1000, default 100). Return this tenant's events with seq > after, ascending, up to limit. Each event has exactly `seq`, `tenant`, `id`, `kind`, `attempt`, and `token`. Kind is submitted, claimed, completed, failed, expired, extended, or cancelled. Attempt and token reflect the job after the transition. Seq is a durable, globally increasing integer starting at 1. Successful event appends have no sequence gaps globally. Tenant-filtered results can have gaps caused by other tenants. Submitted is emitted once per new job; all other event rules are above. State and its event must commit together.

## Persistence and concurrency

Multiple independent processes can open the same database, including an empty database at the same time. Operations must behave as if executed in a serial order. A job cannot have two current leases. Simultaneous duplicate submissions create one job and one submitted event. Use SQLite transactions to make mutations, idempotency decisions, and audit events atomic. Temporary lock contention must not appear as a rejected request.

An acknowledged operation must survive SIGKILL and restart. An interrupted operation may commit fully or roll back fully, never partly. Do not keep the only copy of any queue state in memory. You may choose the database schema; no migration or direct SQL interface is required.

Evaluation covers the contract, dependency ordering, idempotency after retries, tenant isolation, lease boundaries and fencing, contention, audit consistency, and process recovery. It uses fresh databases and synthetic data. It also submits 1000 jobs and drains them in under 30 seconds on two CPUs. Each request must finish within 15 seconds. The score is 1 only when every check passes, otherwise 0.
