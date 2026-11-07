# Distributed Banking Platform – Assignment 2 (core)

Sharding, replication and Two-Phase Commit with Docker Compose on a single host.
Python 3.12 (FastAPI + uvicorn + psycopg 3) and PostgreSQL 16.

```
client.py ──HTTP──► gateway ──(shard map: range → shard, Round Robin over healthy replicas)──┐
                       │                                                                    │
                       └─cross-shard──► coordinator ──2PC──┐                               │
                                          │                 ▼                               ▼
                                       meta-db        bank-a1  bank-a2               bank-b1  bank-b2
                           (shard map, replicas,           \   /                          \   /
                            durable 2PC log)               db-a (A0001–A1000)          db-b (A1001–A2000)
```

| Service | Role |
|---|---|
| `meta-db` | PostgreSQL: Shard Map (`shard_ranges` + `map_epoch`), replica registry (`replicas`), coordinator log (`txns`) |
| `db-a`, `db-b` | PostgreSQL per shard (`max_prepared_transactions=100`); only that shard's accounts |
| `bank-a1/a2`, `bank-b1/b2` | stateless shard replicas sharing their shard DB; also 2PC participants |
| `coordinator` | 2PC coordinator, own container (so it can be stopped alone); admin/status API on host port 8090 |
| `gateway` | the only client entry point, host port 8080 |

## Run

```bash
docker compose up -d --build --wait        # or: demo/reset.sh  (fresh volumes)
python3 client.py balance  A0500
python3 client.py deposit  A0500 50
python3 client.py withdraw A0500 20.50
python3 client.py transfer A0500 A1500 300            # cross-shard => 2PC
python3 client.py transfer A0001 A0002 10             # same shard => one local txn
python3 client.py status   <txn_id>     # or: status --request <request_id>
python3 client.py cluster               # shard map + replica health
docker compose logs -f gateway          # ROUTE lines show shard + replica per request
```

The client uses only the Python standard library. Seed: accounts A0001–A2000, 1000.00 each.

### Experiments

```bash
demo/run_core.sh               # fresh deployment + experiments 1..7 + summary (~1 min)
PAUSE=1 demo/run_core.sh       # waits for Enter before each experiment (video)
demo/run_core.sh --no-reset    # against the running deployment
python3 demo/exp6_coordinator_crash.py   # any single experiment
```

| # | Script | Evidence |
|---|---|---|
| 1 | `exp1_shard_distribution.py` | direct SQL row counts per DB, gateway routing per account, a shard rejecting a foreign account (`WRONG_SHARD`) |
| 2 | `exp2_replica_load.py` | replica id in responses, gateway `ROUTE` log lines, replicas' own logs |
| 3 | `exp3_concurrent_replicas.py` | 200 concurrent deposits/withdrawals via both replicas → exact balance; 30 concurrent withdrawals → no overdraft |
| 4 | `exp4_replica_interruption.py` | `docker compose kill bank-a1` under load → only bank-a2 selected, 0 failed requests, money added == successful deposits; rejoin |
| 5 | `exp5_transfers.py` | same-shard (no coordinator txn) vs cross-shard (state history), duplicate request id, recovery scenario A (YES + NO → abort, `ROLLBACK PREPARED`) |
| 6 | `exp6_coordinator_crash.py` | scenario C: crash after durable `COMMIT_DECIDED` + 1 ack → client sees `UNKNOWN`, DB shows one side committed / other prepared → restart → `COMMITTED`; scenario B: crash after all prepared → restart → continues to commit |
| 7 | `exp7_concurrent_cross_shard.py` | 300 concurrent transfers (both directions, overlapping accounts) → per-account balances match committed transfers, global money conserved, no txn left unresolved |

## Design

### Sharding and Shard Map
Range partitioning on the account number (A0001–A1000 → A, A1001–A2000 → B), stored in
`meta-db.shard_ranges` with a per-range `epoch` and a global `map_epoch`. Gateway and
Coordinator cache it and refresh every 2 s. Each shard DB contains only its own accounts
and an `ownership` table (range, epoch, writable).

### Load balancing and availability
Round Robin over the **healthy** replicas of the owning shard (`bank/routing.py`).
Health: active probe `GET /health` (which also runs `SELECT 1` on the shard DB) every 1 s,
plus passive detection: a transport error while forwarding marks the replica DOWN at once
and the request is retried on the next healthy replica of the same shard. Retrying is safe
because every write carries a request id (below). A recovered replica is selected again
after its next successful probe. The replica list is read from `meta-db.replicas`.

### Concurrency control (replicas share the shard DB)
* Every write is one PostgreSQL transaction that locks the account row(s) with
  `SELECT … FOR UPDATE` before checking and updating the balance → writes from different
  replicas serialize on the row; no lost updates, no overdraft. `CHECK (balance >= 0)` is
  a final guard.
* Same-shard transfer: both rows locked **in account-id order** in one transaction
  (atomic, deadlock-free).
* Cross-shard: the coordinator prepares participants **sequentially in shard order**
  (A then B) and each participant locks one row; prepared transactions never wait, so no
  wait cycle can form. `lock_timeout = 5s` in PREPARE votes NO as a safety net.

### Idempotency (client request ids)
* Local ops: `applied_requests(request_id)` is inserted in the **same** transaction as the
  balance change; a duplicate blocks until the first commits and then replays the stored
  result (`"duplicate": true`). Routing rejections (`421`) are not recorded.
* Cross-shard: `txns.request_id` is UNIQUE; a resubmission returns the existing txn's status.
* The client and gateway generate request ids if the caller does not.

### Two-Phase Commit
States (`meta-db.txns.state`, every change committed before the next step):
`INITIATED → PREPARING → PREPARED → COMMIT_DECIDED → COMPLETED(COMMITTED)` or
`… → ABORT_DECIDED → COMPLETED(ABORTED)`. `votes`, `acks` and a timestamped `history` are stored too.

1. PREPARE: participant (`/2pc/prepare`) checks the epoch fence, locks the row, validates
   (exists, not frozen, sufficient funds), applies the change, inserts
   `twopc_log(txn_id,'PREPARED')` and runs `PREPARE TRANSACTION 'bank_<txn>'` → votes YES.
   Any failure → ROLLBACK → votes NO.
2. Decision is written (`COMMIT_DECIDED` / `ABORT_DECIDED`) **before** any phase-2 message.
3. COMMIT/ABORT are resent until every participant acks. Success is returned only after all
   COMMIT acks; otherwise the client gets `202 PENDING` + `txn_id`, and the recovery loop finishes it.

**Participant idempotency.** `COMMIT PREPARED` for a gid that no longer exists is answered by
looking at `twopc_log`: the row was written *inside* the prepared transaction, so it is
visible iff that transaction committed → duplicate COMMIT is acked. A duplicate ABORT is
acked the same way; an ABORT that arrives before/without PREPARE writes an `ABORTED`
tombstone in `twopc_log`, so a delayed PREPARE for that txn conflicts and votes NO
(no orphan prepared transactions holding locks). Duplicate PREPARE re-votes YES.

**Recovery** (`recovery_loop`, at startup and every 1.5 s, skipping txns this process is
driving): `INITIATED/PREPARING` → presumed abort; `PREPARED` (all YES votes durable) →
decide COMMIT and continue; `COMMIT_DECIDED`/`ABORT_DECIDED` → resend the recorded decision
until all acks. A COMMIT decision is never turned into ABORT, however long a participant is down.

| Scenario | Behaviour | Shown in |
|---|---|---|
| A: one YES, one NO | ABORT_DECIDED, `ROLLBACK PREPARED` on the YES side | exp 5 |
| B: all prepared, coordinator stops | restart → recovery decides COMMIT and completes | exp 6 (part 2) |
| C: COMMIT recorded, not all acked | restart → resends COMMIT, completes; never ABORT | exp 6 (part 1) |

**Status query.** `GET /txn/<txn_id>` or `/txn/by-request/<request_id>` (gateway → coordinator)
returns `COMMITTED`, `ABORTED` or `PENDING`; if the coordinator is unreachable the gateway
answers `UNKNOWN`.

**Fault injection** (experiment 6): `POST :8090/admin/fault {"mode": M}` or env
`FAULT_MODE=M`, `M ∈ after_prepared | after_decision | after_first_ack`; the coordinator
`os._exit(1)`s at that point. Not persisted, so a restart runs normally.

### Durability
PostgreSQL defaults (`fsync=on`, `synchronous_commit=on`), named volumes for all three DBs;
prepared transactions survive DB restarts. Money is integer cents everywhere (amounts are
parsed as `Decimal`, never float), as in Assignment 1.

## Prepared for the bonuses
* **Bonus I (live re-sharding):** persistent Shard Map with per-range and global epoch;
  epoch + `writable` fence checked **inside every shard write transaction** (`ownership …
  FOR SHARE`, so a fencing `UPDATE` waits for in-flight writes and prepared 2PC txns on that
  range); stale/fenced requests get `421` and the gateway refreshes its map and retries;
  client request ids + idempotent ops + txn status query; append-only `ledger` table with an
  ordered `seq` as the change-capture source; meta-db is the place for the persisted
  migration state machine.
* **Bonus II (self-healing):** replicas are stateless, have `/health`, and the gateway reads the
  replica registry from `meta-db.replicas` every 2 s, so a controller can start replacement
  containers via the Docker API and register them without restarting anything. No restart
  policies are used in compose.

## Known limitations (core)
* Single coordinator instance (no coordinator replication / leader election).
* Shard databases are single PostgreSQL instances (DB replication not required).
* While a participant stays prepared (e.g. coordinator down), its row stays locked; other
  writes to that account wait (`lock_timeout` 5 s in 2PC) – inherent to blocking 2PC.
* Container logs are in UTC.

## Layout
```
compose.yml  Dockerfile  requirements.txt  client.py
bank/common.py        ids, money, errors, logging
bank/shard_server.py  shard replica + 2PC participant
bank/routing.py       shard map cache, health checks, Round Robin, failover
bank/gateway.py       client-facing API
bank/coordinator.py   2PC coordinator, durable log, recovery, fault injection
db/meta_init.sql      meta-db schema + shard map + replica registry
db/shard_init.sh      shard schema + seed (only the shard's range)
demo/                 lib.py, exp1..exp7, run_core.sh, reset.sh
```
