-- Metadata database: Shard Map, replica registry and the Coordinator's durable log.

-- Shard Map. One row per account range. `epoch` is the Shard Map epoch at which this
-- range was assigned to its owner; it only ever increases (Bonus I: re-sharding bumps it).
CREATE TABLE shard_ranges (
    range_lo  int  PRIMARY KEY,
    range_hi  int  NOT NULL,
    shard_id  text NOT NULL,
    epoch     bigint NOT NULL,
    CHECK (range_lo <= range_hi)
);

-- Global, monotonically increasing map epoch (single row).
CREATE TABLE map_epoch (
    id     int PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    epoch  bigint NOT NULL
);

-- Replica registry read by the Gateway/Coordinator. Bonus II: the self-healing
-- controller registers/deregisters replacement containers here.
CREATE TABLE replicas (
    replica_id  text PRIMARY KEY,
    shard_id    text NOT NULL,
    url         text NOT NULL,
    enabled     boolean NOT NULL DEFAULT true
);

-- Coordinator durable transaction log (2PC).
CREATE TABLE txns (
    txn_id        uuid PRIMARY KEY,
    request_id    text NOT NULL UNIQUE,         -- client request id => idempotent submit
    from_acct     text NOT NULL,
    to_acct       text NOT NULL,
    amount_cents  bigint NOT NULL CHECK (amount_cents > 0),
    state         text NOT NULL CHECK (state IN ('INITIATED','PREPARING','PREPARED',
                                   'COMMIT_DECIDED','ABORT_DECIDED','COMPLETED')),
    outcome       text CHECK (outcome IN ('COMMITTED','ABORTED')),
    participants  jsonb NOT NULL,
    votes         jsonb NOT NULL DEFAULT '{}',
    acks          jsonb NOT NULL DEFAULT '{}',
    history       jsonb NOT NULL DEFAULT '[]',
    reason        text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX txns_unresolved ON txns (created_at) WHERE state <> 'COMPLETED';

-- Bonus I: persisted re-sharding state machine (one row per migration).
CREATE TABLE migrations (
    id             text PRIMARY KEY,
    range_lo       int NOT NULL,
    range_hi       int NOT NULL,
    source_shard   text NOT NULL,
    target_shard   text NOT NULL,
    new_epoch      bigint NOT NULL,
    phase          text NOT NULL,       -- PLANNED SPLIT SNAPSHOT CATCHUP FENCING CUTOVER_DECIDED ACTIVATED DONE
    slot           text NOT NULL,       -- logical replication slot on the source (CDC)
    snapshot_next  int,                 -- mirror of the target checkpoint (display)
    cdc_lsn        text,                -- mirror of the target checkpoint (display)
    cutover        jsonb,               -- verified counts/checksum + epoch = the cutover decision
    history        jsonb NOT NULL DEFAULT '[]',
    updated_at     timestamptz NOT NULL DEFAULT now()
);

-- Bonus II: desired state for the self-healing controller (one row per shard).
CREATE TABLE replica_sets (
    shard_id  text PRIMARY KEY,
    desired   int  NOT NULL CHECK (desired >= 0),
    image     text NOT NULL,
    db_dsn    text NOT NULL            -- shard DB (replicas' DB_DSN; used to fence dead sessions)
);

-- Bonus II: one row per replica slot = the controller's durable intent.
-- container/replica_id are derived deterministically from (shard, slot, generation), and the
-- row is updated (compare-and-set on generation) BEFORE the container is created => after a
-- controller crash the same name is reused/adopted, never a second container for the slot.
CREATE TABLE replica_slots (
    shard_id    text NOT NULL,
    slot        int  NOT NULL,
    generation  int  NOT NULL,
    replica_id  text NOT NULL,          -- = container name = DNS name on the compose network
    container   text NOT NULL,
    status      text NOT NULL CHECK (status IN ('CREATING','STARTING','RUNNING')),
    ip          text,
    since       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (shard_id, slot)
);

-- Bonus II: append-only audit of controller actions (demo evidence).
CREATE TABLE heal_events (
    id      bigserial PRIMARY KEY,
    at      timestamptz NOT NULL DEFAULT now(),
    event   text NOT NULL,
    detail  text
);

-- Bonus II: one-shot fault injection for the demo (crash right after docker create).
CREATE TABLE controller_flags (key text PRIMARY KEY, value text);

INSERT INTO map_epoch VALUES (1, 1);
INSERT INTO shard_ranges VALUES (1, 1000, 'A', 1), (1001, 2000, 'B', 1);
INSERT INTO replicas VALUES
    ('bank-a1', 'A', 'http://bank-a1:8000', true),
    ('bank-a2', 'A', 'http://bank-a2:8000', true),
    ('bank-b1', 'B', 'http://bank-b1:8000', true),
    ('bank-b2', 'B', 'http://bank-b2:8000', true);

INSERT INTO replica_sets VALUES
    ('A', 2, 'matinghanbari/ds-a2-bank:latest', 'postgresql://bank:bank@db-a:5432/bank'),
    ('B', 2, 'matinghanbari/ds-a2-bank:latest', 'postgresql://bank:bank@db-b:5432/bank');
INSERT INTO replica_slots (shard_id, slot, generation, replica_id, container, status) VALUES
    ('A', 1, 1, 'bank-a1', 'dsbank-bank-a1-1', 'RUNNING'),
    ('A', 2, 1, 'bank-a2', 'dsbank-bank-a2-1', 'RUNNING'),
    ('B', 1, 1, 'bank-b1', 'dsbank-bank-b1-1', 'RUNNING'),
    ('B', 2, 1, 'bank-b2', 'dsbank-bank-b2-1', 'RUNNING');
