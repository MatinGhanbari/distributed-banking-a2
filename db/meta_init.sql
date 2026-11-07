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

INSERT INTO map_epoch VALUES (1, 1);
INSERT INTO shard_ranges VALUES (1, 1000, 'A', 1), (1001, 2000, 'B', 1);
INSERT INTO replicas VALUES
    ('bank-a1', 'A', 'http://bank-a1:8000', true),
    ('bank-a2', 'A', 'http://bank-a2:8000', true),
    ('bank-b1', 'B', 'http://bank-b1:8000', true),
    ('bank-b2', 'B', 'http://bank-b2:8000', true);
