#!/bin/bash
# Shard database init (runs once, by the postgres image entrypoint).
# Env: SHARD_ID, RANGE_LO, RANGE_HI  -> only this shard's accounts are created here.
set -euo pipefail
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<SQL
CREATE TABLE accounts (
    id             text PRIMARY KEY,               -- 'A0001'
    num            int  NOT NULL UNIQUE,           -- 1  (used for range checks)
    balance_cents  bigint NOT NULL CHECK (balance_cents >= 0),
    frozen         boolean NOT NULL DEFAULT false, -- frozen accounts reject writes / vote NO
    updated_at     timestamptz NOT NULL DEFAULT now()
);

-- Ranges this shard owns + the Shard Map epoch of that ownership. Every write checks
-- this row (FOR SHARE) inside its transaction => epoch fencing at the shard write path.
CREATE TABLE ownership (
    range_lo  int PRIMARY KEY,
    range_hi  int NOT NULL,
    epoch     bigint NOT NULL,
    writable  boolean NOT NULL DEFAULT true
);

-- Client request ids already applied (idempotent deposit/withdraw/local transfer).
CREATE TABLE applied_requests (
    request_id   text PRIMARY KEY,
    op           text NOT NULL,
    http_status  int,
    response     jsonb,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- Participant-side 2PC record. kind='PREPARED' is written INSIDE the prepared
-- transaction: visible <=> that transaction committed. kind='ABORTED' is a tombstone
-- written when ABORT arrives before/without PREPARE, so a late PREPARE votes NO.
CREATE TABLE twopc_log (
    txn_id       text PRIMARY KEY,
    kind         text NOT NULL CHECK (kind IN ('PREPARED','ABORTED')),
    account_id   text,
    delta_cents  bigint,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- Append-only journal of every balance change (audit; ordered seq = CDC source for Bonus I).
CREATE TABLE ledger (
    seq          bigserial PRIMARY KEY,
    account_id   text NOT NULL,
    delta_cents  bigint NOT NULL,
    kind         text NOT NULL,       -- deposit | withdraw | transfer | 2pc
    request_id   text,
    txn_id       text,
    replica_id   text,
    at           timestamptz NOT NULL DEFAULT now()
);

INSERT INTO accounts (id, num, balance_cents)
SELECT 'A' || lpad(n::text, 4, '0'), n, 100000          -- 1000.00 each
FROM generate_series(${RANGE_LO}, ${RANGE_HI}) AS n;

INSERT INTO ownership VALUES (${RANGE_LO}, ${RANGE_HI}, 1, true);
SQL
echo "shard ${SHARD_ID}: seeded accounts ${RANGE_LO}..${RANGE_HI}"
