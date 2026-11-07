"""Live re-sharding controller (Bonus I): move accounts [RANGE_LO, RANGE_HI] from the source
shard to the target shard while the platform keeps serving them.

Crash-consistent state machine, persisted in meta-db.migrations (phase + history + cutover
decision). Every step is idempotent, so after an abrupt kill the controller is simply started
again and continues from the persisted phase:

  PLANNED          new epoch = map_epoch + 1; register target replicas; target ownership
                   row (epoch, writable=false)
  SPLIT            split the range out of the source's Shard Map / ownership rows;
                   create the logical replication slot (CDC start position) BEFORE the snapshot
  SNAPSHOT         copy rows from a REPEATABLE READ snapshot in bounded batches; checkpoint
                   (next account number) is written on the target IN THE SAME transaction
  CATCHUP          replay committed changes from the slot (test_decoding, commit order) as
                   after-images; checkpoint = commit LSN, stored atomically with the rows;
                   slot advanced afterwards (durable position, replay is idempotent)
  FENCING          source ownership row -> writable=false, epoch=new (waits for in-flight
                   writes AND prepared 2PC txns, they hold FOR SHARE on it); drain CDC up to
                   the WAL position after the fence; copy request ids; verify count/sum/md5;
                   record the cutover decision
  CUTOVER_DECIDED  target ownership writable=true, then ONE meta-db transaction switches the
                   Shard Map range to the target, bumps map_epoch and records ACTIVATED
  ACTIVATED        verify the source range is unchanged since the cutover, drop the slot,
                   delete the old rows from the source (only now)
  DONE

Source writes are fenced before the target becomes writable => never two writable owners.
Replay from the slot start LSN (created before any snapshot) with after-images makes
re-copying / re-replaying after a crash safe, even when the snapshot is resumed later.
"""
import os
import re
import sys
import time

import psycopg
from psycopg import errors
from psycopg.types.json import Jsonb

from .common import setup_logging

log = setup_logging("migrator")
MID = os.environ.get("MIGRATION_ID", "m1")
LO, HI = int(os.environ.get("RANGE_LO", "1501")), int(os.environ.get("RANGE_HI", "2000"))
SRC, DST = os.environ.get("SOURCE_SHARD", "B"), os.environ.get("TARGET_SHARD", "C")
SLOT = f"mig_{MID}"
SNAPSHOT_BATCH = int(os.environ.get("SNAPSHOT_BATCH", "100"))
SNAPSHOT_SLEEP = float(os.environ.get("SNAPSHOT_SLEEP", "0"))
CDC_BATCH = int(os.environ.get("CDC_BATCH", "200"))
CATCHUP_MIN = float(os.environ.get("CATCHUP_MIN", "0"))
FENCE_HOLD = float(os.environ.get("FENCE_HOLD", "0"))
CLEANUP_DELAY = float(os.environ.get("CLEANUP_DELAY", "0"))
TARGET_REPLICAS = [kv.split("=", 1) for kv in os.environ.get("TARGET_REPLICAS", "").split(",") if kv]

COL_RE = re.compile(r"(\w+)\[[^\]]+\]:('(?:[^']|'')*'|\S+)")


def connect(dsn):
    while True:
        try:
            return psycopg.connect(dsn, autocommit=True, connect_timeout=3)
        except psycopg.OperationalError as e:
            log.info("waiting for %s: %s", dsn.split("@")[-1], e)
            time.sleep(1)


meta, src, dst = connect(os.environ["META_DSN"]), connect(os.environ["SOURCE_DSN"]), connect(os.environ["TARGET_DSN"])


def lsn_int(lsn):
    hi, lo = lsn.split("/")
    return (int(hi, 16) << 32) + int(lo, 16)


def mig():
    return meta.execute("SELECT phase, new_epoch, cutover FROM migrations WHERE id = %s", (MID,)).fetchone()


def set_phase(phase, conn=None, **fields):
    sets = ["phase = %s", "updated_at = now()",
            "history = history || jsonb_build_array(jsonb_build_object('phase', %s::text, 'at', "
            "to_char(clock_timestamp(), 'HH24:MI:SS.MS')))"]
    args = [phase, phase]
    for k, v in fields.items():
        sets.append(f"{k} = %s")
        args.append(Jsonb(v) if isinstance(v, (dict, list)) else v)
    (conn or meta).execute(f"UPDATE migrations SET {', '.join(sets)} WHERE id = %s", (*args, MID))
    log.info("PHASE -> %s", phase)


def checksum(conn):
    n, total, md5 = conn.execute(
        "SELECT count(*), coalesce(sum(balance_cents), 0), "
        "md5(coalesce(string_agg(id || ':' || balance_cents || ':' || frozen, ',' ORDER BY id), '')) "
        "FROM accounts WHERE num BETWEEN %s AND %s", (LO, HI)).fetchone()
    return {"count": n, "sum_cents": int(total), "md5": md5}


def progress():
    row = dst.execute("SELECT snapshot_next, cdc_lsn::text FROM migration_progress WHERE migration_id = %s",
                      (MID,)).fetchone()
    return row or (LO, None)


def split_range(conn, table, extra_where=""):
    """Split the row containing [LO, HI] so that [LO, HI] is its own row (idempotent)."""
    with conn.transaction():
        conn.execute("SET LOCAL lock_timeout = '3s'")
        cols = "range_lo, range_hi, epoch" + (", writable" if table == "ownership" else ", shard_id")
        row = conn.execute(f"SELECT {cols} FROM {table} WHERE %s BETWEEN range_lo AND range_hi {extra_where} "
                           "FOR UPDATE", (LO,)).fetchone()
        lo, hi, epoch, x = row
        if (lo, hi) == (LO, HI):
            return
        assert lo <= LO and HI <= hi, "range must lie inside one existing range"
        conn.execute(f"DELETE FROM {table} WHERE range_lo = %s", (lo,))
        for a, b in ((lo, LO - 1), (LO, HI), (HI + 1, hi)):
            if a <= b:
                if table == "ownership":
                    conn.execute("INSERT INTO ownership VALUES (%s, %s, %s, %s)", (a, b, epoch, x))
                else:
                    conn.execute("INSERT INTO shard_ranges VALUES (%s, %s, %s, %s)", (a, b, x, epoch))
    log.info("split %s: A%04d-A%04d is now its own range", table, LO, HI)


def retry_locked(fn, what):
    while True:
        try:
            return fn()
        except errors.LockNotAvailable:
            log.info("%s waiting for in-flight writes / prepared 2PC txns on the range ...", what)
            time.sleep(0.5)


# ------------------------------------------------------------------ CDC

def parse_change(data):
    """test_decoding line -> (id, num, balance, frozen) for table accounts, else None."""
    if not data.startswith("table public.accounts: ") or ": DELETE:" in data:
        return None
    cols = {k: (v[1:-1].replace("''", "'") if v.startswith("'") else v) for k, v in COL_RE.findall(data)}
    return cols["id"], int(cols["num"]), int(cols["balance_cents"]), cols["frozen"] == "true"


def cdc_round(upto_lsn=None):
    """Replay one bounded batch of committed source transactions. Returns #txns replayed."""
    _, ckpt = progress()
    ck = lsn_int(ckpt) if ckpt else -1
    rows = src.execute("SELECT lsn::text, data FROM pg_logical_slot_peek_changes(%s, %s::pg_lsn, %s, "
                       "'include-xids', '0', 'skip-empty-xacts', '1')", (SLOT, upto_lsn, CDC_BATCH)).fetchall()
    txns, cur = [], []
    for lsn, data in rows:            # group into transactions (commit order)
        if data.startswith("BEGIN"):
            cur = []
        elif data.startswith("COMMIT"):
            txns.append((lsn, cur))
        else:
            cur.append(data)
    txns = [(lsn, ch) for lsn, ch in txns if lsn_int(lsn) > ck]  # skip already-applied txns
    if not txns:
        return 0
    images = {}
    for _, changes in txns:
        for data in changes:
            c = parse_change(data)
            if c and LO <= c[1] <= HI:
                images[c[0]] = c          # after-image; later commits overwrite earlier ones
    last = txns[-1][0]
    with dst.transaction():           # rows + checkpoint atomically on the target
        with dst.cursor() as cur:
            cur.executemany("INSERT INTO accounts (id, num, balance_cents, frozen) VALUES (%s, %s, %s, %s) "
                            "ON CONFLICT (id) DO UPDATE SET balance_cents = EXCLUDED.balance_cents, "
                            "frozen = EXCLUDED.frozen, updated_at = now()", list(images.values()))
        dst.execute("UPDATE migration_progress SET cdc_lsn = %s::pg_lsn WHERE migration_id = %s", (last, MID))
    src.execute("SELECT pg_replication_slot_advance(%s, %s::pg_lsn)", (SLOT, last))  # release WAL
    meta.execute("UPDATE migrations SET cdc_lsn = %s, updated_at = now() WHERE id = %s", (last, MID))
    log.info("CDC replayed %d txns (%d rows of A%04d-A%04d changed), checkpoint lsn=%s",
             len(txns), len(images), LO, HI, last)
    return len(txns)


# ------------------------------------------------------------------ phases

def do_planned():
    epoch = meta.execute("SELECT epoch FROM map_epoch").fetchone()[0] + 1
    meta.execute("INSERT INTO migrations (id, range_lo, range_hi, source_shard, target_shard, new_epoch, phase, slot, "
                 "history) VALUES (%s, %s, %s, %s, %s, %s, 'PLANNED', %s, jsonb_build_array(jsonb_build_object("
                 "'phase', 'PLANNED', 'at', to_char(clock_timestamp(), 'HH24:MI:SS.MS')))) ON CONFLICT DO NOTHING",
                 (MID, LO, HI, SRC, DST, epoch, SLOT))
    epoch = mig()[1]
    for rid, url in TARGET_REPLICAS:
        meta.execute("INSERT INTO replicas VALUES (%s, %s, %s, true) ON CONFLICT (replica_id) DO UPDATE "
                     "SET shard_id = EXCLUDED.shard_id, url = EXCLUDED.url, enabled = true", (rid, DST, url))
    dst.execute("INSERT INTO ownership VALUES (%s, %s, %s, false) ON CONFLICT DO NOTHING", (LO, HI, epoch))
    dst.execute("INSERT INTO migration_progress VALUES (%s, %s, NULL) ON CONFLICT DO NOTHING", (MID, LO))
    log.info("migration %s: A%04d-A%04d  %s -> %s, new epoch %s; target replicas registered", MID, LO, HI,
             SRC, DST, epoch)
    set_phase("SPLIT")


def do_split():
    retry_locked(lambda: split_range(meta, "shard_ranges"), "split shard map")
    retry_locked(lambda: split_range(src, "ownership"), "split source ownership")
    if not src.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (SLOT,)).fetchone():
        lsn = src.execute("SELECT lsn::text FROM pg_create_logical_replication_slot(%s, 'test_decoding')",
                          (SLOT,)).fetchone()[0]
        log.info("CDC slot %s created at lsn %s (before the snapshot)", SLOT, lsn)
    set_phase("SNAPSHOT")


def do_snapshot():
    nxt, _ = progress()
    if nxt > LO:
        log.info("RESUME snapshot at A%04d (checkpoint on target); new snapshot, CDC from slot start covers the gap", nxt)
    with psycopg.connect(os.environ["SOURCE_DSN"]) as snap:   # one REPEATABLE READ snapshot
        snap.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        snap.read_only = True
        while nxt <= HI:
            rows = snap.execute("SELECT id, num, balance_cents, frozen FROM accounts WHERE num BETWEEN %s AND %s "
                                "ORDER BY num LIMIT %s", (nxt, HI, SNAPSHOT_BATCH)).fetchall()
            new_next = (rows[-1][1] + 1) if rows else HI + 1
            with dst.transaction():
                with dst.cursor() as cur:
                    cur.executemany("INSERT INTO accounts (id, num, balance_cents, frozen) VALUES (%s, %s, %s, %s) "
                                    "ON CONFLICT (id) DO UPDATE SET balance_cents = EXCLUDED.balance_cents, "
                                    "frozen = EXCLUDED.frozen", rows)
                dst.execute("UPDATE migration_progress SET snapshot_next = %s WHERE migration_id = %s", (new_next, MID))
            meta.execute("UPDATE migrations SET snapshot_next = %s, updated_at = now() WHERE id = %s", (new_next, MID))
            if rows:
                log.info("SNAPSHOT copied %s..%s (%d rows), checkpoint next=A%04d", rows[0][0], rows[-1][0],
                         len(rows), new_next)
            nxt = new_next
            time.sleep(SNAPSHOT_SLEEP)
    set_phase("CATCHUP")


def do_catchup():
    t0 = time.monotonic()
    while True:
        n = cdc_round()
        if n < CDC_BATCH and time.monotonic() - t0 >= CATCHUP_MIN:
            break  # lag is small: one more (fenced) drain will finish it quickly
        time.sleep(0.3)
    set_phase("FENCING")


def do_fencing():
    epoch = mig()[1]

    def fence():
        with src.transaction():
            src.execute("SET LOCAL lock_timeout = '2s'")
            src.execute("UPDATE ownership SET writable = false, epoch = GREATEST(epoch, %s) WHERE range_lo = %s",
                        (epoch, LO))
    retry_locked(fence, "FENCE")
    target = src.execute("SELECT pg_current_wal_lsn()::text").fetchone()[0]
    log.info("FENCED A%04d-A%04d on shard %s (writable=false, epoch %s); final drain up to lsn %s",
             LO, HI, SRC, epoch, target)
    time.sleep(FENCE_HOLD)
    while cdc_round(upto_lsn=target):
        pass
    # request ids touching the range (exactly-once for retries routed to the new owner)
    rows = src.execute("SELECT request_id, op, accounts, http_status, response FROM applied_requests r WHERE EXISTS "
                       "(SELECT 1 FROM unnest(r.accounts) a WHERE substr(a, 2)::int BETWEEN %s AND %s)",
                       (LO, HI)).fetchall()
    with dst.cursor() as cur:
        cur.executemany("INSERT INTO applied_requests (request_id, op, accounts, http_status, response) "
                        "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                        [(a, b, c, d, Jsonb(e)) for a, b, c, d, e in rows])
    s_sum, d_sum = checksum(src), checksum(dst)
    if s_sum != d_sum:  # should not happen; source is fenced => a direct copy is exact
        log.info("VERIFY mismatch %s vs %s -> direct re-copy from fenced source", s_sum, d_sum)
        rows = src.execute("SELECT id, num, balance_cents, frozen FROM accounts WHERE num BETWEEN %s AND %s",
                           (LO, HI)).fetchall()
        with dst.transaction(), dst.cursor() as cur:
            cur.executemany("INSERT INTO accounts (id, num, balance_cents, frozen) VALUES (%s, %s, %s, %s) "
                            "ON CONFLICT (id) DO UPDATE SET balance_cents = EXCLUDED.balance_cents, "
                            "frozen = EXCLUDED.frozen", rows)
        d_sum = checksum(dst)
        if s_sum != d_sum:
            raise SystemExit("verification failed twice; refusing to cut over")
    log.info("VERIFY source == target: %d accounts, sum %s, md5 %s; %d request ids copied",
             s_sum["count"], s_sum["sum_cents"], s_sum["md5"][:12], len(rows))
    set_phase("CUTOVER_DECIDED", cutover={**s_sum, "epoch": epoch, "fence_lsn": target})


def do_cutover():
    epoch = mig()[1]
    dst.execute("UPDATE ownership SET writable = true, epoch = %s WHERE range_lo = %s", (epoch, LO))
    with meta.transaction():  # atomic Shard Map switch + phase record
        meta.execute("UPDATE shard_ranges SET shard_id = %s, epoch = %s WHERE range_lo = %s", (DST, epoch, LO))
        meta.execute("UPDATE map_epoch SET epoch = GREATEST(epoch, %s)", (epoch,))
        set_phase("ACTIVATED")
    log.info("ACTIVATED: Shard Map epoch %s, A%04d-A%04d owned by shard %s", epoch, LO, HI, DST)


def do_cleanup():
    time.sleep(CLEANUP_DELAY)
    cut = mig()[2]
    now_src = checksum(src)
    if now_src["count"]:
        if {k: now_src[k] for k in ("count", "sum_cents", "md5")} != {k: cut[k] for k in ("count", "sum_cents", "md5")}:
            raise SystemExit(f"source range changed after cutover?! {now_src} vs {cut}; not deleting")
        n_dst = dst.execute("SELECT count(*) FROM accounts WHERE num BETWEEN %s AND %s", (LO, HI)).fetchone()[0]
        if n_dst != cut["count"]:
            raise SystemExit("target row count differs from the verified cutover; not deleting")
        log.info("CLEANUP verified: source unchanged since cutover (md5 %s), target has %d rows",
                 cut["md5"][:12], n_dst)
    if src.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (SLOT,)).fetchone():
        src.execute("SELECT pg_drop_replication_slot(%s)", (SLOT,))
    n = src.execute("DELETE FROM accounts WHERE num BETWEEN %s AND %s", (LO, HI)).rowcount
    log.info("CLEANUP dropped CDC slot, deleted %d old rows from shard %s (fenced ownership row kept)", n, SRC)
    set_phase("DONE")


STEPS = {"PLANNED": do_planned, "SPLIT": do_split, "SNAPSHOT": do_snapshot, "CATCHUP": do_catchup,
         "FENCING": do_fencing, "CUTOVER_DECIDED": do_cutover, "ACTIVATED": do_cleanup}


def main():
    m = mig()
    if m:
        snap, lsn = progress()
        log.info("RESUME migration %s in phase %s (snapshot next=A%04d, cdc lsn=%s)", MID, m[0], snap, lsn)
    else:
        do_planned()
    while (phase := mig()[0]) != "DONE":
        STEPS[phase]()
    log.info("migration %s DONE", MID)


if __name__ == "__main__":
    sys.exit(main())
