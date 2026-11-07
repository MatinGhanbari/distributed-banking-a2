"""Banking shard replica (bank-a1, bank-a2, bank-b1, bank-b2).

Stateless application replica: every replica of a shard shares that shard's PostgreSQL
database, which is the only authority for balances. Concurrency control between
replicas is done in the database:

* row locks  - every write does SELECT ... FOR UPDATE on the account row(s) inside one
               transaction, so concurrent writes from different replicas serialize
               (no lost updates); local transfers lock both rows in id order (no deadlock).
* CHECK (balance_cents >= 0) as a last line of defence.
* epoch fence - every write first reads this shard's `ownership` row FOR SHARE and
               rejects stale/foreign Shard Map epochs (Bonus I re-sharding hook).
* idempotency - client request ids are recorded in `applied_requests` in the SAME
               transaction as the balance change; a retry replays the stored result.

It is also a 2PC participant: /2pc/prepare uses PostgreSQL PREPARE TRANSACTION, and
/2pc/commit|abort are safe to repeat (see twopc_log in db/shard_init.sh).
"""
import os
from contextlib import asynccontextmanager

from fastapi import Body, FastAPI
from fastapi.responses import JSONResponse
from psycopg import errors
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from .common import (BankError, acct_num, check_txn_id, fmt, parse_amount,
                     require_request_id, setup_logging)

REPLICA_ID = os.environ["REPLICA_ID"]
SHARD_ID = os.environ["SHARD_ID"]
log = setup_logging(REPLICA_ID)
pool = ConnectionPool(os.environ["DB_DSN"], min_size=2, max_size=int(os.environ.get("DB_POOL", "30")),
                      kwargs={"autocommit": True}, open=False)


@asynccontextmanager
async def lifespan(_app):
    pool.open(wait=True, timeout=60)
    log.info("replica %s of shard %s ready", REPLICA_ID, SHARD_ID)
    yield
    pool.close()


app = FastAPI(lifespan=lifespan)
CTX = {"replica": REPLICA_ID, "shard": SHARD_ID}


@app.exception_handler(BankError)
async def _bank_error(_req, e: BankError):
    return JSONResponse(status_code=e.status, content=e.body(**CTX))


def ok(**data):
    return {"ok": True, **data, **CTX}


def check_owner(cur, num: int, epoch, lock: bool):
    """Epoch fencing at the shard write path. FOR SHARE blocks a concurrent fencing
    UPDATE of the ownership row until this transaction (or prepared txn) finishes."""
    cur.execute("SELECT epoch, writable FROM ownership WHERE %s BETWEEN range_lo AND range_hi"
                + (" FOR SHARE" if lock else ""), (num,))
    row = cur.fetchone()
    if row is None:
        raise BankError("WRONG_SHARD", 421, f"account number {num} is not owned by shard {SHARD_ID}")
    own_epoch, writable = row
    if epoch is None or int(epoch) < own_epoch:
        raise BankError("STALE_EPOCH", 421, f"request epoch {epoch} < ownership epoch {own_epoch}")
    if not writable:
        raise BankError("FENCED", 421, f"range is fenced on shard {SHARD_ID} (epoch {own_epoch})")


def lock_account(cur, acct: str):
    cur.execute("SELECT balance_cents, frozen FROM accounts WHERE id = %s FOR UPDATE", (acct,))
    row = cur.fetchone()
    if row is None:
        raise BankError("NO_SUCH_ACCOUNT", 404, f"account {acct} does not exist")
    return row


def change(cur, acct, delta, kind, rid=None, txn_id=None) -> int:
    cur.execute("UPDATE accounts SET balance_cents = balance_cents + %s, updated_at = now() "
                "WHERE id = %s RETURNING balance_cents", (delta, acct))
    new = cur.fetchone()[0]
    cur.execute("INSERT INTO ledger (account_id, delta_cents, kind, request_id, txn_id, replica_id) "
                "VALUES (%s, %s, %s, %s, %s, %s)", (acct, delta, kind, rid, txn_id, REPLICA_ID))
    return new


def idempotent(rid: str, op: str, work):
    """Run `work(cur)` once per request id, atomically with recording its result."""
    with pool.connection() as conn:
        with conn.transaction():
            cur = conn.cursor()
            # A concurrent duplicate blocks here until the first one commits.
            cur.execute("INSERT INTO applied_requests (request_id, op) VALUES (%s, %s) "
                        "ON CONFLICT DO NOTHING", (rid, op))
            if cur.rowcount == 0:
                cur.execute("SELECT http_status, response FROM applied_requests WHERE request_id = %s", (rid,))
                status, stored = cur.fetchone()
                log.info("DUPLICATE %s rid=%s -> replaying stored result", op, rid)
                return JSONResponse(status_code=status, content={**stored, "duplicate": True, **CTX})
            try:
                status, body = 200, ok(**work(cur))
            except BankError as e:
                if e.status == 421:
                    raise  # routing errors roll back and are NOT recorded (retry elsewhere)
                status, body = e.status, e.body(**CTX)
            cur.execute("UPDATE applied_requests SET http_status = %s, response = %s WHERE request_id = %s",
                        (status, Jsonb(body), rid))
    return JSONResponse(status_code=status, content=body)


# ---------------------------------------------------------------- local operations

@app.get("/health")
def health():
    try:
        with pool.connection(timeout=2) as conn:
            conn.execute("SELECT 1")
    except Exception as e:  # DB down => replica unhealthy
        return JSONResponse(status_code=503, content={"ok": False, "error": str(e), **CTX})
    return ok(status="UP")


@app.get("/accounts/{acct}")
def balance(acct: str, epoch: int | None = None):
    num = acct_num(acct)
    with pool.connection() as conn, conn.cursor() as cur:
        check_owner(cur, num, epoch, lock=False)
        cur.execute("SELECT balance_cents FROM accounts WHERE id = %s", (acct,))
        row = cur.fetchone()
    if row is None:
        raise BankError("NO_SUCH_ACCOUNT", 404, f"account {acct} does not exist")
    log.info("BALANCE %s = %s", acct, fmt(row[0]))
    return ok(account=acct, balance=fmt(row[0]))


@app.post("/deposit")
def deposit(body: dict = Body(...)):
    acct, rid = body.get("account"), require_request_id(body.get("request_id"))
    num, cents = acct_num(acct), parse_amount(body.get("amount"))

    def work(cur):
        check_owner(cur, num, body.get("epoch"), lock=True)
        _, frozen = lock_account(cur, acct)
        if frozen:
            raise BankError("ACCOUNT_FROZEN", 409, f"account {acct} is frozen")
        new = change(cur, acct, cents, "deposit", rid)
        log.info("DEPOSIT %s +%s -> %s rid=%s", acct, fmt(cents), fmt(new), rid)
        return {"account": acct, "balance": fmt(new)}
    return idempotent(rid, "deposit", work)


@app.post("/withdraw")
def withdraw(body: dict = Body(...)):
    acct, rid = body.get("account"), require_request_id(body.get("request_id"))
    num, cents = acct_num(acct), parse_amount(body.get("amount"))

    def work(cur):
        check_owner(cur, num, body.get("epoch"), lock=True)
        bal, frozen = lock_account(cur, acct)
        if frozen:
            raise BankError("ACCOUNT_FROZEN", 409, f"account {acct} is frozen")
        if bal < cents:
            log.info("WITHDRAW %s -%s REJECTED (balance %s) rid=%s", acct, fmt(cents), fmt(bal), rid)
            raise BankError("INSUFFICIENT_FUNDS", 409, f"balance {fmt(bal)} < {fmt(cents)}", balance=fmt(bal))
        new = change(cur, acct, -cents, "withdraw", rid)
        log.info("WITHDRAW %s -%s -> %s rid=%s", acct, fmt(cents), fmt(new), rid)
        return {"account": acct, "balance": fmt(new)}
    return idempotent(rid, "withdraw", work)


@app.post("/transfer_local")
def transfer_local(body: dict = Body(...)):
    """Same-shard transfer: ONE local ACID transaction, rows locked in id order."""
    src, dst, rid = body.get("from"), body.get("to"), require_request_id(body.get("request_id"))
    n1, n2, cents = acct_num(src), acct_num(dst), parse_amount(body.get("amount"))
    if src == dst:
        raise BankError("BAD_REQUEST", 400, "cannot transfer to the same account")

    def work(cur):
        for n in sorted((n1, n2)):
            check_owner(cur, n, body.get("epoch"), lock=True)
        rows = {}
        for a in sorted((src, dst)):          # deterministic lock order => no deadlock
            rows[a] = lock_account(cur, a)
        if rows[src][1] or rows[dst][1]:
            raise BankError("ACCOUNT_FROZEN", 409, "an account is frozen")
        if rows[src][0] < cents:
            raise BankError("INSUFFICIENT_FUNDS", 409, f"balance {fmt(rows[src][0])} < {fmt(cents)}",
                            balance=fmt(rows[src][0]))
        b1 = change(cur, src, -cents, "transfer", rid)
        b2 = change(cur, dst, cents, "transfer", rid)
        log.info("TRANSFER(local) %s -> %s %s  [%s=%s, %s=%s] rid=%s",
                 src, dst, fmt(cents), src, fmt(b1), dst, fmt(b2), rid)
        return {"from": src, "to": dst, "amount": fmt(cents), "from_balance": fmt(b1), "to_balance": fmt(b2)}
    return idempotent(rid, "transfer", work)


# ---------------------------------------------------------------- 2PC participant

def gid_of(txn_id):
    return "bank_" + check_txn_id(txn_id)


def twopc_kind(cur, txn_id):
    cur.execute("SELECT kind FROM twopc_log WHERE txn_id = %s", (txn_id,))
    row = cur.fetchone()
    return row[0] if row else None


@app.post("/2pc/prepare")
def prepare(body: dict = Body(...)):
    """Phase 1: validate, apply the change and PREPARE TRANSACTION (durable, not committed).
    Votes YES only after the prepare is on disk. Repeating PREPARE is safe."""
    txn_id, acct, epoch = body.get("txn_id"), body.get("account"), body.get("epoch")
    gid, num, delta = gid_of(txn_id), acct_num(acct), int(body.get("delta"))
    with pool.connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_prepared_xacts WHERE gid = %s", (gid,))
        if cur.fetchone():
            return ok(vote="YES", duplicate=True)
        kind = twopc_kind(cur, txn_id)
        if kind == "PREPARED":
            return ok(vote="YES", duplicate=True, note="already committed")
        if kind == "ABORTED":
            return ok(vote="NO", reason="ALREADY_ABORTED")
        try:
            cur.execute("BEGIN")
            cur.execute("SET LOCAL lock_timeout = '5s'")
            check_owner(cur, num, epoch, lock=True)
            bal, frozen = lock_account(cur, acct)
            if frozen:
                raise BankError("ACCOUNT_FROZEN", 409, f"account {acct} is frozen")
            if bal + delta < 0:
                raise BankError("INSUFFICIENT_FUNDS", 409, f"balance {fmt(bal)} < {fmt(-delta)}")
            new = change(cur, acct, delta, "2pc", txn_id=txn_id)
            cur.execute("INSERT INTO twopc_log (txn_id, kind, account_id, delta_cents) "
                        "VALUES (%s, 'PREPARED', %s, %s)", (txn_id, acct, delta))
            cur.execute(f"PREPARE TRANSACTION '{gid}'")
        except (BankError, errors.LockNotAvailable, errors.UniqueViolation) as e:
            cur.execute("ROLLBACK")
            reason = e.code if isinstance(e, BankError) else type(e).__name__
            log.info("2PC PREPARE txn=%s %s %s -> vote NO (%s)", txn_id[:8], acct, fmt(delta), reason)
            return ok(vote="NO", reason=reason)
    log.info("2PC PREPARE txn=%s %s %s (would be %s) -> PREPARED, vote YES",
             txn_id[:8], acct, fmt(delta), fmt(new))
    return ok(vote="YES")


@app.post("/2pc/commit")
def commit(body: dict = Body(...)):
    txn_id = body.get("txn_id")
    gid = gid_of(txn_id)
    with pool.connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(f"COMMIT PREPARED '{gid}'")
        except errors.UndefinedObject:
            # Not prepared any more: committed already (row visible) or something is wrong.
            if twopc_kind(cur, txn_id) == "PREPARED":
                log.info("2PC COMMIT txn=%s -> already committed (duplicate COMMIT, ack)", txn_id[:8])
                return ok(ack=True, duplicate=True)
            raise BankError("UNKNOWN_TXN", 409, "no prepared or committed transaction with this id")
        except errors.ObjectInUse:
            raise BankError("BUSY", 503, "prepared transaction busy, retry")
    log.info("2PC COMMIT txn=%s -> COMMITTED", txn_id[:8])
    return ok(ack=True)


@app.post("/2pc/abort")
def abort(body: dict = Body(...)):
    txn_id = body.get("txn_id")
    gid = gid_of(txn_id)
    with pool.connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(f"ROLLBACK PREPARED '{gid}'")
            log.info("2PC ABORT txn=%s -> prepared transaction ROLLED BACK", txn_id[:8])
            return ok(ack=True)
        except errors.UndefinedObject:
            pass
        except errors.ObjectInUse:
            raise BankError("BUSY", 503, "prepared transaction busy, retry")
        kind = twopc_kind(cur, txn_id)
        if kind == "PREPARED":  # would mean a COMMIT decision existed: protocol violation
            raise BankError("ALREADY_COMMITTED", 409, "transaction already committed here")
        if kind == "ABORTED":
            return ok(ack=True, duplicate=True)
        # Never prepared here: write a tombstone so a delayed PREPARE votes NO.
        try:
            cur.execute("BEGIN")
            cur.execute("SET LOCAL lock_timeout = '2s'")
            cur.execute("INSERT INTO twopc_log (txn_id, kind) VALUES (%s, 'ABORTED')", (txn_id,))
            cur.execute("COMMIT")
        except (errors.LockNotAvailable, errors.UniqueViolation):
            cur.execute("ROLLBACK")  # a PREPARE is in flight right now -> coordinator retries
            raise BankError("BUSY", 503, "prepare in progress, retry abort")
    log.info("2PC ABORT txn=%s -> not prepared here, tombstone written", txn_id[:8])
    return ok(ack=True, tombstone=True)
