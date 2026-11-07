"""Transaction Coordinator: Two-Phase Commit for cross-shard transfers.

Durable state lives in meta-db table `txns` (autocommit => every state change is
committed + fsynced before the next protocol step):

  INITIATED -> PREPARING -> PREPARED -> COMMIT_DECIDED -> COMPLETED (outcome COMMITTED)
                        \\-> ABORT_DECIDED ---------------> COMPLETED (outcome ABORTED)

* The decision (COMMIT_DECIDED / ABORT_DECIDED) is written BEFORE any COMMIT/ABORT is sent.
* Success is reported to the client only after every participant acked COMMIT;
  otherwise the client gets status PENDING + txn_id and can query /txn/<id>.
* Recovery loop (on startup and every RECOVERY_INTERVAL s) finishes every non-COMPLETED
  transaction:  INITIATED/PREPARING -> presumed abort;  PREPARED (all YES recorded) ->
  decide COMMIT;  *_DECIDED -> resend the recorded decision until all acks (a COMMIT
  decision is never replaced by ABORT, even if a participant is down for a long time).
* Fault injection for the experiments: POST /admin/fault {"mode": "..."} or env FAULT_MODE
  modes: after_prepared | after_decision | after_first_ack  -> process exits abruptly.
Assumes a single coordinator instance (as in the assignment topology).
"""
import json
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import Body, FastAPI
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from .common import BankError, check_txn_id, fmt, parse_amount, require_request_id, setup_logging
from .routing import Router

log = setup_logging("coordinator")
META_DSN = os.environ["META_DSN"]
pool = ConnectionPool(META_DSN, min_size=2, max_size=30, kwargs={"autocommit": True, "row_factory": dict_row},
                      open=False)
router = Router(META_DSN, log, request_timeout=10.0)
PHASE2_WAIT = float(os.environ.get("PHASE2_WAIT", "6"))         # how long a client request waits for acks
RECOVERY_INTERVAL = float(os.environ.get("RECOVERY_INTERVAL", "1.5"))
FAULT = {"mode": os.environ.get("FAULT_MODE") or None}
FAULT_MODES = {None, "after_prepared", "after_decision", "after_first_ack"}

active, active_lock = set(), threading.Lock()   # txns currently driven by this process


def claim(txn_id) -> bool:
    with active_lock:
        if txn_id in active:
            return False
        active.add(txn_id)
        return True


def release(txn_id):
    with active_lock:
        active.discard(txn_id)


# ------------------------------------------------------------ durable log helpers

def load(txn_id=None, request_id=None):
    with pool.connection() as c:
        if txn_id:
            return c.execute("SELECT * FROM txns WHERE txn_id = %s", (txn_id,)).fetchone()
        return c.execute("SELECT * FROM txns WHERE request_id = %s", (request_id,)).fetchone()


def set_state(txn_id, state, **fields):
    sets, args = ["state = %s", "updated_at = now()",
                  "history = history || jsonb_build_array(jsonb_build_object('state', %s::text, 'at', "
                  "to_char(clock_timestamp(), 'HH24:MI:SS.MS')))"], [state, state]
    for k, v in fields.items():
        sets.append(f"{k} = %s")
        args.append(Jsonb(v) if isinstance(v, (dict, list)) else v)
    with pool.connection() as c:
        c.execute(f"UPDATE txns SET {', '.join(sets)} WHERE txn_id = %s", (*args, txn_id))
    log.info("TXN %s state -> %s %s", str(txn_id)[:8], state, fields.get("outcome") or "")


def fault(point):
    if FAULT["mode"] == point:
        log.critical("FAULT INJECTION: coordinator crashing at '%s'", point)
        os._exit(1)


def status_body(t):
    if t["state"] == "COMPLETED":
        status = t["outcome"]
    else:
        status = "PENDING"
    return {
        "ok": status == "COMMITTED", "status": status, "txn_id": str(t["txn_id"]),
        "request_id": t["request_id"], "state": t["state"], "outcome": t["outcome"],
        "from": t["from_acct"], "to": t["to_acct"], "amount": fmt(t["amount_cents"]),
        "votes": t["votes"], "acks": t["acks"], "reason": t["reason"], "history": t["history"],
    }


def http_status(body):
    return {"COMMITTED": 200, "ABORTED": 409}.get(body["status"], 202)


# ------------------------------------------------------------ protocol

def run_2pc(txn_id):
    t = load(txn_id)
    set_state(txn_id, "PREPARING")
    votes, all_yes, reason = {}, True, None
    # Participants are prepared sequentially in shard order => locks are always taken in
    # the same global order (shard A row before shard B row) => no distributed deadlock.
    for p in sorted(t["participants"], key=lambda p: p["shard"]):
        try:
            replica, resp = router.call(p["shard"], "POST", "/2pc/prepare",
                                        json={"txn_id": str(txn_id), "account": p["account"],
                                              "delta": p["delta"], "epoch": p["epoch"]})
            b = resp.json()
            vote = b.get("vote") if resp.status_code == 200 else "NO"
            why = b.get("reason") or b.get("error")
            votes[p["shard"]] = {"vote": vote, "reason": why, "replica": replica.id}
        except Exception as e:  # unreachable shard / timeout => NO
            vote, why = "NO", f"{type(e).__name__}: {getattr(e, 'message', e)}"
            votes[p["shard"]] = {"vote": "NO", "reason": why}
        log.info("TXN %s PREPARE shard %s (%s %s) -> %s %s", str(txn_id)[:8], p["shard"], p["account"],
                 fmt(p["delta"]), vote, f"({why})" if vote != "YES" else "")
        if vote != "YES":
            all_yes, reason = False, f"shard {p['shard']} voted NO: {why}"
            break
    if all_yes:
        set_state(txn_id, "PREPARED", votes=votes)
        fault("after_prepared")
        set_state(txn_id, "COMMIT_DECIDED")          # <- durable decision point
        fault("after_decision")
        return finish(txn_id, "COMMIT", PHASE2_WAIT)
    set_state(txn_id, "ABORT_DECIDED", votes=votes, reason=reason)
    return finish(txn_id, "ABORT", PHASE2_WAIT)


def finish(txn_id, decision, wait):
    """Phase 2: deliver the recorded decision to every participant until all ack."""
    t = load(txn_id)
    acks = dict(t["acks"])
    deadline = time.monotonic() + wait
    path = "/2pc/commit" if decision == "COMMIT" else "/2pc/abort"
    while True:
        for p in sorted(t["participants"], key=lambda p: p["shard"]):
            if p["shard"] in acks:
                continue
            try:
                replica, resp = router.call(p["shard"], "POST", path, json={"txn_id": str(txn_id)})
                b = resp.json()
                if resp.status_code == 200 and b.get("ack"):
                    acks[p["shard"]] = replica.id
                    with pool.connection() as c:
                        c.execute("UPDATE txns SET acks = %s, updated_at = now() WHERE txn_id = %s",
                                  (Jsonb(acks), txn_id))
                    log.info("TXN %s %s ack from shard %s (%s)%s", str(txn_id)[:8], decision, p["shard"],
                             replica.id, " [duplicate]" if b.get("duplicate") else "")
                    if decision == "COMMIT":
                        fault("after_first_ack")
                else:
                    log.info("TXN %s %s to shard %s not acked: %s", str(txn_id)[:8], decision, p["shard"], b)
            except Exception as e:
                log.info("TXN %s %s to shard %s failed (%s), will retry", str(txn_id)[:8], decision,
                         p["shard"], getattr(e, "message", e))
        if len(acks) == len(t["participants"]):
            set_state(txn_id, "COMPLETED", outcome="COMMITTED" if decision == "COMMIT" else "ABORTED")
            break
        if time.monotonic() > deadline:
            log.info("TXN %s still waiting for %s acks -> PENDING (recovery loop continues)",
                     str(txn_id)[:8], decision)
            break
        time.sleep(0.5)
    return status_body(load(txn_id))


def recover_one(txn_id):
    t = load(txn_id)
    st = t["state"]
    if st == "COMPLETED":
        return
    log.info("RECOVERY txn %s found in state %s", str(txn_id)[:8], st)
    if st in ("INITIATED", "PREPARING"):      # no decision recorded => presumed abort
        set_state(txn_id, "ABORT_DECIDED", reason="coordinator restarted before a decision")
        st = "ABORT_DECIDED"
    elif st == "PREPARED":                     # all YES votes are durable => continue to commit
        set_state(txn_id, "COMMIT_DECIDED")
        st = "COMMIT_DECIDED"
    finish(txn_id, "COMMIT" if st == "COMMIT_DECIDED" else "ABORT", wait=0)


def recovery_loop():
    while True:
        try:
            with pool.connection() as c:
                ids = [r["txn_id"] for r in c.execute(
                    "SELECT txn_id FROM txns WHERE state <> 'COMPLETED' ORDER BY created_at LIMIT 200")]
            for txn_id in ids:
                if claim(txn_id):
                    try:
                        recover_one(txn_id)
                    finally:
                        release(txn_id)
        except Exception as e:
            log.info("recovery loop error: %s", e)
        time.sleep(RECOVERY_INTERVAL)


# ------------------------------------------------------------ HTTP API

@asynccontextmanager
async def lifespan(_app):
    pool.open(wait=True, timeout=60)
    router.start()
    log.info("coordinator ready (fault mode: %s); starting recovery loop", FAULT["mode"])
    threading.Thread(target=recovery_loop, daemon=True).start()
    yield


app = FastAPI(lifespan=lifespan)


@app.exception_handler(BankError)
async def _bank_error(_req, e: BankError):
    return JSONResponse(status_code=e.status, content=e.body())


@app.get("/health")
def health():
    with pool.connection() as c:
        c.execute("SELECT 1")
    return {"ok": True, "status": "UP", "fault_mode": FAULT["mode"]}


@app.post("/transfer")
def transfer(body: dict = Body(...)):
    rid = require_request_id(body.get("request_id"))
    src, dst, cents = body.get("from"), body.get("to"), parse_amount(body.get("amount"))
    (s1, e1), (s2, e2) = router.shard_for(src), router.shard_for(dst)
    if s1 == s2:
        raise BankError("BAD_REQUEST", 400, "same-shard transfer does not need 2PC")
    participants = [{"shard": s1, "account": src, "delta": -cents, "epoch": e1},
                    {"shard": s2, "account": dst, "delta": cents, "epoch": e2}]
    txn_id = uuid.uuid4()
    claim(txn_id)  # claimed BEFORE the insert so the recovery loop never touches it
    try:
        with pool.connection() as c:
            row = c.execute(
                "INSERT INTO txns (txn_id, request_id, from_acct, to_acct, amount_cents, state, participants, history) "
                "VALUES (%s, %s, %s, %s, %s, 'INITIATED', %s, jsonb_build_array(jsonb_build_object('state', "
                "'INITIATED', 'at', to_char(clock_timestamp(), 'HH24:MI:SS.MS')))) "
                "ON CONFLICT (request_id) DO NOTHING RETURNING txn_id",
                (txn_id, rid, src, dst, cents, Jsonb(participants))).fetchone()
        if row is None:  # same client request id seen before => do not run it twice
            t = load(request_id=rid)
            log.info("DUPLICATE transfer rid=%s -> existing txn %s (%s)", rid, str(t["txn_id"])[:8], t["state"])
            b = {**status_body(t), "duplicate": True}
            return JSONResponse(status_code=http_status(b), content=b)
        log.info("TXN %s INITIATED %s -> %s %s rid=%s", str(txn_id)[:8], src, dst, fmt(cents), rid)
        b = run_2pc(txn_id)
        return JSONResponse(status_code=http_status(b), content=b)
    finally:
        release(txn_id)


@app.get("/txn/{txn_id}")
def txn_status(txn_id: str):
    t = load(txn_id=check_txn_id(txn_id))
    if t is None:
        raise BankError("NO_SUCH_TXN", 404, "unknown transaction id")
    return status_body(t)


@app.get("/txn/by-request/{request_id}")
def txn_by_request(request_id: str):
    t = load(request_id=request_id)
    if t is None:
        raise BankError("NO_SUCH_TXN", 404, "no transaction for this request id")
    return status_body(t)


@app.get("/txns")
def txns(limit: int = 20):
    with pool.connection() as c:
        rows = c.execute("SELECT * FROM txns ORDER BY created_at DESC LIMIT %s", (limit,)).fetchall()
    return [status_body(t) for t in rows]


@app.get("/admin/fault")
def get_fault():
    return {"mode": FAULT["mode"]}


@app.post("/admin/fault")
def set_fault(body: dict = Body(...)):
    mode = body.get("mode")
    if mode not in FAULT_MODES:
        raise BankError("BAD_REQUEST", 400, f"mode must be one of {sorted(m for m in FAULT_MODES if m)} or null")
    FAULT["mode"] = mode
    log.info("FAULT mode set to %s", mode)
    return {"mode": mode}
