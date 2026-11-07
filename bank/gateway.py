"""Shard-Aware Gateway: the only endpoint clients talk to.

For every request: (1) shard selection by account ownership (Shard Map), (2) replica
selection among that shard's healthy replicas (Round Robin, see routing.py).
Cross-shard transfers are handed to the Coordinator (2PC). Every routed request is
logged as  ROUTE op=... acct=... shard=... replica=...  (used by the experiments).
"""
import os
import time
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import Body, FastAPI
from fastapi.responses import JSONResponse

from .common import BankError, acct_num, fmt, parse_amount, setup_logging
from .routing import Router

log = setup_logging("gateway")
router = Router(os.environ["META_DSN"], log, request_timeout=8.0)
COORDINATOR = os.environ.get("COORDINATOR_URL", "http://coordinator:8000")
coord = httpx.Client(timeout=httpx.Timeout(30.0, connect=2.0))
# How long a request may wait while its range is fenced for a re-sharding cutover.
FENCE_WAIT = float(os.environ.get("FENCE_WAIT", "20"))


@asynccontextmanager
async def lifespan(_app):
    router.start()
    log.info("gateway ready")
    yield


app = FastAPI(lifespan=lifespan)


@app.exception_handler(BankError)
async def _bank_error(_req, e: BankError):
    return JSONResponse(status_code=e.status, content=e.body())


def forward(op, acct, method, path, payload=None, epoch_accts=()):
    """Route to the owning shard. A 421 (WRONG_SHARD / STALE_EPOCH / FENCED) means our Shard
    Map is stale or a cutover is in progress: nothing was applied, so refresh the map and
    retry (bounded by FENCE_WAIT) - this is the brief pause of a migrating range."""
    deadline, logged = time.monotonic() + FENCE_WAIT, False
    while True:
        shard, epoch = router.shard_for(acct)
        for a in epoch_accts:  # local transfer: send the highest epoch of the ranges involved
            epoch = max(epoch, router.shard_for(a)[1])
        if payload is not None:
            replica, resp = router.call(shard, method, path, json={**payload, "epoch": epoch})
        else:
            replica, resp = router.call(shard, method, path, params={"epoch": epoch})
        if resp.status_code == 421 and time.monotonic() < deadline:
            if not logged:
                log.info("ROUTE op=%s acct=%s rejected by %s (%s) -> refresh shard map + retry",
                         op, acct, replica.id, resp.json().get("error"))
                logged = True
            time.sleep(0.3)
            router.refresh()
            if op == "transfer" and payload and router.shard_for(payload["from"])[0] != router.shard_for(payload["to"])[0]:
                return None  # ownership changed under us: the caller re-decides local vs 2PC
            continue
        break
    body = resp.json()
    log.info("ROUTE op=%s acct=%s shard=%s replica=%s epoch=%s -> %s %s", op, acct, shard, replica.id,
             epoch, resp.status_code, body.get("error", ""))
    body.update(shard=shard, replica=replica.id, routing="local")
    return JSONResponse(status_code=resp.status_code, content=body)


def write_payload(body, *fields):
    rid = body.get("request_id") or f"gw-{uuid.uuid4()}"
    p = {f: body.get(f) for f in fields}
    p["amount"] = fmt(parse_amount(body.get("amount")))  # validate before routing
    p["request_id"] = rid
    return p


@app.get("/health")
def health():
    return {"ok": True, "status": "UP"}


@app.get("/cluster")
def cluster():
    return router.snapshot()


@app.get("/balance/{acct}")
def balance(acct: str):
    return forward("balance", acct, "GET", f"/accounts/{acct}")


@app.post("/deposit")
def deposit(body: dict = Body(...)):
    p = write_payload(body, "account")
    return forward("deposit", p["account"], "POST", "/deposit", p)


@app.post("/withdraw")
def withdraw(body: dict = Body(...)):
    p = write_payload(body, "account")
    return forward("withdraw", p["account"], "POST", "/withdraw", p)


@app.post("/transfer")
def transfer(body: dict = Body(...)):
    p = write_payload(body, "from", "to")
    acct_num(p["from"]), acct_num(p["to"])
    (s1, _), (s2, _) = router.shard_for(p["from"]), router.shard_for(p["to"])
    if s1 == s2:
        # same shard => one local ACID transaction on that shard, no coordinator needed
        r = forward("transfer", p["from"], "POST", "/transfer_local", p, epoch_accts=(p["to"],))
        if r is not None:
            return r
        (s1, _), (s2, _) = router.shard_for(p["from"]), router.shard_for(p["to"])
    try:
        resp = coord.post(COORDINATOR + "/transfer", json=p)
        b = resp.json()
    except (httpx.TransportError, ValueError) as e:  # ValueError: answer not from the coordinator
        log.info("ROUTE op=transfer %s->%s kind=2pc coordinator UNREACHABLE (%s) rid=%s",
                 p["from"], p["to"], type(e).__name__, p["request_id"])
        return JSONResponse(status_code=502, content={
            "ok": False, "status": "UNKNOWN", "request_id": p["request_id"], "routing": "2pc",
            "message": "coordinator unreachable, outcome unknown; query /txn/by-request/<request_id>"})
    log.info("ROUTE op=transfer %s(shard %s)->%s(shard %s) kind=2pc txn=%s -> %s", p["from"], s1, p["to"], s2,
             (b.get("txn_id") or "")[:8], b.get("status") or b.get("error"))
    b["routing"] = "2pc"
    return JSONResponse(status_code=resp.status_code, content=b)


def coord_get(path):
    try:
        resp = coord.get(COORDINATOR + path, timeout=5)
        return JSONResponse(status_code=resp.status_code, content=resp.json())
    except (httpx.TransportError, ValueError):
        return JSONResponse(status_code=503, content={
            "ok": False, "status": "UNKNOWN", "message": "coordinator unreachable; outcome not known yet"})


@app.get("/txn/by-request/{request_id}")
def txn_by_request(request_id: str):
    return coord_get(f"/txn/by-request/{request_id}")


@app.get("/txn/{txn_id}")
def txn(txn_id: str):
    return coord_get(f"/txn/{txn_id}")
