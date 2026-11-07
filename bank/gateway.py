"""Shard-Aware Gateway: the only endpoint clients talk to.

For every request: (1) shard selection by account ownership (Shard Map), (2) replica
selection among that shard's healthy replicas (Round Robin, see routing.py).
Cross-shard transfers are handed to the Coordinator (2PC). Every routed request is
logged as  ROUTE op=... acct=... shard=... replica=...  (used by the experiments).
"""
import os
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import Body, FastAPI
from fastapi.responses import JSONResponse

from .common import BankError, acct_num, fmt, parse_amount, setup_logging
from .routing import Router

log = setup_logging("gateway")
router = Router(os.environ["META_DSN"], log)
COORDINATOR = os.environ.get("COORDINATOR_URL", "http://coordinator:8000")
coord = httpx.Client(timeout=httpx.Timeout(30.0, connect=2.0))


@asynccontextmanager
async def lifespan(_app):
    router.start()
    log.info("gateway ready")
    yield


app = FastAPI(lifespan=lifespan)


@app.exception_handler(BankError)
async def _bank_error(_req, e: BankError):
    return JSONResponse(status_code=e.status, content=e.body())


def forward(op, acct, method, path, payload=None):
    """Route to the owning shard; on a fencing/epoch rejection (421) refresh the map once."""
    for attempt in (1, 2):
        shard, epoch = router.shard_for(acct)
        if payload is not None:
            replica, resp = router.call(shard, method, path, json={**payload, "epoch": epoch})
        else:
            replica, resp = router.call(shard, method, path, params={"epoch": epoch})
        if resp.status_code == 421 and attempt == 1:
            log.info("ROUTE op=%s acct=%s rejected by %s (%s) -> refreshing shard map",
                     op, acct, replica.id, resp.json().get("error"))
            router.refresh()
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
        return forward("transfer", p["from"], "POST", "/transfer_local", p)
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
