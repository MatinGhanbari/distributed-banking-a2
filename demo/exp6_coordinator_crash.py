#!/usr/bin/env python3
"""Experiment 6 - stop the Coordinator after the durable COMMIT decision but before all acks."""
import uuid
from lib import *

title(6, "Interrupted distributed transaction",
      "coordinator dies after COMMIT is durable but before all acks; after restart it completes the COMMIT")

def pending(db):
    return sql1(db, "SELECT count(*) FROM pg_prepared_xacts")

def crash_case(mode, src, dst, amount, label):
    a, b = db_balance(src), db_balance(dst)
    rid = f"exp6-{mode}-{uuid.uuid4().hex[:8]}"
    step(f"{label}: arm fault '{mode}', then transfer {src} -> {dst} {amount}")
    admin("POST", "/admin/fault", {"mode": mode})
    st, r = client.transfer(src, dst, amount, request_id=rid)
    info(f"client got HTTP {st}: status={r.get('status')}  ({r.get('message', '')[:60]})")
    check(r.get("status") == "UNKNOWN" and not r.get("ok"), "client is NOT told success (outcome unknown)")
    wait_until(lambda: "exited" in compose("ps", "-a", "--format", "{{.State}}", "coordinator"), 10)
    info(f"coordinator container: {compose('ps', '-a', '--format', '{{.Status}}', 'coordinator').strip()}")
    st, q = client.status(request_id=rid)
    info(f"txn status query while coordinator is down -> {q.get('status')}")
    check(q.get("status") == "UNKNOWN", "status query exposes UNKNOWN while the coordinator is down")

    step("Durable state on disk while the coordinator is down (direct SQL)")
    state = sql1("meta-db", f"SELECT state || ' acks=' || acks::text FROM txns WHERE request_id = '{rid}'")
    info(f"meta-db txns: {state}")
    info(f"db-a: {src}={fmt(db_balance(src))} prepared_xacts={pending('db-a')}   "
         f"db-b: {dst}={fmt(db_balance(dst))} prepared_xacts={pending('db-b')}")
    s0 = state

    step("docker compose start coordinator  -> recovery loop")
    compose("start", "coordinator")
    final = wait_until(lambda: (lambda s: s if s.get("status") in ("COMMITTED", "ABORTED") else None)(
        client.status(request_id=rid)[1]), 30)
    final = final or {}
    for l in [l for l in logs("coordinator").splitlines() if "RECOVERY" in l or "ready" in l][-2:]:
        info(DIM + l + X)
    info(f"txn status now: {final.get('status')}   history: " + " → ".join(h["state"] for h in final.get("history", [])))
    info(f"db-a: {src}={fmt(db_balance(src))}   db-b: {dst}={fmt(db_balance(dst))}   "
         f"prepared_xacts: {pending('db-a')}/{pending('db-b')}")
    check(final.get("status") == "COMMITTED", "transaction COMMITTED after restart (never replaced by ABORT)")
    check(db_balance(src) == a - money(amount) and db_balance(dst) == b + money(amount), "both sides applied exactly once")
    check(pending("db-a") == "0" and pending("db-b") == "0", "no prepared transactions left")
    return s0

s0 = crash_case("after_first_ack", "A0800", "A1800", "200.00", "Scenario C (required)")
check(s0.startswith("COMMIT_DECIDED") and '"A"' in s0 and '"B"' not in s0,
      "crash point verified: COMMIT_DECIDED durable, only shard A acked")
s0 = crash_case("after_prepared", "A0801", "A1801", "100.00", "Scenario B: all prepared, coordinator stops")
check(s0.startswith("PREPARED"), "crash point verified: both prepared, no decision yet -> recovery continued to COMMIT")
finish(6)
