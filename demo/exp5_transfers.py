#!/usr/bin/env python3
"""Experiment 5 - same-shard vs cross-shard transfers (+ 2PC abort, recovery scenario A)."""
from lib import *

title(5, "Local and cross-shard transfers", "same-shard = one local ACID txn; cross-shard = Two-Phase Commit")

def txn_count():
    return int(sql1("meta-db", "SELECT count(*) FROM txns"))

step("Same-shard transfer A0101 -> A0102  100.00  (both rows in db-a)")
b1, b2, n0 = db_balance("A0101"), db_balance("A0102"), txn_count()
set_balance("A0101", 100000)
b1 = db_balance("A0101")
st, r = client.transfer("A0101", "A0102", "100.00")
info(f"gateway routing={r.get('routing')} replica={r.get('replica')}  A0101 {fmt(b1)}->{r.get('from_balance')}"
     f"  A0102 {fmt(b2)}->{r.get('to_balance')}")
note("both accounts are in ONE database => a single local transaction is atomic; no coordinator")
check(st == 200 and r.get("routing") == "local", "handled locally by one Shard A replica")
check(db_balance("A0101") == b1 - 10000 and db_balance("A0102") == b2 + 10000, "db-a: both rows updated")
check(txn_count() == n0, "no 2PC transaction was created in the coordinator log")

step("Cross-shard transfer A0500 (Shard A) -> A1500 (Shard B)  300.00   (the example from the spec)")
set_balance("A0500", 100000), set_balance("A1500", 50000)   # setup via the Gateway: 1000.00 / 500.00
a, b = db_balance("A0500"), db_balance("A1500")
st, r = client.transfer("A0500", "A1500", "300.00")
info(f"status={r.get('status')} txn={r.get('txn_id', '')[:8]} votes="
     f"{ {k: v['vote'] for k, v in r.get('votes', {}).items()} } acks={r.get('acks')}")
info("state history: " + " → ".join(h["state"] for h in r.get("history", [])))
info(f"db-a A0500: {fmt(a)} -> {fmt(db_balance('A0500'))}    db-b A1500: {fmt(b)} -> {fmt(db_balance('A1500'))}")
note("two independent databases cannot share one local transaction => 2PC: both PREPARE (durable),")
note("coordinator durably records COMMIT_DECIDED, then both COMMIT PREPARED; success only after both acks")
check(st == 200 and r.get("status") == "COMMITTED", "2PC committed")
check([h["state"] for h in r["history"]] == ["INITIATED", "PREPARING", "PREPARED", "COMMIT_DECIDED", "COMPLETED"],
      "durable states INITIATED→PREPARING→PREPARED→COMMIT_DECIDED→COMPLETED")
check(db_balance("A0500") == a - 30000 and db_balance("A1500") == b + 30000, "debit on db-a and credit on db-b")

step("Client retry with the SAME request_id (idempotency: must not transfer twice)")
st2, r2 = client.transfer("A0500", "A1500", "300.00", request_id=r["request_id"])
info(f"retry -> status={r2.get('status')} duplicate={r2.get('duplicate')} txn={r2.get('txn_id', '')[:8]}")
check(r2.get("duplicate") and r2.get("txn_id") == r.get("txn_id") and db_balance("A0500") == a - 30000,
      "retry returned the original result; money moved only once")

step("Scenario A: Shard A votes YES, Shard B rejects  (A1700 frozen directly in db-b)")
sql("db-b", "UPDATE accounts SET frozen = true WHERE id = 'A1700'")
set_balance("A0700", 100000)
a, b = db_balance("A0700"), db_balance("A1700")
st, r = client.transfer("A0700", "A1700", "50.00")
sql("db-b", "UPDATE accounts SET frozen = false WHERE id = 'A1700'")
votes = {k: (v["vote"], v.get("reason")) for k, v in r.get("votes", {}).items()}
info(f"status={r.get('status')} votes={votes}")
info("state history: " + " → ".join(h["state"] for h in r.get("history", [])))
rb = [l for l in logs("bank-a1").splitlines() + logs("bank-a2").splitlines()
      if "ROLLED BACK" in l and r.get("txn_id", "?")[:8] in l]
info(DIM + (rb[0] if rb else "no rollback log line") + X)
prepared = sql1("db-a", "SELECT count(*) FROM pg_prepared_xacts"), sql1("db-b", "SELECT count(*) FROM pg_prepared_xacts")
check(st == 409 and r.get("status") == "ABORTED", "transaction ABORTED")
check(votes.get("A", ("",))[0] == "YES" and votes.get("B", ("",))[0] == "NO", "A voted YES (was prepared), B voted NO")
check(bool(rb), "Shard A's prepared transaction was rolled back (ROLLBACK PREPARED)")
check(db_balance("A0700") == a and db_balance("A1700") == b, "both balances unchanged")
check(prepared == ("0", "0"), "no prepared transaction left in db-a / db-b")

step("Cross-shard with insufficient funds: A0701 -> A1701  5000.00")
st, r = client.transfer("A0701", "A1701", "5000.00")
info(f"status={r.get('status')} reason={r.get('reason')}")
check(r.get("status") == "ABORTED", "source shard votes NO, whole transfer aborted")
finish(5)
