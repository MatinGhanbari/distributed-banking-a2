#!/usr/bin/env python3
"""Experiment 7 - concurrent cross-shard transfers on overlapping accounts: conservation of money."""
import random
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from lib import *

title(7, "Concurrent cross-shard transfers",
      "300 concurrent transfers in both directions between 10 overlapping accounts")
note("no distributed deadlock: participants are prepared in fixed shard order (A then B), each holds")
note("one row lock per shard; local transfers lock rows in id order; lock_timeout 5s votes NO as a safety net")

accts = [f"A{901 + i:04d}" for i in range(5)] + [f"A{1901 + i:04d}" for i in range(5)]
before = {a: db_balance(a) for a in accts}
tot_before = total_money()
random.seed(7)
jobs = []
for i in range(300):
    src, dst = random.sample(accts, 2)
    jobs.append((src, dst, f"{random.randint(1, 400)}.{random.randint(0, 99):02d}"))
cross = sum(1 for s, d, _ in jobs if (int(s[1:]) <= 1000) != (int(d[1:]) <= 1000))
step(f"Running 300 transfers ({cross} cross-shard, {300 - cross} same-shard) with 24 client threads")
t0 = time.time()
with ThreadPoolExecutor(24) as ex:
    res = list(ex.map(lambda j: (j, *client.transfer(*j)), jobs))
info(f"done in {time.time() - t0:.1f}s")

# Uncertain outcomes (PENDING/UNKNOWN) are resolved by querying the txn status by request id
outcome = []
for (src, dst, amt), st, b in res:
    s = b.get("status") or ("COMMITTED" if st == 200 else "ABORTED")
    if s in ("PENDING", "UNKNOWN"):
        q = wait_until(lambda: (lambda x: x if x.get("status") in ("COMMITTED", "ABORTED") else None)(
            client.status(request_id=b["request_id"])[1]), 30) or {}
        s = q.get("status", "UNKNOWN")
    outcome.append((src, dst, money(amt), s, b.get("routing"), b.get("error") or b.get("reason")))
cnt = Counter((o[4], o[3]) for o in outcome)
for (routing, s), n in sorted(cnt.items()):
    info(f"{routing:5} {s:9}: {n}")
reasons = Counter(o[5].split(":")[-1].strip() for o in outcome if o[3] != "COMMITTED" and o[5])
info(f"abort reasons: {dict(reasons)}")

step("Verification against the databases")
expected = dict(before)
for src, dst, amt, s, *_ in outcome:
    if s == "COMMITTED":
        expected[src] -= amt
        expected[dst] += amt
after = {a: db_balance(a) for a in accts}
print(f"  {B}{'account':8} {'before':>9} {'expected':>9} {'db now':>9}{X}")
for a in accts:
    info(f"{a:8} {fmt(before[a]):>9} {fmt(expected[a]):>9} {fmt(after[a]):>9}")
tot_after = total_money()
states = dict(sql("meta-db", "SELECT state, count(*) FROM txns GROUP BY state"))
prep = sql1("db-a", "SELECT count(*) FROM pg_prepared_xacts"), sql1("db-b", "SELECT count(*) FROM pg_prepared_xacts")
info(f"total money db-a+db-b: before {fmt(sum(tot_before))}  after {fmt(sum(tot_after))}")
info(f"coordinator txn states: {states}   prepared_xacts left: {prep}")

check(all(o[3] in ("COMMITTED", "ABORTED") for o in outcome), "every transfer has a final outcome")
check(after == expected, "every account balance equals initial + committed transfers")
check(sum(after.values()) == sum(before.values()), "sum of the 10 accounts unchanged")
check(sum(tot_after) == sum(tot_before), "global money conserved across both shards")
check(min(after.values()) >= 0, "no negative balance")
check(set(states) == {"COMPLETED"} and prep == ("0", "0"), "all 2PC txns COMPLETED, nothing left prepared")
finish(7)
