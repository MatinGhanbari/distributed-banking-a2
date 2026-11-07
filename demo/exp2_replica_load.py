#!/usr/bin/env python3
"""Experiment 2 - many requests to one shard are spread over its replicas (logs show which)."""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from lib import *

title(2, "Replica load distribution", "requests for Shard A accounts are handled by both bank-a1 and bank-a2")
since = now_iso()
time.sleep(1)  # --since has 1s resolution

step("Sending 60 requests (balance + deposit 1.00) for accounts A0201..A0210, 6 client threads")
def one(i):
    acct = f"A{201 + i % 10:04d}"
    return (client.balance(acct) if i % 2 else client.deposit(acct, "1.00"))[1].get("replica")
with ThreadPoolExecutor(6) as ex:
    served = Counter(ex.map(one, range(60)))
for rep, n in sorted(served.items()):
    info(f"responses served by {rep}: {n:3}  {'█' * n}")

step("Gateway log (ROUTE lines) - which replica served each request")
lines = [l for l in logs("gateway", since).splitlines() if "ROUTE op=" in l and "acct=A02" in l]
for l in lines[:6]:
    info(DIM + l + X)
info(f"... {len(lines)} ROUTE lines in total")
by_log = Counter(l.split("replica=")[1].split()[0] for l in lines)
info(f"per replica according to the gateway log: {dict(by_log)}")

step("Each replica's own log")
own = {r: sum(1 for l in logs(r, since).splitlines() if "A02" in l) for r in ("bank-a1", "bank-a2")}
info(f"lines mentioning A02xx: {own}")

check(set(served) == {"bank-a1", "bank-a2"}, "both Shard A replicas served requests")
check(min(served.values()) >= 20, f"load is spread (round robin): {dict(served)}")
check(by_log == served, "gateway log matches the responses")
check(all(v > 0 for v in own.values()), "both replicas logged the work they did")
check(not any(r.startswith("bank-b") for r in served), "no Shard A request went to a Shard B replica")
finish(2)
