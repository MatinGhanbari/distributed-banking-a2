#!/usr/bin/env python3
"""Experiment 3 - concurrent deposits/withdrawals through different replicas stay correct."""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from lib import *

title(3, "Concurrent replica operations",
      "concurrent writes via bank-a1 AND bank-a2 on the same rows: no lost update, no overdraft")
note("concurrency control: each write runs SELECT ... FOR UPDATE on the account row inside one")
note("PostgreSQL transaction in the shared shard DB, so writes from both replicas serialize")

acct = "A0010"
start = db_balance(acct)
step(f"{acct}: 100 deposits of 10.00 + 100 withdrawals of 5.00, 20 threads   (start {fmt(start)})")
ops = [("d", "10.00")] * 100 + [("w", "5.00")] * 100
def run(op):
    f = client.deposit if op[0] == "d" else client.withdraw
    st, b = f(acct, op[1])
    return st, b.get("replica")
with ThreadPoolExecutor(20) as ex:
    res = list(ex.map(run, ops))
okc = sum(1 for st, _ in res if st == 200)
reps = Counter(r for _, r in res)
expected = start + 100 * 1000 - 100 * 500
final = db_balance(acct)
info(f"successful: {okc}/200   served by: {dict(reps)}")
info(f"expected {fmt(start)} + 1000.00 - 500.00 = {fmt(expected)}   |   db-a now: {fmt(final)}")
check(okc == 200, "all 200 operations succeeded")
check(len(reps) == 2, "both replicas handled operations concurrently")
check(final == expected, "final balance is exact (no lost updates)")
check(client.balance(acct)[1].get("balance") == fmt(final), "Gateway reports the same balance as the DB")

acct = "A0011"
set_balance(acct, 100000)
start = db_balance(acct)
n_ok = start // 5000
step(f"{acct}: 30 concurrent withdrawals of 50.00 on a balance of {fmt(start)} (only {n_ok} can succeed)")
with ThreadPoolExecutor(30) as ex:
    res = list(ex.map(lambda _: client.withdraw(acct, "50.00"), range(30)))
codes = Counter(b.get("error", "OK") for _, b in res)
reps = Counter(b.get("replica") for _, b in res)
final = db_balance(acct)
info(f"results: {dict(codes)}   served by: {dict(reps)}   db-a now: {fmt(final)}")
check(codes.get("OK", 0) == n_ok and codes.get("INSUFFICIENT_FUNDS", 0) == 30 - n_ok,
      f"exactly {n_ok} succeeded, the rest got INSUFFICIENT_FUNDS")
check(final == start - n_ok * 5000 and final >= 0, "balance never went negative, matches successful ops")
finish(3)
