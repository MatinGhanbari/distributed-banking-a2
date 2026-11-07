#!/usr/bin/env python3
"""Experiment 4 - kill one replica under load; the Gateway stops selecting it."""
import threading
from collections import Counter
from lib import *

title(4, "Replica interruption", "bank-a1 is killed under load; traffic moves to bank-a2 with no failed requests")
note("health: Gateway probes GET /health every 1s; a connection error also marks a replica DOWN at once")
note("and the request is retried on the next healthy replica (same request_id => applied once)")

accts = [f"A{301 + i:04d}" for i in range(10)]
before = {a: db_balance(a) for a in accts}
results, stop, phase = [], threading.Event(), {"p": "before"}

def load(tid):
    i = 0
    while not stop.is_set():
        st, b = client.deposit(accts[(tid + i) % 10], "1.00")
        results.append((phase["p"], st, b.get("replica"), b.get("error")))
        i += 1
        time.sleep(0.02)

step("Start load: 4 client threads depositing 1.00 into A0301..A0310")
threads = [threading.Thread(target=load, args=(t,)) for t in range(4)]
for t in threads:
    t.start()
time.sleep(3)
info(f"replica health: {replica_status()}")

step("docker compose kill bank-a1   (abrupt crash, load keeps running)")
phase["p"] = "during"
compose("kill", "bank-a1")
time.sleep(1.5)
phase["p"] = "after"
time.sleep(3)
st_down = replica_status()
info(f"replica health: {st_down}")
gw = [l for l in logs("gateway").splitlines() if "HEALTH bank-a1" in l]
for l in gw[-2:]:
    info(DIM + l + X)

step("docker compose start bank-a1   (replica comes back)")
phase["p"] = "recovering"
compose("start", "bank-a1")
wait_until(lambda: replica_status().get("bank-a1") == "UP", 30)
phase["p"] = "rejoined"
time.sleep(2.5)
stop.set()
for t in threads:
    t.join()

step("Who served the requests in each phase")
for p in ("before", "during", "after", "rejoined"):
    c = Counter(r for ph, st, r, _ in results if ph == p and st == 200)
    info(f"{p:9}: {dict(sorted(c.items()))}")
errors = [r for r in results if r[1] != 200]
ok_count = sum(1 for r in results if r[1] == 200)
delta = sum(db_balance(a) - before[a] for a in accts)
info(f"requests: {len(results)}, failed: {len(errors)}  {errors[:3]}")
info(f"successful deposits: {ok_count} x 1.00 = {fmt(ok_count * 100)}   |   money added in db-a: {fmt(delta)}")

after = Counter(r for ph, st, r, _ in results if ph == "after" and st == 200)
rejoined = Counter(r for ph, st, r, _ in results if ph == "rejoined" and st == 200)
check(st_down.get("bank-a1") == "DOWN" and st_down.get("bank-a2") == "UP", "Gateway marked bank-a1 DOWN, bank-a2 UP")
check(after and set(after) == {"bank-a2"}, "after the crash only bank-a2 was selected")
check(not errors, "no client request failed during the interruption")
check(delta == ok_count * 100, "every successful deposit applied exactly once (retries were idempotent)")
check(rejoined.get("bank-a1", 0) > 0, "bank-a1 is used again after it came back")
finish(4)
