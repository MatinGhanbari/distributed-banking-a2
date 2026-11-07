#!/usr/bin/env python3
"""Bonus II - custom self-healing controller: replica crashes, freezes and controller crashes
under concurrent banking load."""
import random
import threading
from collections import Counter
from lib import *

title("B2", "Autonomous self-healing", "keep 2 healthy replicas per shard through kills, freezes and "
      "controller crashes, without duplicates and without breaking transactions")
HEAL = ("--profile", "healing")
AS = [f"A{601 + i:04d}" for i in range(6)]
BS = [f"A{1601 + i:04d}" for i in range(6)]
ALL = AS + BS


def docker(*args):
    r = subprocess.run(["docker", *args], capture_output=True, text=True)
    return r.stdout.strip()


def slot(shard, n):
    return sql("meta-db", f"SELECT generation, replica_id, status FROM replica_slots WHERE shard_id='{shard}' AND slot={n}")[0]


def wait_slot(shard, n, gen, timeout=40):
    return wait_until(lambda: slot(shard, n)[0] == str(gen) and slot(shard, n)[2] == "RUNNING", timeout, 0.3)


def events(since_id):
    return sql("meta-db", f"SELECT id, to_char(at, 'HH24:MI:SS.MS'), event, detail FROM heal_events WHERE id > {since_id} ORDER BY id")


def last_event_id():
    return int(sql1("meta-db", "SELECT coalesce(max(id), 0) FROM heal_events"))


def show_events(since_id, keep=("DETECT", "REPLACE", "CREATE", "ADOPT", "HEALTHY", "FAULT", "SCALE", "ORPHAN")):
    for _, at, ev, d in events(since_id):
        if ev in keep:
            info(f"{DIM}{at} {ev:8} {d[:110]}{X}")


def replica_containers():
    out = docker("ps", "-a", "--filter", "label=dsbank.role=bank-replica", "--format",
                 '{{.Names}}|{{.Label "dsbank.shard"}}|{{.Label "dsbank.slot"}}|{{.Label "dsbank.generation"}}|{{.State}}')
    return [tuple(l.split("|")) for l in out.splitlines() if l]


def restart_policies():
    names = docker("ps", "-a", "--filter", "name=dsbank", "--filter", "label=dsbank.role", "-q").split() + \
            docker("ps", "-aq", "--filter", "name=dsbank-").split()
    out = docker("inspect", "-f", "{{.Name}} {{.HostConfig.RestartPolicy.Name}}", *sorted(set(names)))
    return dict(l.lstrip("/").split() for l in out.splitlines())


def all_policies():
    ids = docker("ps", "-aq").split()
    out = docker("inspect", "-f", "{{.Name}} {{.HostConfig.RestartPolicy.Name}}", *ids)
    return dict(l.lstrip("/").split() for l in out.splitlines())


def kill_replica(name, how="kill"):
    time.sleep(3)  # let gateway + coordinator see the previous replacement before the next fault
    docker(how, name)
    info(f"{R}{B}docker {how} {name}{X}")
    return time.time()


# ------------------------------------------------------------------ setup
step("Desired state (meta-db.replica_sets) and replica slots")
for r in sql("meta-db", "SELECT shard_id, desired FROM replica_sets ORDER BY 1"):
    info(f"shard {r[0]}: desired {r[1]} replicas")
for r in sql("meta-db", "SELECT shard_id || slot, generation, replica_id, status FROM replica_slots ORDER BY 1"):
    info(f"slot {r[0]}: gen {r[1]} {r[2]:10} {r[3]}")

step("docker compose --profile healing up -d healer   (our controller, Docker API via /var/run/docker.sock)")
compose(*HEAL, "up", "-d", "healer")
time.sleep(2)
for l in logs("healer").splitlines()[:1]:
    if "controller start" in l:
        info(DIM + l[:150] + X)
pol = all_policies()
info("RestartPolicy: " + ", ".join(f"{n}={p}" for n, p in sorted(pol.items())))
check(pol and all(p == "no" for p in pol.values()), "no container uses a Docker restart policy (all 'no')")

b1_before = docker("inspect", "-f", "{{.Id}} {{.State.StartedAt}}", "dsbank-bank-b1-1")
before = {a: db_balance(a) for a in ALL}
tot_before = sum(total_money())

# ------------------------------------------------------------------ load
results, stop, lock = [], threading.Event(), threading.Lock()


def record(kind, src, dst, cents, fn):
    t0 = time.time()
    st, b = fn()
    with lock:
        results.append(dict(kind=kind, src=src, dst=dst, cents=cents, st=st, b=b, lat=time.time() - t0))


def worker(tid):
    rnd = random.Random(100 + tid)
    while not stop.is_set():
        c = rnd.randint(100, 500)
        pool_ = AS if tid % 2 == 0 else BS
        if tid in (0, 1):
            a = rnd.choice(pool_); record("deposit", a, None, c, lambda: client.deposit(a, fmt(c)))
        elif tid in (2, 3):
            a = rnd.choice(pool_); record("withdraw", a, None, c, lambda: client.withdraw(a, fmt(c)))
        elif tid in (4, 5):
            s, d = rnd.sample(pool_, 2); record("transfer", s, d, c, lambda: client.transfer(s, d, fmt(c)))
        else:
            s, d = rnd.choice(AS), rnd.choice(BS)
            if rnd.random() < 0.5:
                s, d = d, s
            record("transfer", s, d, c, lambda: client.transfer(s, d, fmt(c)))
        time.sleep(0.03)


step("Start concurrent load: 8 clients (deposits, withdrawals, local A/B transfers, cross-shard 2PC)")
threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
for t in threads:
    t.start()
time.sleep(2)

# ------------------------------------------------------------------ fault 1: crash
step("Fault 1: replica crash  (SIGKILL bank-a1 under load)")
e0 = last_event_id()
t0 = kill_replica("dsbank-bank-a1-1")
ok1 = wait_slot("A", 1, 2)
info(f"slot A1 -> {slot('A', 1)}  in {time.time() - t0:.1f}s")
show_events(e0)
check(ok1, "crashed replica detected and replaced (bank-a1-g2 healthy + registered)")

# ------------------------------------------------------------------ fault 2: freeze
step("Fault 2: unresponsive replica  (docker pause bank-b2: process frozen, container still 'running')")
e0 = last_event_id()
t0 = kill_replica("dsbank-bank-b2-1", "pause")
ok2 = wait_slot("B", 2, 2)
info(f"slot B2 -> {slot('B', 2)}  in {time.time() - t0:.1f}s")
show_events(e0)
note("its DB sessions are terminated before removal => a frozen replica can neither hold row locks nor commit later")
check(ok2, "frozen replica detected by failed health probes, fenced and replaced (bank-b2-g2)")

# ------------------------------------------------------------------ fault 3: controller crash after create
step("Fault 3: controller crashes right after 'docker create'  (one-shot fault flag), then bank-a2 is killed")
e0 = last_event_id()
sql("meta-db", "INSERT INTO controller_flags VALUES ('crash_after_create', '1') ON CONFLICT DO NOTHING")
kill_replica("dsbank-bank-a2-1")
wait_until(lambda: "exited" in compose("ps", "-a", "--format", "{{.State}}", "healer"), 20)
show_events(e0)
info(f"healer container: {compose('ps', '-a', '--format', '{{.Status}}', 'healer').strip()}")
info(f"slot A2 intent in meta-db: {slot('A', 2)}")
info(f"containers for slot A2: {[c for c in replica_containers() if c[1] == 'A' and c[2] == '2']}")
step("Restart the controller: docker compose --profile healing start healer")
e0 = last_event_id()
compose(*HEAL, "start", "healer")
ok3 = wait_slot("A", 2, 2)
show_events(e0)
a2 = [c for c in replica_containers() if c[1] == "A" and c[2] == "2"]
info(f"containers for slot A2 now: {a2}")
check(ok3 and len(a2) == 1, "restarted controller ADOPTED the half-created container: exactly 1 replica, no duplicate")

# ------------------------------------------------------------------ fault 4: kill -9 controller, failure while it is down
step("Fault 4: kill -9 the controller; while it is down, replacement bank-a1-g2 also crashes")
e0 = last_event_id()
time.sleep(3)
compose("kill", "healer")
info(f"{R}{B}docker compose kill healer{X}  (SIGKILL)")
kill_replica("bank-a1-g2")
time.sleep(2)
info(f"while the controller is down: {replica_status()}")
compose(*HEAL, "start", "healer")
ok4 = wait_slot("A", 1, 3)
show_events(e0)
check(ok4, "after controller restart the missed failure is repaired (bank-a1-g3)")

# ------------------------------------------------------------------ desired count change
step("Change desired state: shard B 2 -> 3 replicas, then back to 2")
e0 = last_event_id()
sql("meta-db", "UPDATE replica_sets SET desired = 3 WHERE shard_id = 'B'")
ok5 = wait_slot("B", 3, 1)
info(f"replicas: {replica_status()}")
sql("meta-db", "UPDATE replica_sets SET desired = 2 WHERE shard_id = 'B'")
ok6 = wait_until(lambda: not sql("meta-db", "SELECT 1 FROM replica_slots WHERE shard_id='B' AND slot=3"), 20)
time.sleep(2.5)
show_events(e0, keep=("SCALE", "HEALTHY"))
check(ok5 and ok6, "controller converges to the configured replica count (scale up and down)")

time.sleep(2)
stop.set()
for t in threads:
    t.join()

# ------------------------------------------------------------------ verification
step("Final state")
conts = replica_containers()
for c in sorted(conts):
    info(f"{c[0]:18} shard {c[1]} slot {c[2]} gen {c[3]} {c[4]}")
per_slot = Counter((c[1], c[2]) for c in conts)
running = Counter(c[1] for c in conts if c[4] == "running")
reg = dict(sql("meta-db", "SELECT shard_id, count(*) FROM replicas WHERE enabled GROUP BY 1"))
st = replica_status()
info(f"gateway view: {st}")
pol = all_policies()
b1_after = docker("inspect", "-f", "{{.Id}} {{.State.StartedAt}}", "dsbank-bank-b1-1")

step("Load results")
for r in results:  # resolve uncertain outcomes (retry with the same request id / status query)
    b = r["b"]
    if r["kind"] == "transfer" and b.get("routing") == "2pc":
        s = b.get("status") or ("ERROR:" + str(b.get("error")))
        if s in ("PENDING", "UNKNOWN"):
            s = (wait_until(lambda: (lambda x: x if x.get("status") in ("COMMITTED", "ABORTED") else None)(
                client.status(request_id=b["request_id"])[1]), 30) or {}).get("status", "UNKNOWN")
        r["ok"], r["final"] = s == "COMMITTED", s
    else:
        if r["st"] not in (200, 409, 404):
            f = {"deposit": client.deposit, "withdraw": client.withdraw}.get(r["kind"])
            r["st"], r["b"] = (f(r["src"], fmt(r["cents"]), b["request_id"]) if f else
                               client.transfer(r["src"], r["dst"], fmt(r["cents"]), b["request_id"]))
        r["ok"] = r["st"] == 200
        r["final"] = "OK" if r["ok"] else r["b"].get("error")
kinds = Counter((r["kind"] if r["kind"] != "transfer" else "transfer/" + (r["b"].get("routing") or "?"), r["final"])
                for r in results)
info("outcomes: " + ", ".join(f"{k[0]} {k[1]}={n}" for k, n in sorted(kinds.items())))
reasons = Counter(r["b"].get("reason") for r in results if r["final"] == "ABORTED")
if reasons:
    info(f"2PC abort reasons: {dict(reasons)}")
info(f"max latency {max(r['lat'] for r in results):.1f}s over {len(results)} requests")
expected = dict(before)
dep = wd = 0
for r in results:
    if r["ok"]:
        if r["kind"] == "deposit":
            expected[r["src"]] += r["cents"]; dep += r["cents"]
        elif r["kind"] == "withdraw":
            expected[r["src"]] -= r["cents"]; wd += r["cents"]
        else:
            expected[r["src"]] -= r["cents"]; expected[r["dst"]] += r["cents"]
after = {a: db_balance(a) for a in ALL}
tot_after = sum(total_money())
prep = [sql1(d, "SELECT count(*) FROM pg_prepared_xacts") for d in ("db-a", "db-b")]
states = dict(sql("meta-db", "SELECT state, count(*) FROM txns GROUP BY state"))
info(f"global money: {fmt(tot_before)} + {fmt(dep)} - {fmt(wd)} = {fmt(tot_before + dep - wd)} | now {fmt(tot_after)}")
info(f"txn states {states}, prepared xacts A/B: {prep}")

check(running == Counter({"A": 2, "B": 2}) and reg == {"A": "2", "B": "2"} and all(v == "UP" for v in st.values()),
      "exactly 2 healthy, registered replicas per shard (docker + registry + gateway agree)")
check(all(v == 1 for v in per_slot.values()), "no duplicate replica containers for any slot")
check(b1_before == b1_after, "healthy replica bank-b1 was never touched (same container, same start time)")
check(all(p == "no" for p in pol.values()), "replacement containers also have RestartPolicy=no")
check(all(r["final"] in ("OK", "COMMITTED", "ABORTED", "INSUFFICIENT_FUNDS") for r in results),
      "every banking request has a definite outcome")
check(after == expected, f"all {len(ALL)} accounts = initial + successful operations")
check(tot_after == tot_before + dep - wd, "global money conserved")
check(prep == ["0", "0"] and set(states) == {"COMPLETED"}, "no stuck prepared / unresolved 2PC transactions")
finish("B2")
