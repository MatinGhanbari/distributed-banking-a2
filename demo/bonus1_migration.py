#!/usr/bin/env python3
"""Bonus I - live re-sharding of A1501-A2000 from Shard B to new Shard C under load,
with the migration controller killed in 4 different phases."""
import json
import random
import threading
from collections import Counter, defaultdict
from lib import *

title("B1", "Live re-sharding under adversarial failures",
      "move A1501-A2000 B -> C while they keep serving; kill the controller in 4 phases")
SHARDC = ("--profile", "shardc")
M = [f"A{1501 + i:04d}" for i in range(10)]     # migrating accounts (B -> C)
N = [f"A{1201 + i:04d}" for i in range(5)]      # B accounts that stay on B
A = [f"A{951 + i:04d}" for i in range(5)]       # Shard A accounts (cross-shard partners)
ALL = M + N + A


def owner_db(acct):
    n = int(acct[1:])
    if n <= 1000:
        return "db-a"
    st = sql1("meta-db", f"SELECT shard_id FROM shard_ranges WHERE {n} BETWEEN range_lo AND range_hi")
    return "db-" + st.lower()


def bal(acct):
    v = sql1(owner_db(acct), f"SELECT balance_cents FROM accounts WHERE id = '{acct}'")
    return int(v) if v is not None else None


def mig():
    r = sql("meta-db", "SELECT phase, coalesce(snapshot_next, 0), coalesce(cdc_lsn, '-') FROM migrations WHERE id='m1'")
    return r[0] if r else ("-", "0", "-")


def totals():
    t = 0
    for db in ("db-a", "db-b", "db-c"):
        try:
            t += int(sql1(db, "SELECT coalesce(sum(balance_cents), 0) FROM accounts"))
        except Exception:
            pass
    return t


def wait_phase(pred, timeout=60):
    return wait_until(lambda: (lambda m: m if pred(m) else None)(mig()), timeout, 0.15)


def kill_and_restart(label, pred, show):
    step(f"Crash {label}")
    m = wait_phase(pred)
    compose("kill", "migrator")
    info(f"{R}{B}docker kill migrator{X}  (phase {m[0]}, snapshot next=A{int(m[1]):04d}, cdc lsn={m[2]})")
    show()
    compose(*SHARDC, "start", "migrator")
    time.sleep(1.5)
    res = [l for l in logs("migrator", since_kill[0]).splitlines() if "RESUME" in l]
    for l in res[:2]:
        info(DIM + l + X)
    since_kill[0] = now_iso()
    return m


since_kill = [now_iso()]

# ------------------------------------------------------------------ setup
step("Before: Shard Map and data placement")
for r in cluster()["ranges"]:
    info(f"shard {r['shard']}: {r['from']}..{r['to']}  epoch {r['epoch']}")
info(f"db-b rows in A1501-A2000: {sql1('db-b', 'SELECT count(*) FROM accounts WHERE num BETWEEN 1501 AND 2000')}")

step("docker compose --profile shardc up -d db-c bank-c1 bank-c2   (new shard at runtime)")
compose(*SHARDC, "up", "-d", "--wait", "db-c", "bank-c1", "bank-c2")
info(f"db-c accounts: {sql1('db-c', 'SELECT count(*) FROM accounts')} (empty, not in the Shard Map yet)")

before = {a: bal(a) for a in ALL}
total_before = totals()

# ------------------------------------------------------------------ load
results, stop, phase_now, lock = [], threading.Event(), ["-"], threading.Lock()


def record(kind, src, dst, cents, fn):
    t0 = time.time()
    st, b = fn()
    with lock:
        results.append(dict(kind=kind, src=src, dst=dst, cents=cents, st=st, b=b, lat=time.time() - t0,
                            phase=phase_now[0]))


def worker(tid):
    rnd = random.Random(tid)
    while not stop.is_set():
        c = rnd.randint(100, 500)
        if tid == 0:
            a = rnd.choice(M); record("deposit", a, None, c, lambda: client.deposit(a, fmt(c)))
        elif tid == 1:
            a = rnd.choice(M); record("withdraw", a, None, c, lambda: client.withdraw(a, fmt(c)))
        elif tid == 2:
            s, d = rnd.sample(M, 2); record("transfer", s, d, c, lambda: client.transfer(s, d, fmt(c)))
        elif tid == 3:
            s, d = rnd.choice(A), rnd.choice(M)
            if rnd.random() < 0.5:
                s, d = d, s
            record("transfer", s, d, c, lambda: client.transfer(s, d, fmt(c)))
        elif tid == 4:
            s, d = rnd.sample(N, 2); record("transfer", s, d, c, lambda: client.transfer(s, d, fmt(c)))
        else:
            s, d = rnd.choice(N), rnd.choice(M)
            if rnd.random() < 0.5:
                s, d = d, s
            record("transfer", s, d, c, lambda: client.transfer(s, d, fmt(c)))
        time.sleep(0.05)


def phase_poller():
    while not stop.is_set():
        try:
            phase_now[0] = mig()[0]
        except Exception:
            pass
        time.sleep(0.3)


step("Start continuous load (6 clients): deposits/withdrawals/local transfers on A1501-A1510,")
info("cross-shard A095x<->A15xx, B-local A120x, boundary A120x<->A150x (local before, 2PC after cutover)")
threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)] + [threading.Thread(target=phase_poller)]
for t in threads:
    t.start()
time.sleep(2)

step("docker compose --profile shardc up -d migrator   (live migration starts)")
compose(*SHARDC, "up", "-d", "migrator")

# ------------------------------------------------------------------ 4 crashes
m1 = kill_and_restart("(i) during SNAPSHOT", lambda m: m[0] == "SNAPSHOT" and int(m[1]) >= 1601, lambda: (
    info(f"target checkpoint (db-c.migration_progress): "
         f"{sql('db-c', 'SELECT snapshot_next, cdc_lsn FROM migration_progress')[0]}"),
    info(f"db-c rows copied so far: {sql1('db-c', 'SELECT count(*) FROM accounts')}")))

m2 = kill_and_restart("(ii) during CDC delta replay (CATCHUP)", lambda m: m[0] == "CATCHUP" and m[2] != "-", lambda: (
    info(f"target checkpoint: {sql('db-c', 'SELECT snapshot_next, cdc_lsn FROM migration_progress')[0]}"),
    info(f"source slot: {sql('db-b', 'SELECT slot_name, confirmed_flush_lsn FROM pg_replication_slots')}")))

fence_state = {}


def show_fence():
    fence_state["b"] = sql("db-b", "SELECT range_lo, epoch, writable FROM ownership WHERE range_lo = 1501")[0]
    fence_state["c"] = sql("db-c", "SELECT range_lo, epoch, writable FROM ownership")[0]
    fence_state["map"] = sql1("meta-db", "SELECT shard_id || ' e' || epoch FROM shard_ranges WHERE range_lo = 1501")
    info(f"db-b ownership A1501-: epoch={fence_state['b'][1]} writable={fence_state['b'][2]}   "
         f"db-c ownership: epoch={fence_state['c'][1]} writable={fence_state['c'][2]}   map: {fence_state['map']}")
    note("no writable owner for the range right now -> its requests wait (brief pause); others continue")


m3 = kill_and_restart("(iii) during FENCING", lambda m: m[0] == "FENCING" and sql1(
    "db-b", "SELECT writable FROM ownership WHERE range_lo = 1501") == "f", show_fence)
check(fence_state["b"][2] == "f" and fence_state["c"][2] == "f",
      "fenced on B before C became writable (never two writable owners)")

stale = {}


def stale_attack():
    cut = json.loads(sql1("meta-db", "SELECT cutover FROM migrations WHERE id = 'm1'"))
    info(f"cutover decision (meta-db): {cut['count']} accounts, sum {fmt(cut['sum_cents'])}, md5 {cut['md5'][:12]}, "
         f"epoch {cut['epoch']};  map: {sql1('meta-db', 'SELECT shard_id || chr(32) || epoch FROM shard_ranges WHERE range_lo=1501')}")
    note("deliberately stale requests sent DIRECTLY to bank-b1 / bank-b2 (old owner, bypassing the Gateway):")
    code = ("import json,urllib.request as u\n"
            "def p(path,b):\n"
            " r=u.Request('http://localhost:8000'+path,json.dumps(b).encode(),{'Content-Type':'application/json'})\n"
            " try: x=u.urlopen(r); return x.status, json.loads(x.read())\n"
            " except Exception as e: return e.code, json.loads(e.read())\n"
            "out=[]\n"
            "for ep in (1,2):\n"
            " out.append(('deposit e%d'%ep,)+p('/deposit',{'account':'A1505','amount':'100','request_id':'stale-d%d-'+__import__('uuid').uuid4().hex,'epoch':ep}))\n"
            " out.append(('withdraw e%d'%ep,)+p('/withdraw',{'account':'A1505','amount':'1','request_id':'stale-w%d-'+__import__('uuid').uuid4().hex,'epoch':ep}))\n"
            " out.append(('transfer e%d'%ep,)+p('/transfer_local',{'from':'A1505','to':'A1506','amount':'1','request_id':'stale-t-'+__import__('uuid').uuid4().hex,'epoch':ep}))\n"
            " out.append(('2pc prepare e%d'%ep,)+p('/2pc/prepare',{'txn_id':str(__import__('uuid').uuid4()),'account':'A1505','delta':100,'epoch':ep}))\n"
            "print(json.dumps([(a,s,b.get('error') or b.get('vote')+'/'+str(b.get('reason'))) for a,s,b in out]))")
    md5_before = sql1("db-b", "SELECT md5(coalesce(string_agg(id || ':' || balance_cents || ':' || frozen, ',' ORDER BY id), '')) FROM accounts WHERE num BETWEEN 1501 AND 2000")
    ledger_before = sql1("db-b", "SELECT coalesce(max(seq), 0) FROM ledger")
    rejected = True
    for rep in ("bank-b1", "bank-b2"):
        res = json.loads(compose("exec", "-T", rep, "python", "-c", code))
        info(f"{rep}: " + ", ".join(f"{a}->{s} {e}" for a, s, e in res))
        rejected &= all((s == 421) or (e.startswith("NO/")) for a, s, e in res)
    md5_after = sql1("db-b", "SELECT md5(coalesce(string_agg(id || ':' || balance_cents || ':' || frozen, ',' ORDER BY id), '')) FROM accounts WHERE num BETWEEN 1501 AND 2000")
    stale.update(rejected=rejected, unchanged=md5_before == md5_after == cut["md5"],
                 ledger=sql1("db-b", "SELECT coalesce(max(seq), 0) FROM ledger WHERE account_id BETWEEN 'A1501' AND 'A2000' AND seq > %s" % ledger_before),
                 prepared=sql1("db-b", "SELECT count(*) FROM pg_prepared_xacts"))
    info(f"old rows on db-b: md5 {md5_after[:12]} (cutover {cut['md5'][:12]})   new ledger entries for the range: {stale['ledger']}")


m4 = kill_and_restart("(iv) after map activation, before cleanup", lambda m: m[0] == "ACTIVATED", stale_attack)
check(stale["rejected"], "every stale request to the old owner was rejected (421 / vote NO)")
check(stale["unchanged"] and stale["ledger"] == "0" and stale["prepared"] == "0",
      "old owner unchanged: md5 == verified cutover checksum, no ledger entry, nothing prepared")

step("Wait for the migration to finish (cleanup)")
done = wait_phase(lambda m: m[0] == "DONE", 60)
time.sleep(2)
stop.set()
for t in threads:
    t.join()
for l in [l for l in logs("migrator").splitlines() if any(k in l for k in ("FENCED", "VERIFY", "ACTIVATED:", "CLEANUP"))][-5:]:
    info(DIM + l + X)
hist = json.loads(sql1("meta-db", "SELECT history FROM migrations WHERE id = 'm1'"))
info("phase history: " + " → ".join(h["phase"] for h in hist))
check(done is not None, "migration reached DONE after 4 controller crashes, without manual repair")
check(m1[0] == "SNAPSHOT" and m2[0] == "CATCHUP" and m3[0] == "FENCING" and m4[0] == "ACTIVATED",
      "crashes hit SNAPSHOT, CATCHUP, FENCING and ACTIVATED")

# ------------------------------------------------------------------ verification
step("Load results")
for r in results:  # resolve uncertain outcomes (retry same request id / status query)
    b = r["b"]
    if r["kind"] == "transfer" and b.get("routing") == "2pc":
        s = b.get("status") or ("ERROR:" + str(b.get("error")))
        if s in ("PENDING", "UNKNOWN"):
            s = (wait_until(lambda: (lambda x: x if x.get("status") in ("COMMITTED", "ABORTED") else None)(
                client.status(request_id=b["request_id"])[1]), 30) or {}).get("status", "UNKNOWN")
        r["ok"] = s == "COMMITTED"
        r["final"] = s
    else:
        if r["st"] not in (200, 409, 404):
            f = {"deposit": client.deposit, "withdraw": client.withdraw}.get(r["kind"])
            st, b2 = (f(r["src"], fmt(r["cents"]), b["request_id"]) if f else
                      client.transfer(r["src"], r["dst"], fmt(r["cents"]), b["request_id"]))
            r["st"], r["b"] = st, b2
        r["ok"] = r["st"] == 200
        r["final"] = "OK" if r["ok"] else r["b"].get("error")
by_phase = defaultdict(Counter)
for r in results:
    if r["src"] in M or r["dst"] in M:
        by_phase[r["phase"]]["ok" if r["ok"] else "rejected"] += 1
for ph in ("SPLIT", "SNAPSHOT", "CATCHUP", "FENCING", "CUTOVER_DECIDED", "ACTIVATED", "DONE"):
    if ph in by_phase:
        info(f"ops touching migrating accounts during {ph:15}: {dict(by_phase[ph])}")
kinds = Counter((r["kind"] if r["kind"] != "transfer" else "transfer/" + (r["b"].get("routing") or "?"), r["final"])
                for r in results)
info("outcomes: " + ", ".join(f"{k[0]} {k[1]}={n}" for k, n in sorted(kinds.items())))
lat_m = max((r["lat"] for r in results if r["src"] in M or r["dst"] in M), default=0)
lat_n = max((r["lat"] for r in results if r["src"] in N and r["dst"] in N), default=0)
err_n = [r for r in results if r["src"] in N and r["dst"] in N and not r["ok"] and r["final"] != "INSUFFICIENT_FUNDS"]
info(f"max latency: migrating accounts {lat_m:.1f}s (cutover pause) | untouched B accounts {lat_n:.2f}s")

expected = dict(before)
dep = wd = 0
for r in results:
    if not r["ok"]:
        continue
    if r["kind"] == "deposit":
        expected[r["src"]] += r["cents"]; dep += r["cents"]
    elif r["kind"] == "withdraw":
        expected[r["src"]] -= r["cents"]; wd += r["cents"]
    else:
        expected[r["src"]] -= r["cents"]; expected[r["dst"]] += r["cents"]
after = {a: bal(a) for a in ALL}
step("Verification against the databases")
print(f"  {B}{'account':8} {'owner':6} {'before':>9} {'expected':>9} {'db now':>9}{X}")
for a in M[:4] + N[:2] + A[:2]:
    info(f"{a:8} {owner_db(a):6} {fmt(before[a]):>9} {fmt(expected[a]):>9} {fmt(after[a]):>9}")
info(f"... ({len(ALL)} accounts checked)")
total_after = totals()
info(f"global money: before {fmt(total_before)} + deposits {fmt(dep)} - withdrawals {fmt(wd)} = "
     f"{fmt(total_before + dep - wd)} | now A+B+C {fmt(total_after)}")
placement = (sql1("db-b", "SELECT count(*) FROM accounts WHERE num BETWEEN 1501 AND 2000"),
             sql1("db-c", "SELECT count(*) FROM accounts WHERE num BETWEEN 1501 AND 2000"))
info(f"rows A1501-A2000: db-b {placement[0]}, db-c {placement[1]}  |  served by: {client.balance('A1505')[1].get('replica')}, "
     f"{client.balance('A1505')[1].get('replica')}")
prep = [sql1(d, "SELECT count(*) FROM pg_prepared_xacts") for d in ("db-a", "db-b", "db-c")]
states = dict(sql("meta-db", "SELECT state, count(*) FROM txns GROUP BY state"))
slots = sql1("db-b", "SELECT count(*) FROM pg_replication_slots")

first = next(r for r in results if r["kind"] == "deposit" and r["ok"] and r["phase"] in ("SPLIT", "SNAPSHOT", "-"))
b_before = bal(first["src"])
st, dup = client.deposit(first["src"], fmt(first["cents"]), first["b"]["request_id"])
info(f"retry of a deposit applied on B before the move (rid {first['b']['request_id'][:12]}..) -> "
     f"served by {dup.get('replica')}, duplicate={dup.get('duplicate')}")

check(after == expected, f"all {len(ALL)} accounts = initial + successful operations (no lost / duplicated update)")
check(total_after == total_before + dep - wd, "global money conserved across A + B + C")
check(placement == ("0", "500"), "A1501-A2000 now only on db-c; old rows removed after verified completion")
check(by_phase["SNAPSHOT"]["ok"] > 0 and by_phase["CATCHUP"]["ok"] > 0,
      "migrating accounts kept accepting operations during snapshot and catch-up")
check(not err_n, "accounts that stay on B were never affected (no errors)")
check(all(r["final"] in ("OK", "COMMITTED", "ABORTED", "INSUFFICIENT_FUNDS") for r in results),
      "every operation has a definite outcome (no unknown / error results)")
check(set(states) == {"COMPLETED"} and prep == ["0", "0", "0"], "all 2PC transactions COMPLETED, nothing prepared")
check(slots == "0", "CDC slot dropped")
check(dup.get("duplicate") and bal(first["src"]) == b_before, "exactly-once: request id from before the move is recognised on C")
finish("B1")
