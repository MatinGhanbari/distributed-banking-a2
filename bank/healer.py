"""Self-Healing Controller (Bonus II) - our own reconciliation loop on the Docker Engine API.

Desired state  : meta-db.replica_sets (shard -> desired replica count, image, shard DB)
Durable intent : meta-db.replica_slots (shard, slot) -> generation, replica/container name, status
Observed state : Docker containers (labels dsbank.*) + GET /health probes

Every PROBE_INTERVAL seconds, for each slot:
  RUNNING  : container exited/dead/missing -> FAILED at once; running/paused but /health fails
             FAIL_THRESHOLD times in a row -> FAILED (unresponsive: paused, hung, DB unreachable)
  FAILED   : deregister from `replicas` (gateway stops routing), fence its DB sessions
             (pg_terminate_backend by client IP => locks of a frozen replica are released, its
             open transactions roll back), remove the container, then compare-and-set the slot
             to generation+1 / status CREATING  (intent persisted BEFORE docker create)
  CREATING : container named after (shard, slot, generation) exists? adopt it : create it;
             start -> STARTING
  STARTING : healthy -> register in `replicas` -> RUNNING;  exited / START_TIMEOUT -> FAILED
Also: scale up/down to `desired`, remove orphan healer-made containers not referenced by a slot.

Crash safety: names are deterministic per (slot, generation) and Docker names are unique, so a
restarted controller adopts what a crashed one created and can never start a duplicate.
Healthy replicas are never touched. No Docker restart policy is used (RestartPolicy=no).
"""
import os
import time
from concurrent.futures import ThreadPoolExecutor

import docker
import httpx
import psycopg

from .common import setup_logging

log = setup_logging("healer")
NET = os.environ.get("HEAL_NETWORK", "dsbank_default")
PROBE_INTERVAL = float(os.environ.get("PROBE_INTERVAL", "1"))
FAIL_THRESHOLD = int(os.environ.get("FAIL_THRESHOLD", "3"))
START_TIMEOUT = float(os.environ.get("START_TIMEOUT", "40"))
CMD = ["uvicorn", "bank.shard_server:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
HEALTHCHECK = {"test": ["CMD", "python", "-c", "import urllib.request; "
                        "urllib.request.urlopen('http://localhost:8000/health', timeout=2)"],
               "interval": 2_000_000_000, "timeout": 3_000_000_000, "retries": 30}

dk = docker.from_env()
probe_http = httpx.Client(timeout=1.0)
pool = ThreadPoolExecutor(8)
fails = {}  # (shard, slot, generation) -> consecutive failed probes (in memory is fine)


def connect():
    while True:
        try:
            return psycopg.connect(os.environ["META_DSN"], autocommit=True, connect_timeout=3)
        except psycopg.OperationalError as e:
            log.info("waiting for meta-db: %s", e)
            time.sleep(1)


meta = connect()


def event(ev, detail):
    meta.execute("INSERT INTO heal_events (event, detail) VALUES (%s, %s)", (ev, detail))
    log.info("%-8s %s", ev, detail)


def get_container(name):
    try:
        return dk.containers.get(name)
    except docker.errors.NotFound:
        return None


def probe(rid):
    try:
        r = probe_http.get(f"http://{rid}:8000/health")
        return r.status_code == 200 and r.json().get("ok") is True
    except Exception:
        return False


def ip_of(c):
    try:
        return c.attrs["NetworkSettings"]["Networks"][NET]["IPAddress"] or None
    except (KeyError, TypeError):
        return None


def name_for(shard, slot, gen):
    return f"bank-{shard.lower()}{slot}-g{gen}"


def register(shard, rid):
    meta.execute("INSERT INTO replicas VALUES (%s, %s, %s, true) ON CONFLICT (replica_id) DO UPDATE "
                 "SET shard_id = EXCLUDED.shard_id, url = EXCLUDED.url, enabled = true",
                 (rid, shard, f"http://{rid}:8000"))


def fence_sessions(dsn, ip):
    """Kill the failed replica's DB sessions so a frozen replica cannot keep row locks or
    commit later; prepared 2PC transactions are not session-bound and are unaffected."""
    if not ip:
        return 0
    with psycopg.connect(dsn, autocommit=True, connect_timeout=3) as c:
        rows = c.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE client_addr = %s::inet "
                         "AND pid <> pg_backend_pid()", (ip,)).fetchall()
    return len(rows)


def fail(s, sets, reason):
    shard, slot, gen, rid, cname, status, ip = s
    event("DETECT", f"{rid} (slot {shard}{slot}, gen {gen}): {reason}")
    meta.execute("DELETE FROM replicas WHERE replica_id = %s", (rid,))           # 1. deregister
    c = get_container(cname)
    # Only a container that still exists and is running/paused can still own DB sessions; an
    # exited one's sockets were closed by the kernel. Never use a stored IP: it may be reused.
    live_ip = ip_of(c) if c and c.status in ("running", "paused") else None
    n = fence_sessions(sets[shard][2], live_ip)                                   # 2. fence DB sessions
    if c:
        c.remove(force=True)                                                      # 3. remove
    new_gen = gen + 1
    new = name_for(shard, slot, new_gen)
    meta.execute("UPDATE replica_slots SET generation = %s, replica_id = %s, container = %s, status = 'CREATING', "
                 "ip = NULL, since = now() WHERE shard_id = %s AND slot = %s AND generation = %s",
                 (new_gen, new, new, shard, slot, gen))                           # 4. durable intent
    event("REPLACE", f"slot {shard}{slot}: removed {cname} (deregistered, {n} DB sessions fenced); "
                     f"intent: create {new} (gen {new_gen})")


def create(shard, slot, gen, rid, sets):
    flag = meta.execute("DELETE FROM controller_flags WHERE key = 'crash_after_create' RETURNING value").fetchone()
    _, image, dsn = sets[shard]
    c = dk.containers.create(
        image, command=CMD, name=rid, detach=True, network=NET,
        environment={"REPLICA_ID": rid, "SHARD_ID": shard, "DB_DSN": dsn, "PYTHONUNBUFFERED": "1"},
        labels={"dsbank.role": "bank-replica", "dsbank.shard": shard, "dsbank.slot": str(slot),
                "dsbank.generation": str(gen), "dsbank.managed-by": "healer"},
        restart_policy={"Name": "no"}, healthcheck=HEALTHCHECK)
    event("CREATE", f"{rid} created (container {c.short_id})")
    if flag:
        event("FAULT", "crash_after_create: controller exits right after docker create")
        os._exit(1)
    return c


def handle(s, sets, healthy):
    shard, slot, gen, rid, cname, status, ip = s
    key = (shard, slot, gen)
    c = get_container(cname)
    state = c.status if c else "missing"
    if status == "RUNNING":
        if state in ("exited", "dead", "missing", "created", "removing"):
            return fail(s, sets, f"container {state}")
        if healthy.get(rid):
            fails[key] = 0
            if ip is None:
                meta.execute("UPDATE replica_slots SET ip = %s WHERE shard_id = %s AND slot = %s",
                             (ip_of(c), shard, slot))
            register(shard, rid)  # idempotent (covers a crash between health and register)
            return
        fails[key] = fails.get(key, 0) + 1
        log.info("PROBE    %s /health failed (%d/%d, container %s)", rid, fails[key], FAIL_THRESHOLD, state)
        if fails[key] >= FAIL_THRESHOLD:
            fail(s, sets, f"unresponsive: {fails[key]} failed health probes (container {state})")
    elif status == "CREATING":
        if c is None:
            c = create(shard, slot, gen, rid, sets)
        else:
            event("ADOPT", f"{rid} already exists ({state}) - created before a controller crash; reusing it")
        if c.status in ("created", "exited"):
            c.start()
        meta.execute("UPDATE replica_slots SET status = 'STARTING', since = now() WHERE shard_id = %s AND slot = %s "
                     "AND generation = %s", (shard, slot, gen))
        log.info("START    %s started, waiting for /health", rid)
    elif status == "STARTING":
        if state in ("exited", "dead", "missing"):
            return fail(s, sets, f"replacement container {state}")
        if healthy.get(rid):
            c.reload()
            register(shard, rid)
            meta.execute("UPDATE replica_slots SET status = 'RUNNING', ip = %s, since = now() WHERE shard_id = %s "
                         "AND slot = %s AND generation = %s", (ip_of(c), shard, slot, gen))
            event("HEALTHY", f"{rid} healthy -> registered with the gateway (shard {shard})")
            return
        age = meta.execute("SELECT extract(epoch FROM now() - since) FROM replica_slots WHERE shard_id = %s "
                           "AND slot = %s", (shard, slot)).fetchone()[0]
        if age > START_TIMEOUT:
            fail(s, sets, f"not healthy {START_TIMEOUT:.0f}s after start")


def reconcile():
    sets = {r[0]: (r[1], r[2], r[3]) for r in meta.execute("SELECT shard_id, desired, image, db_dsn FROM replica_sets")}
    for shard, (desired, _, _) in sets.items():          # scale up: new slots
        for slot in range(1, desired + 1):
            rid = name_for(shard, slot, 1)
            if meta.execute("INSERT INTO replica_slots (shard_id, slot, generation, replica_id, container, status) "
                            "VALUES (%s, %s, 1, %s, %s, 'CREATING') ON CONFLICT DO NOTHING",
                            (shard, slot, rid, rid)).rowcount:
                event("SCALE", f"shard {shard}: desired {desired} -> new slot {shard}{slot}")
    slots = meta.execute("SELECT shard_id, slot, generation, replica_id, container, status, ip FROM replica_slots "
                         "ORDER BY shard_id, slot").fetchall()
    for s in slots:                                       # scale down: retire extra slots
        if s[0] not in sets or s[1] > sets[s[0]][0]:
            meta.execute("DELETE FROM replicas WHERE replica_id = %s", (s[3],))
            c = get_container(s[4])
            if c:
                c.remove(force=True)
            meta.execute("DELETE FROM replica_slots WHERE shard_id = %s AND slot = %s", (s[0], s[1]))
            event("SCALE", f"shard {s[0]}: slot {s[0]}{s[1]} retired, {s[4]} removed")
    slots = [s for s in slots if s[0] in sets and s[1] <= sets[s[0]][0]]
    rids = [s[3] for s in slots if s[5] in ("RUNNING", "STARTING")]
    healthy = dict(zip(rids, pool.map(probe, rids)))      # probe all in parallel
    for s in slots:
        try:
            handle(s, sets, healthy)
        except psycopg.OperationalError:
            raise
        except Exception as e:  # one broken slot must not block the others
            log.info("slot %s%s: %s: %s", s[0], s[1], type(e).__name__, e)
    wanted = {s[4] for s in slots}                        # orphans from superseded generations
    for c in dk.containers.list(all=True, filters={"label": "dsbank.managed-by=healer"}):
        if c.name not in wanted and c.name not in {s[4] for s in
                                                   meta.execute("SELECT shard_id, slot, generation, replica_id, "
                                                                "container FROM replica_slots").fetchall()}:
            c.remove(force=True)
            event("ORPHAN", f"removed {c.name} (not referenced by any slot)")


def main():
    global meta
    found = dk.containers.list(all=True, filters={"label": "dsbank.role=bank-replica"})
    log.info("controller start: %d replica containers found by label: %s", len(found),
             ", ".join(f"{c.name}[{c.labels.get('dsbank.shard')}{c.labels.get('dsbank.slot')} "
                       f"g{c.labels.get('dsbank.generation')} {c.status}]" for c in sorted(found, key=lambda c: c.name)))
    for r in meta.execute("SELECT shard_id, slot, generation, replica_id, status FROM replica_slots ORDER BY 1, 2"):
        log.info("slot %s%s gen %s -> %s (%s)", *r)
    while True:
        t0 = time.monotonic()
        try:
            reconcile()
        except psycopg.OperationalError as e:
            log.info("meta-db error: %s - reconnecting", e)
            meta = connect()
        except Exception as e:  # keep reconciling; every step is idempotent
            log.info("reconcile error: %s: %s", type(e).__name__, e)
        time.sleep(max(0.0, PROBE_INTERVAL - (time.monotonic() - t0)))


if __name__ == "__main__":
    main()
