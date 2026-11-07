"""Small helpers shared by the experiment scripts (stdlib only, run on the host)."""
import json
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import client  # noqa: E402  (the real banking client: talks only to the Gateway)

COORD_ADMIN = os.environ.get("COORD_ADMIN", "http://localhost:8090")
B, DIM, R, G, Y, C, M, X = ("\033[1m", "\033[2m", "\033[31m", "\033[32m", "\033[33m", "\033[36m",
                            "\033[35m", "\033[0m")
_checks = []
_started = datetime.now().astimezone()  # local time WITH offset (docker logs --since)


def title(num, name, goal):
    line = "═" * 72
    print(f"\n{M}{B}{line}\n  EXPERIMENT {num}: {name}\n{line}{X}")
    print(f"{DIM}  goal: {goal}{X}")


def step(msg):
    print(f"\n{C}{B}▶ {msg}{X}")


def info(msg):
    print(f"  {msg}")


def note(msg):
    print(f"  {Y}» {msg}{X}")


def check(cond, msg):
    _checks.append(bool(cond))
    print(f"  {G + '✔' if cond else R + '✘'} {msg}{X}")
    return cond


def finish(num):
    ok = all(_checks) and _checks
    verdict = f"{G}{B}PASS{X}" if ok else f"{R}{B}FAIL{X}"
    print(f"\n  {B}Experiment {num}: {verdict}  ({sum(_checks)}/{len(_checks)} checks){X}")
    sys.exit(0 if ok else 1)


def compose(*args, check_rc=True):
    r = subprocess.run(["docker", "compose", *args], cwd=ROOT, capture_output=True, text=True)
    if check_rc and r.returncode != 0:
        raise RuntimeError(f"docker compose {' '.join(args)} failed: {r.stderr}")
    return r.stdout


def sql(db, query):
    """Query a database DIRECTLY (bypassing the platform) -> list of rows (tuples of str)."""
    dbname = "meta" if db == "meta-db" else "bank"
    out = compose("exec", "-T", db, "psql", "-U", "bank", "-d", dbname, "-tA", "-F", "|", "-c", query)
    return [tuple(line.split("|")) for line in out.strip().splitlines() if line]


def sql1(db, query):
    rows = sql(db, query)
    return rows[0][0] if rows else None


def logs(service, since=None):
    since = since or now_iso(_started)
    return compose("logs", "--no-log-prefix", "--since", since, service)


def now_iso(t=None):
    return (t or datetime.now().astimezone()).isoformat(timespec="seconds")


def set_balance(acct, target_cents):
    """Bring an account to an exact balance THROUGH the platform (deposit / withdraw)."""
    cur = db_balance(acct)
    if cur < target_cents:
        client.deposit(acct, fmt(target_cents - cur))
    elif cur > target_cents:
        client.withdraw(acct, fmt(cur - target_cents))
    return db_balance(acct) == target_cents


def admin(method, path, payload=None):
    req = urllib.request.Request(COORD_ADMIN + path, method=method,
                                 data=json.dumps(payload).encode() if payload is not None else None,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read())


def cluster():
    return client.call("GET", "/cluster")[1]


def replica_status():
    return {r["id"]: r["status"] for r in cluster()["replicas"]}


def wait_until(fn, timeout=30, interval=0.5):
    end = time.time() + timeout
    while time.time() < end:
        try:
            v = fn()
            if v:
                return v
        except Exception:
            pass
        time.sleep(interval)
    return None


def money(s):
    """'1000.00' -> 100000 cents (int)."""
    whole, _, frac = str(s).partition(".")
    sign = -1 if whole.startswith("-") else 1
    return sign * (abs(int(whole)) * 100 + int((frac + "00")[:2]))


def fmt(c):
    return f"{'-' if c < 0 else ''}{abs(c) // 100}.{abs(c) % 100:02d}"


def db_balance(acct):
    db = "db-a" if int(acct[1:]) <= 1000 else "db-b"
    v = sql1(db, f"SELECT balance_cents FROM accounts WHERE id = '{acct}'")
    return int(v) if v is not None else None


def total_money():
    a = int(sql1("db-a", "SELECT sum(balance_cents) FROM accounts"))
    b = int(sql1("db-b", "SELECT sum(balance_cents) FROM accounts"))
    return a, b
