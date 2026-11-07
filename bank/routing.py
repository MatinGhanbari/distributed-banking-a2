"""Shard-aware routing shared by the Gateway and the Coordinator.

1. Shard selection: the Shard Map (table shard_ranges in meta-db, range based) is
   cached and refreshed every MAP_REFRESH seconds; account number -> (shard, epoch).
2. Replica selection: Round Robin over the *healthy* replicas of that shard.
   Health = active checks (GET /health every HEALTH_INTERVAL s) + passive (a connection
   error while forwarding marks the replica DOWN at once and the request is retried on
   the next healthy replica; safe because every write carries a request id).
The replica list also comes from meta-db (table `replicas`), so replacement replicas
registered there (Bonus II) are picked up without restarting the Gateway.
"""
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import psycopg

from .common import BankError, acct_num

MAP_REFRESH = float(os.environ.get("MAP_REFRESH", "2"))
HEALTH_INTERVAL = float(os.environ.get("HEALTH_INTERVAL", "1"))


class Replica:
    def __init__(self, rid, shard, url):
        self.id, self.shard, self.url = rid, shard, url
        self.healthy = False


class Router:
    def __init__(self, meta_dsn, log, request_timeout=15.0):
        self.dsn, self.log = meta_dsn, log
        self.lock = threading.Lock()
        self.ranges = []          # [(lo, hi, shard, epoch)]
        self.map_epoch = 0
        self.replicas = {}        # id -> Replica
        self.rr = {}              # shard -> round robin counter
        self.http = httpx.Client(timeout=httpx.Timeout(request_timeout, connect=1.0),
                                 limits=httpx.Limits(max_connections=200, max_keepalive_connections=50))
        self.probe = httpx.Client(timeout=0.8)
        self.pool = ThreadPoolExecutor(max_workers=8)

    # ------------------------------------------------------------ lifecycle
    def start(self):
        while True:
            try:
                self.refresh()
                break
            except Exception as e:
                self.log.info("waiting for meta-db: %s", e)
                time.sleep(1)
        self.check_all()  # know replica health before serving the first request
        threading.Thread(target=self._loop, args=(self.refresh, MAP_REFRESH), daemon=True).start()
        threading.Thread(target=self._loop, args=(self.check_all, HEALTH_INTERVAL), daemon=True).start()

    def _loop(self, fn, interval):
        while True:
            time.sleep(interval)
            try:
                fn()
            except Exception as e:
                self.log.info("router background task failed: %s", e)

    def refresh(self):
        with psycopg.connect(self.dsn, autocommit=True, connect_timeout=3) as c:
            ranges = c.execute("SELECT range_lo, range_hi, shard_id, epoch FROM shard_ranges "
                               "ORDER BY range_lo").fetchall()
            epoch = c.execute("SELECT epoch FROM map_epoch").fetchone()[0]
            reps = c.execute("SELECT replica_id, shard_id, url FROM replicas WHERE enabled "
                             "ORDER BY replica_id").fetchall()
        with self.lock:
            if epoch != self.map_epoch:
                self.log.info("SHARD MAP epoch %s: %s", epoch,
                              ", ".join(f"{s}=A{lo:04d}-A{hi:04d}(e{e})" for lo, hi, s, e in ranges))
            self.ranges, self.map_epoch = ranges, epoch
            wanted = {r[0] for r in reps}
            for rid, shard, url in reps:
                cur = self.replicas.get(rid)
                if cur is None or cur.url != url or cur.shard != shard:
                    self.replicas[rid] = Replica(rid, shard, url)
            for rid in list(self.replicas):
                if rid not in wanted:
                    del self.replicas[rid]

    def check_all(self):
        reps = list(self.replicas.values())
        list(self.pool.map(self._check, reps))

    def _check(self, r: Replica):
        try:
            up = self.probe.get(r.url + "/health").status_code == 200
        except httpx.HTTPError:
            up = False
        if up != r.healthy:
            self.log.info("HEALTH %s (shard %s) %s -> %s", r.id, r.shard,
                          "UP" if r.healthy else "DOWN", "UP" if up else "DOWN")
        r.healthy = up

    def mark_down(self, r: Replica, why):
        if r.healthy:
            self.log.info("HEALTH %s (shard %s) UP -> DOWN (request failed: %s)", r.id, r.shard, why)
        r.healthy = False

    # ------------------------------------------------------------ routing
    def shard_for(self, acct):
        num = acct_num(acct)
        with self.lock:
            for lo, hi, shard, epoch in self.ranges:
                if lo <= num <= hi:
                    return shard, epoch
        raise BankError("NO_SUCH_ACCOUNT", 404, f"no shard owns account {acct}")

    def pick(self, shard, exclude=()):
        with self.lock:
            healthy = [r for r in sorted(self.replicas.values(), key=lambda r: r.id)
                       if r.shard == shard and r.healthy and r.id not in exclude]
            if not healthy:
                return None
            i = self.rr.get(shard, 0)
            self.rr[shard] = i + 1
            return healthy[i % len(healthy)]

    def call(self, shard, method, path, **kw):
        """Send to one healthy replica of `shard`; fail over to the next on transport errors."""
        tried = set()
        while True:
            r = self.pick(shard, tried)
            if r is None:
                raise BankError("SHARD_UNAVAILABLE", 503, f"no healthy replica for shard {shard}")
            tried.add(r.id)
            try:
                resp = self.http.request(method, r.url + path, **kw)
                resp.json()  # a non-JSON answer did not come from a bank replica
                return r, resp
            except httpx.TransportError as e:
                self.mark_down(r, type(e).__name__)
            except ValueError:
                # e.g. a stopped container's name resolved by an upstream DNS/proxy
                self.mark_down(r, f"non-JSON HTTP {resp.status_code}")

    def snapshot(self):
        with self.lock:
            return {
                "map_epoch": self.map_epoch,
                "ranges": [{"shard": s, "from": f"A{lo:04d}", "to": f"A{hi:04d}", "epoch": e}
                           for lo, hi, s, e in self.ranges],
                "replicas": [{"id": r.id, "shard": r.shard, "url": r.url,
                              "status": "UP" if r.healthy else "DOWN"}
                             for r in sorted(self.replicas.values(), key=lambda r: r.id)],
            }
