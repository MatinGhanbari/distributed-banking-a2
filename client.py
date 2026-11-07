#!/usr/bin/env python3
"""Banking client CLI. Talks ONLY to the Gateway (knows nothing about shards/replicas).

  python client.py balance  A0001
  python client.py deposit  A0001 50.25
  python client.py withdraw A0001 10
  python client.py transfer A0500 A1500 300 [--request-id ID]
  python client.py status   <txn_id>         |  status --request <request_id>
  python client.py cluster                   (shard map + replica health)

Gateway URL: --gateway or $BANK_GATEWAY (default http://localhost:8080).
Writes always carry a request id (random if not given); on a network error the same
request is retried once with the SAME id, so it is applied at most once.
Standard library only.
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid

GATEWAY = os.environ.get("BANK_GATEWAY", "http://localhost:8080")


def call(method, path, payload=None, gateway=None, timeout=40):
    """Returns (http_status, json_body). Network errors -> (0, {...})."""
    url = (gateway or GATEWAY) + path
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except ValueError:
            return e.code, {"ok": False, "error": f"HTTP {e.code}"}
    except (urllib.error.URLError, OSError) as e:
        return 0, {"ok": False, "error": "NETWORK", "message": str(e)}


def write(path, payload, gateway=None):
    payload.setdefault("request_id", f"cli-{uuid.uuid4()}")
    status, body = call("POST", path, payload, gateway)
    if status == 0:  # retry once, same request id => idempotent
        time.sleep(0.5)
        status, body = call("POST", path, payload, gateway)
    body.setdefault("request_id", payload["request_id"])
    return status, body


def balance(acct, gateway=None):
    return call("GET", f"/balance/{acct}", gateway=gateway)


def deposit(acct, amount, request_id=None, gateway=None):
    return write("/deposit", {"account": acct, "amount": str(amount), **_rid(request_id)}, gateway)


def withdraw(acct, amount, request_id=None, gateway=None):
    return write("/withdraw", {"account": acct, "amount": str(amount), **_rid(request_id)}, gateway)


def transfer(src, dst, amount, request_id=None, gateway=None):
    return write("/transfer", {"from": src, "to": dst, "amount": str(amount), **_rid(request_id)}, gateway)


def status(txn_id=None, request_id=None, gateway=None):
    path = f"/txn/by-request/{request_id}" if request_id else f"/txn/{txn_id}"
    return call("GET", path, gateway=gateway)


def _rid(rid):
    return {"request_id": rid} if rid else {}


def main():
    ap = argparse.ArgumentParser(description="Distributed banking client")
    ap.add_argument("--gateway", default=None)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("balance"); s.add_argument("account")
    for name in ("deposit", "withdraw"):
        s = sub.add_parser(name); s.add_argument("account"); s.add_argument("amount")
        s.add_argument("--request-id")
    s = sub.add_parser("transfer"); s.add_argument("src"); s.add_argument("dst"); s.add_argument("amount")
    s.add_argument("--request-id")
    s = sub.add_parser("status"); s.add_argument("txn_id", nargs="?"); s.add_argument("--request")
    sub.add_parser("cluster")
    a = ap.parse_args()
    g = a.gateway
    if a.cmd == "balance":
        st, b = balance(a.account, g)
    elif a.cmd == "deposit":
        st, b = deposit(a.account, a.amount, a.request_id, g)
    elif a.cmd == "withdraw":
        st, b = withdraw(a.account, a.amount, a.request_id, g)
    elif a.cmd == "transfer":
        st, b = transfer(a.src, a.dst, a.amount, a.request_id, g)
    elif a.cmd == "status":
        if not (a.txn_id or a.request):
            ap.error("status needs a txn_id or --request")
        st, b = status(a.txn_id, a.request, g)
    else:
        st, b = call("GET", "/cluster", gateway=g)
    print(json.dumps(b, indent=2, ensure_ascii=False))
    sys.exit(0 if 200 <= st < 300 else 1)


if __name__ == "__main__":
    main()
