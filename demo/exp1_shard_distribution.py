#!/usr/bin/env python3
"""Experiment 1 - accounts live only in their assigned shard DB; the Gateway routes correctly."""
from lib import *

title(1, "Shard distribution", "each account is stored in exactly one shard DB and the Gateway routes to it")

step("Shard Map (from meta-db, as used by the Gateway)")
for r in cluster()["ranges"]:
    info(f"shard {r['shard']}: {r['from']} .. {r['to']}   (epoch {r['epoch']})")

step("Rows physically stored in each shard database (direct SQL)")
for db in ("db-a", "db-b"):
    n, lo, hi = sql(db, "SELECT count(*), min(id), max(id) FROM accounts")[0]
    info(f"{db}: {n} accounts, {lo} .. {hi}")
check(sql1("db-a", "SELECT count(*) FROM accounts WHERE num > 1000") == "0", "db-a holds no Shard B accounts")
check(sql1("db-b", "SELECT count(*) FROM accounts WHERE num <= 1000") == "0", "db-b holds no Shard A accounts")

step("Route test accounts through the Gateway and compare with the databases")
print(f"  {B}{'account':8} {'expected':9} {'gateway→':9} {'replica':8} {'in db-a':8} {'in db-b':8}{X}")
for acct in ("A0001", "A0500", "A1000", "A1001", "A1500", "A2000"):
    expected = "A" if int(acct[1:]) <= 1000 else "B"
    st, b = client.balance(acct)
    in_a = sql1("db-a", f"SELECT count(*) FROM accounts WHERE id='{acct}'")
    in_b = sql1("db-b", f"SELECT count(*) FROM accounts WHERE id='{acct}'")
    info(f"{acct:8} {expected:9} {b.get('shard', '?'):9} {b.get('replica', '?'):8} {in_a:8} {in_b:8}")
    good = st == 200 and b.get("shard") == expected and (in_a, in_b) == (("1", "0") if expected == "A" else ("0", "1"))
    check(good, f"{acct} owned by shard {expected}, stored only in db-{expected.lower()}, served by {b.get('replica')}")

step("A shard itself refuses accounts it does not own (direct call to bank-a1, bypassing the Gateway)")
out = compose("exec", "-T", "bank-a1", "python", "-c",
              "import urllib.request as u\ntry: u.urlopen('http://localhost:8000/accounts/A1500?epoch=1')\n"
              "except Exception as e: print(e.code, e.read().decode())")
info(out.strip()[:140])
check("421" in out and "WRONG_SHARD" in out, "bank-a1 rejects A1500 with WRONG_SHARD (ownership checked at the shard)")
finish(1)
