"""Shared helpers: account ids, money (integer cents, never float), errors, logging."""
import logging
import re
import sys
from decimal import Decimal, InvalidOperation

ACCOUNT_RE = re.compile(r"^A\d{4}$")
MAX_AMOUNT_CENTS = 100_000_000_000  # 1,000,000,000.00 per operation
TXN_ID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


class BankError(Exception):
    """Expected, client-visible error with a machine-readable code and an HTTP status."""

    def __init__(self, code, status, message, **extra):
        super().__init__(message)
        self.code, self.status, self.message, self.extra = code, status, message, extra

    def body(self, **ctx):
        return {"ok": False, "error": self.code, "message": self.message, **self.extra, **ctx}


def acct_num(acct) -> int:
    """'A0042' -> 42 (raises BAD_REQUEST on a malformed id)."""
    if not isinstance(acct, str) or not ACCOUNT_RE.match(acct):
        raise BankError("BAD_REQUEST", 400, f"invalid account id {acct!r} (expected A0001..A9999)")
    return int(acct[1:])


def parse_amount(value) -> int:
    """Positive amount with <= 2 decimals -> integer cents."""
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise BankError("BAD_REQUEST", 400, f"invalid amount {value!r}")
    if not d.is_finite() or d <= 0 or d != d.quantize(Decimal("0.01")):
        raise BankError("BAD_REQUEST", 400, "amount must be positive with at most 2 decimals")
    cents = int(d * 100)
    if cents > MAX_AMOUNT_CENTS:
        raise BankError("BAD_REQUEST", 400, "amount too large")
    return cents


def fmt(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    cents = abs(cents)
    return f"{sign}{cents // 100}.{cents % 100:02d}"


def require_request_id(rid) -> str:
    if not isinstance(rid, str) or not (1 <= len(rid) <= 100):
        raise BankError("BAD_REQUEST", 400, "request_id (1..100 chars) is required")
    return rid


def check_txn_id(txn_id) -> str:
    # txn ids end up inside 'PREPARE TRANSACTION <literal>' -> strictly validated
    if not isinstance(txn_id, str) or not TXN_ID_RE.match(txn_id):
        raise BankError("BAD_REQUEST", 400, "invalid txn_id")
    return txn_id


def setup_logging(name: str) -> logging.Logger:
    logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                        format=f"%(asctime)s [{name}] %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logging.getLogger(name)
