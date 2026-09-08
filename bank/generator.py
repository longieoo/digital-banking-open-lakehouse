"""Deterministic, constrained synthetic sources (all identifiers are explicitly fake)."""
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import random

PROFILES = {"small": (20, 200), "medium": (1000, 100000), "large": (10000, 1000000)}
EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


def stamp(days=0, seconds=0):
    return (EPOCH + timedelta(days=days, seconds=seconds)).isoformat()


def generate(profile="small", seed=42, customers=None, transactions=None):
    """Amounts are decimal strings at the JSON boundary, never binary floats.

    Core transactions are account postings. Transfers have two explicit posting
    legs; card purchases have exactly one core posting. Failed events never post.
    Opening balances are zero. Withdrawals may overdraw (educational simplification).
    """
    n, count = PROFILES[profile]
    n = n if customers is None else customers
    count = count if transactions is None else transactions
    if n < 2 or count < 1:
        raise ValueError("Need at least two customers and one transaction")
    rng = random.Random(seed)
    out = {k: [] for k in (
        "branches", "customers", "customer_history", "accounts", "transactions",
        "transfers", "cards", "card_transactions", "gl_accounts", "journal_entries",
        "journal_lines", "source_totals",
    )}
    out["branches"] = [dict(branch_id=f"SYN-B{i}", branch_name=f"Synthetic Branch {i}",
                            region="Demo", city=f"Demo City {i}", created_at=stamp(), updated_at=stamp())
                       for i in range(2)]
    for i in range(n):
        customer = dict(customer_id=f"SYN-C{i:06d}", full_name=f"Synthetic Customer {i}",
                        email=f"customer-{i}@example.invalid", phone=f"SYN-PHONE-{i}",
                        date_of_birth="1990-01-01", segment="Standard", branch_id=f"SYN-B{i % 2}",
                        status="ACTIVE", created_at=stamp(), updated_at=stamp())
        out["customer_history"].append(dict(customer))
        if i % 3 == 0:
            customer = {**customer, "segment": "Priority", "branch_id": f"SYN-B{(i + 1) % 2}",
                        "updated_at": stamp(15)}
            out["customer_history"].append(dict(customer))
        out["customers"].append(customer)
        out["accounts"].append(dict(account_id=f"SYN-A{i:06d}", customer_id=customer["customer_id"],
            branch_id=f"SYN-B{i % 2}", account_type="CURRENT", currency="USD",
            status="FROZEN" if i % 7 == 0 else "ACTIVE", opened_at=stamp(), closed_at=None,
            created_at=stamp(), updated_at=stamp(20) if i % 7 == 0 else stamp()))
        out["cards"].append(dict(card_id=f"SYN-CARD-{i:06d}", account_id=f"SYN-A{i:06d}",
            card_type="DEBIT", status="ACTIVE", created_at=stamp(), updated_at=stamp()))
    out["gl_accounts"] = [dict(gl_account_id=k, account_name=v, updated_at=stamp()) for k, v in
                          [("SYN-GL-CASH", "Cash control"), ("SYN-GL-DEPOSIT", "Customer deposits")]]

    def posting(index, account, kind, amount, day, status="SUCCESS", original=None, transfer=None):
        time = stamp(day, index % 3600)
        txn = dict(transaction_id=f"SYN-T{index:09d}", account_id=account, transaction_type=kind,
                   amount=str(amount), currency="USD", status=status,
                   channel="CARD" if kind == "CARD_PAYMENT" else "MOBILE", business_date=time[:10],
                   event_time=time, updated_at=time, original_transaction_id=original, transfer_id=transfer)
        out["transactions"].append(txn)
        if status == "SUCCESS":
            eid = f"SYN-J{index:09d}"
            out["journal_entries"].append(dict(entry_id=eid, transaction_id=txn["transaction_id"],
                business_date=time[:10], currency="USD", updated_at=time))
            is_credit = kind in {"DEPOSIT", "TRANSFER_IN", "REVERSAL_CREDIT"}
            for side, gl in [("debit", "SYN-GL-CASH" if is_credit else "SYN-GL-DEPOSIT"),
                             ("credit", "SYN-GL-DEPOSIT" if is_credit else "SYN-GL-CASH")]:
                out["journal_lines"].append(dict(line_id=f"{eid}-{side}", entry_id=eid,
                    gl_account_id=gl, debit=str(amount if side == "debit" else Decimal(0)),
                    credit=str(amount if side == "credit" else Decimal(0)), updated_at=time))
        return txn

    for i in range(count):
        account = f"SYN-A{rng.randrange(n):06d}"
        amount = Decimal(rng.randrange(1, 500001)) / 100
        day = rng.randrange(30)
        kind = ["DEPOSIT", "WITHDRAWAL", "CARD_PAYMENT"][i % 3]
        txn = posting(i, account, kind, amount, day, "FAILED" if i % 19 == 0 else "SUCCESS")
        if kind == "CARD_PAYMENT":
            out["card_transactions"].append(dict(card_transaction_id=f"SYN-CT{i:09d}",
                card_id=f"SYN-CARD-{int(account[5:]):06d}", transaction_id=txn["transaction_id"],
                amount=str(amount), currency="USD", status=txn["status"], merchant="SYN-MERCHANT",
                event_time=txn["event_time"], updated_at=txn["updated_at"]))
        if i % 31 == 1 and kind != "DEPOSIT" and txn["status"] == "SUCCESS":
            posting(count + i, account, "REVERSAL_CREDIT", amount, min(day + 1, 30),
                    original=txn["transaction_id"])
    for i in range(max(1, count // 20)):
        source = i % n
        dest = (source + 1) % n
        tid = f"SYN-X{i:09d}"
        amount = Decimal("25.00")
        out["transfers"].append(dict(transfer_id=tid, source_account_id=f"SYN-A{source:06d}",
            destination_account_id=f"SYN-A{dest:06d}", amount=str(amount), currency="USD",
            status="SUCCESS", created_at=stamp(i % 30), updated_at=stamp(i % 30)))
        posting(2 * count + 2 * i, f"SYN-A{source:06d}", "TRANSFER_OUT", amount, i % 30, transfer=tid)
        posting(2 * count + 2 * i + 1, f"SYN-A{dest:06d}", "TRANSFER_IN", amount, i % 30, transfer=tid)
    totals = defaultdict(lambda: {"source_count": 0, "source_amount": Decimal(0)})
    for t in out["transactions"]:
        total = totals[t["business_date"]]
        total["source_count"] += 1
        if t["status"] == "SUCCESS":
            total["source_amount"] += Decimal(t["amount"])
    out["source_totals"] = [dict(total_id=f"{day}-USD", business_date=day, currency="USD",
        source_count=v["source_count"], source_amount=str(v["source_amount"]), updated_at=stamp(32))
        for day, v in sorted(totals.items())]
    return out


def failure_cases(data):
    """Return isolated test input, never silently seed invalid rows into constrained SQL."""
    row = dict(data["transactions"][1])
    return {"duplicate": [row, dict(row)],
            "invalid": [{**row, "amount": "-1.00"}],
            "late_update": [{**row, "updated_at": stamp(40), "channel": "BRANCH"}]}
