"""Read-only local BI for a single hash-verified, certified release per request."""
from __future__ import annotations

import argparse
import csv
from datetime import date
from decimal import Decimal, InvalidOperation
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from bank.release import directory, release_id, verify

WEB = Path(__file__).parent / 'web'
ASSETS = {'/': ('index.html', 'text/html'), '/app.js': ('app.js', 'application/javascript'),
          '/style.css': ('style.css', 'text/css')}
REQUIRED = {'fct_transactions', 'fct_transfers', 'fct_account_balance_daily',
            'mart_customer_360', 'mart_reconciliation_daily', 'dim_account'}


def snapshot(root):
    """Resolve the pointer once. Never mix rows from two publication versions."""
    root = Path(root)
    pointer = root / 'current.json'
    if pointer.is_symlink():
        raise ValueError('Invalid publication pointer')
    current = json.loads(pointer.read_text(encoding='utf-8'))
    value = release_id(current['release_id'])
    if current.get('manifest') != f'{value}/release.json':
        raise ValueError('Invalid manifest route')
    document = verify(value, root=root)
    if current.get('as_of_date') != document['as_of_date']:
        raise ValueError('Publication date mismatch')
    if not REQUIRED <= set(document['exports']):
        raise ValueError('Serving models are missing')
    tables = {}
    for name in REQUIRED:
        item = document['exports'][name]
        data = (directory(value, root) / item['file']).read_bytes()
        # Hash the same bytes used for rendering, including changes after verify().
        if hashlib.sha256(data).hexdigest() != item['sha256']:
            raise ValueError('Certified export changed during read')
        tables[name] = list(csv.DictReader(io.StringIO(data.decode('utf-8'), newline='')))
    return document, tables


def money(rows, column):
    total = Decimal('0')
    for row in rows:
        try:
            value = Decimal(row[column])
        except (InvalidOperation, KeyError) as exc:
            raise ValueError('Invalid monetary field') from exc
        if not value.is_finite():
            raise ValueError('Non-finite monetary field')
        total += value
    return format(total, '.2f')


def report(document, tables, currency=None, start=None, end=None):
    """Aggregate certified Gold only; monetary values stay decimal strings in JSON."""
    currencies = sorted({r['currency'] for r in tables['fct_transactions']} |
                        {r['currency'] for r in tables['fct_account_balance_daily']})
    currency = currency or document['reporting_currency']
    start, end = start or document['start_date'], end or document['as_of_date']
    date.fromisoformat(start)
    date.fromisoformat(end)
    if currency not in currencies or not document['start_date'] <= start <= end <= document['as_of_date']:
        raise ValueError('Filters outside certified bounds')
    transactions = [r for r in tables['fct_transactions']
                    if r['currency'] == currency and start <= r['business_date'] <= end]
    successful = [r for r in transactions if r['status'] == 'SUCCESS']
    balances = [r for r in tables['fct_account_balance_daily']
                if r['currency'] == currency and r['business_date'] <= end]
    balance_date = max((r['business_date'] for r in balances), default=None)
    balances = [r for r in balances if r['business_date'] == balance_date]
    transfers = [r for r in tables['fct_transfers'] if r['currency'] == currency
                 and start <= r['business_date'] <= end and r['status'] == 'SUCCESS']
    reconciliation = [r for r in tables['mart_reconciliation_daily']
                      if r['currency'] == currency and start <= r['business_date'] <= end]
    customers = [r for r in tables['mart_customer_360'] if r['reporting_currency'] == currency]
    daily, channels, types = {}, {}, {}
    for row in transactions:
        for groups, key in [(daily, row['business_date']), (channels, row['channel']),
                            (types, row['transaction_type'])]:
            groups[key] = groups.get(key, 0) + 1
    return {
        'provenance': {key: document.get(key) for key in ['release_id', 'start_date', 'as_of_date',
            'reporting_currency', 'certified_at', 'code_revision', 'validation_code_digest']},
        'filters': {'currency': currency, 'start': start, 'end': end, 'currencies': currencies,
                    'min_date': document['start_date'], 'max_date': document['as_of_date']},
        'summary': {'transactions': len(transactions), 'successful': len(successful),
            'failed': sum(r['status'] == 'FAILED' for r in transactions),
            'transaction_value': money(transactions, 'amount'),
            'posted_value': money(successful, 'amount'),
            'net_posted_movement': money(transactions, 'signed_amount'),
            'transfer_count': len(transfers), 'transferred_value': money(transfers, 'amount'),
            'closing_balance': money(balances, 'closing_balance') if balance_date else None,
            'balance_date': balance_date, 'balance_accounts': len(balances),
            'reconciliation_groups': len(reconciliation),
            'reconciliation_failed': sum(r['reconciliation_status'] != 'PASS' for r in reconciliation)},
        'daily': [{'date': key, 'count': daily[key]} for key in sorted(daily)],
        'channels': [{'name': key, 'count': channels[key]} for key in sorted(channels)],
        'types': [{'name': key, 'count': types[key]} for key in sorted(types)],
        'customers': customers, 'customer_as_of_date': document['as_of_date'],
        'transactions': sorted(transactions, key=lambda r: (r['event_time'], r['transaction_id']), reverse=True)[:100],
        'transaction_limit': 100, 'reconciliation': sorted(reconciliation, key=lambda r: r['business_date']),
        'semantics': {'synthetic': True, 'verified_hashes': True,
            'money_encoding': 'exact decimal strings; one native currency; no FX conversion',
            'transactions': 'Core posting legs; requested amount across statuses. Transfers count twice here.',
            'transfers': 'Successful transfers counted once using fct_transfers.',
            'balances': 'Last available date at or before selected end; never summed across dates.',
            'customers': 'Certified as-of snapshot and 30-day window; independent of selected transaction dates.'}}


def handler(root):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass  # No query strings, identifiers or client data in access logs.

        def respond(self, status, data, content_type='application/json'):
            if not isinstance(data, bytes):
                data = json.dumps(data, ensure_ascii=False).encode('utf-8')
            self.send_response(status)
            self.send_header('Content-Type', content_type + '; charset=utf-8')
            self.send_header('Content-Length', str(len(data)))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self'; "
                             "img-src 'self'; connect-src 'self'; object-src 'none'; frame-ancestors 'none'")
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            url = urlsplit(self.path)
            try:
                if url.path in ASSETS:
                    name, kind = ASSETS[url.path]
                    return self.respond(200, (WEB / name).read_bytes(), kind)
                if url.path not in {'/health', '/api/report'}:
                    return self.respond(404, {'error': 'Not found'})
                query = parse_qs(url.query, keep_blank_values=True)
                if set(query) - {'currency', 'start', 'end'} or any(len(v) != 1 for v in query.values()):
                    return self.respond(400, {'error': 'Invalid filters'})
                document, tables = snapshot(root)
                if url.path == '/health':
                    return self.respond(200, {'status': 'ready', 'release_id': document['release_id'],
                                              'as_of_date': document['as_of_date']})
                try:
                    data = report(document, tables, **{k: v[0] for k, v in query.items()})
                except ValueError:
                    return self.respond(400, {'error': 'Invalid filters'})
                return self.respond(200, data)
            except (ValueError, KeyError, TypeError, OSError, csv.Error):
                return self.respond(503, {'error': 'Certified data unavailable or integrity check failed'})
    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8090)
    parser.add_argument('--root', default=os.environ.get('RELEASE_ROOT', 'runtime/releases'))
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), handler(args.root))
    print(f'Local synthetic-data dashboard on port {args.port}', flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
