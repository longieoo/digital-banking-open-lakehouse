"""Small explicit source contracts shared by Silver and validation tooling."""
from bank.adapters import TABLES

DECIMALS = {'amount', 'debit', 'credit', 'source_amount'}
DATES = {'date_of_birth', 'business_date'}
TIMESTAMPS = {'event_time', 'updated_at', 'created_at', 'opened_at', 'closed_at'}
NULLABLE = {'closed_at', 'original_transaction_id', 'transfer_id'}
CREDIT_TYPES = {'DEPOSIT', 'TRANSFER_IN', 'REVERSAL_CREDIT'}
DEBIT_TYPES = {'WITHDRAWAL', 'CARD_PAYMENT', 'TRANSFER_OUT', 'REVERSAL_DEBIT'}
REFERENCES = {
    'customers': [('branch_id', 'branches', 'branch_id')],
    'customer_history': [('customer_id', 'customers', 'customer_id')],
    'accounts': [('customer_id', 'customers', 'customer_id'), ('branch_id', 'branches', 'branch_id')],
    'transfers': [('source_account_id', 'accounts', 'account_id'),
                  ('destination_account_id', 'accounts', 'account_id')],
    'transactions': [('account_id', 'accounts', 'account_id'), ('transfer_id','transfers','transfer_id')],
    'cards': [('account_id', 'accounts', 'account_id')],
    'card_transactions': [('card_id', 'cards', 'card_id'), ('transaction_id', 'transactions', 'transaction_id')],
    'journal_entries': [('transaction_id', 'transactions', 'transaction_id')],
    'journal_lines': [('entry_id', 'journal_entries', 'entry_id'), ('gl_account_id', 'gl_accounts', 'gl_account_id')],
}


def fields(table):
    # Generator is the executable fixture contract; no randomness affects columns.
    from bank.generator import generate
    return list(generate(customers=2, transactions=3)[table][0])


def column_type(name):
    if name in DECIMALS:
        return 'decimal(22,2)'
    if name in DATES:
        return 'date'
    if name in TIMESTAMPS:
        return 'timestamp'
    if name == 'source_count':
        return 'bigint'
    return 'string'


def primary_key(table):
    return [TABLES[table][1], 'updated_at'] if table == 'customer_history' else [TABLES[table][1]]
