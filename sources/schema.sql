CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS card;
CREATE SCHEMA IF NOT EXISTS gl;
CREATE TABLE IF NOT EXISTS core.branches (
 branch_id text PRIMARY KEY, branch_name text NOT NULL, region text NOT NULL,
 city text NOT NULL, created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS core.customers (
 customer_id text PRIMARY KEY, full_name text NOT NULL, email text UNIQUE NOT NULL,
 phone text NOT NULL, date_of_birth date NOT NULL, segment text NOT NULL,
 branch_id text REFERENCES core.branches, status text CHECK(status IN ('ACTIVE','CLOSED')),
 created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS core.customer_history (
 LIKE core.customers INCLUDING DEFAULTS,
 PRIMARY KEY(customer_id, updated_at), FOREIGN KEY(customer_id) REFERENCES core.customers);
CREATE TABLE IF NOT EXISTS core.accounts (
 account_id text PRIMARY KEY CHECK(account_id LIKE 'SYN-%'),
 customer_id text NOT NULL REFERENCES core.customers, branch_id text REFERENCES core.branches,
 account_type text NOT NULL, currency char(3) NOT NULL CHECK(currency IN ('USD','EUR','VND')),
 status text NOT NULL CHECK(status IN ('ACTIVE','FROZEN','CLOSED')),
 opened_at timestamptz NOT NULL, closed_at timestamptz, created_at timestamptz NOT NULL,
 updated_at timestamptz NOT NULL, CHECK(closed_at IS NULL OR closed_at >= opened_at));
CREATE TABLE IF NOT EXISTS core.transfers (
 transfer_id text PRIMARY KEY, source_account_id text NOT NULL REFERENCES core.accounts,
 destination_account_id text NOT NULL REFERENCES core.accounts, amount numeric(18,2) NOT NULL CHECK(amount > 0),
 currency char(3) NOT NULL, status text NOT NULL CHECK(status IN ('SUCCESS','FAILED')),
 created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
 CHECK(source_account_id <> destination_account_id));
CREATE TABLE IF NOT EXISTS core.transactions (
 transaction_id text PRIMARY KEY, account_id text NOT NULL REFERENCES core.accounts,
 transaction_type text NOT NULL CHECK(transaction_type IN
 ('DEPOSIT','WITHDRAWAL','CARD_PAYMENT','TRANSFER_IN','TRANSFER_OUT','REVERSAL_CREDIT','REVERSAL_DEBIT')),
 amount numeric(18,2) NOT NULL CHECK(amount > 0), currency char(3) NOT NULL,
 status text NOT NULL CHECK(status IN ('SUCCESS','FAILED','PENDING')),
 channel text NOT NULL CHECK(channel IN ('MOBILE','CARD','BRANCH','ATM')),
 business_date date NOT NULL, event_time timestamptz NOT NULL, updated_at timestamptz NOT NULL,
 original_transaction_id text REFERENCES core.transactions DEFERRABLE INITIALLY DEFERRED,
 transfer_id text REFERENCES core.transfers,
 CHECK(transaction_id <> original_transaction_id),
 CHECK((transaction_type LIKE 'REVERSAL_%') = (original_transaction_id IS NOT NULL)));
CREATE INDEX IF NOT EXISTS transactions_watermark ON core.transactions(updated_at, transaction_id);
CREATE INDEX IF NOT EXISTS transactions_business_date ON core.transactions(business_date);
CREATE TABLE IF NOT EXISTS card.cards (
 card_id text PRIMARY KEY CHECK(card_id LIKE 'SYN-CARD-%'), account_id text NOT NULL REFERENCES core.accounts,
 card_type text NOT NULL, status text NOT NULL, created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS card.card_transactions (
 card_transaction_id text PRIMARY KEY, card_id text NOT NULL REFERENCES card.cards,
 transaction_id text NOT NULL UNIQUE REFERENCES core.transactions, amount numeric(18,2) NOT NULL CHECK(amount>0),
 currency char(3) NOT NULL, status text NOT NULL, merchant text NOT NULL,
 event_time timestamptz NOT NULL, updated_at timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS gl.gl_accounts (
 gl_account_id text PRIMARY KEY, account_name text NOT NULL, updated_at timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS gl.journal_entries (
 entry_id text PRIMARY KEY, transaction_id text NOT NULL UNIQUE REFERENCES core.transactions,
 business_date date NOT NULL, currency char(3) NOT NULL, updated_at timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS gl.journal_lines (
 line_id text PRIMARY KEY, entry_id text NOT NULL REFERENCES gl.journal_entries,
 gl_account_id text NOT NULL REFERENCES gl.gl_accounts,
 debit numeric(18,2) NOT NULL CHECK(debit>=0), credit numeric(18,2) NOT NULL CHECK(credit>=0),
 updated_at timestamptz NOT NULL, CHECK((debit>0 AND credit=0) OR (credit>0 AND debit=0)));
CREATE TABLE IF NOT EXISTS core.source_totals (
 total_id text PRIMARY KEY, business_date date NOT NULL, currency char(3) NOT NULL,
 source_count bigint NOT NULL, source_amount numeric(22,2) NOT NULL, updated_at timestamptz NOT NULL);

-- Operational updates must not reuse an already observed source version.
CREATE OR REPLACE FUNCTION core.ensure_update_version() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW IS DISTINCT FROM OLD AND NEW.updated_at <= OLD.updated_at THEN
   NEW.updated_at := greatest(clock_timestamp(), OLD.updated_at + interval '1 microsecond');
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS transaction_version ON core.transactions;
CREATE TRIGGER transaction_version BEFORE UPDATE ON core.transactions
 FOR EACH ROW EXECUTE FUNCTION core.ensure_update_version();
DROP TRIGGER IF EXISTS customer_version ON core.customers;
CREATE TRIGGER customer_version BEFORE UPDATE ON core.customers
 FOR EACH ROW EXECUTE FUNCTION core.ensure_update_version();
CREATE OR REPLACE FUNCTION core.capture_customer_history() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO core.customer_history SELECT NEW.* ON CONFLICT(customer_id,updated_at) DO NOTHING;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS customer_history_capture ON core.customers;
CREATE TRIGGER customer_history_capture AFTER INSERT OR UPDATE ON core.customers
 FOR EACH ROW EXECUTE FUNCTION core.capture_customer_history();
