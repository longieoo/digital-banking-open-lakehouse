DO $$ BEGIN
 IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='bank_ingest') THEN CREATE ROLE bank_ingest NOLOGIN; END IF;
 IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='bank_analyst') THEN CREATE ROLE bank_analyst NOLOGIN; END IF;
END $$;
GRANT USAGE ON SCHEMA core,card,gl TO bank_ingest;
GRANT SELECT ON ALL TABLES IN SCHEMA core,card,gl TO bank_ingest;
GRANT INSERT,UPDATE ON core.source_totals TO bank_ingest;
CREATE SCHEMA IF NOT EXISTS reporting;
CREATE OR REPLACE VIEW reporting.masked_customers AS
 SELECT customer_id, 'Customer ***' || right(customer_id,4) AS masked_name,
 'SYN-***' || right(phone,2) AS masked_phone, segment,branch_id,status FROM core.customers;
GRANT USAGE ON SCHEMA reporting TO bank_analyst;
GRANT SELECT ON reporting.masked_customers TO bank_analyst;
