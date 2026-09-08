select transaction_id from {{ ref('fct_transactions') }}
where amount <= 0 or status not in ('SUCCESS','FAILED','PENDING')
   or currency not in ('USD','EUR','VND') or customer_sk is null
   or channel not in ('MOBILE','CARD','BRANCH','ATM')
   or (status <> 'SUCCESS' and signed_amount <> 0)
