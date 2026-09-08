from decimal import Decimal


def risk_sql(threshold='10000.00'):
    amount = Decimal(threshold)
    if not amount.is_finite() or amount <= 0:
        raise ValueError('Positive finite threshold required')
    return f'''
    WITH counts AS (
      SELECT transaction_id, account_id, event_time, amount, status,
        sum(CASE WHEN transaction_type='TRANSFER_OUT' AND status='SUCCESS' THEN 1 ELSE 0 END)
          OVER (PARTITION BY account_id ORDER BY event_time
                RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW) AS transfer_count,
        sum(CASE WHEN status='FAILED' THEN 1 ELSE 0 END)
          OVER (PARTITION BY account_id ORDER BY event_time
                RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW) AS failure_count
      FROM risk_events
    )
    SELECT transaction_id,account_id,event_time,'AMOUNT' AS rule FROM counts
      WHERE amount > CAST('{amount}' AS DECIMAL(22,2)) AND status='SUCCESS'
    UNION ALL
    SELECT transaction_id,account_id,event_time,'TRANSFER_VELOCITY' AS rule FROM counts WHERE transfer_count >= 3
    UNION ALL
    SELECT transaction_id,account_id,event_time,'REPEATED_FAILURES' AS rule FROM counts WHERE failure_count >= 3
    '''
