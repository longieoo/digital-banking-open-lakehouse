-- Grain: one transaction type and its posting direction.
select transaction_type, direction from (values
    ('DEPOSIT', 1), ('WITHDRAWAL', -1), ('CARD_PAYMENT', -1),
    ('TRANSFER_IN', 1), ('TRANSFER_OUT', -1), ('REVERSAL_CREDIT', 1), ('REVERSAL_DEBIT', -1)
) as types(transaction_type, direction)
