select transaction_id from {{ ref('fact_transactions') }} where amount < 0 or amount > 100000
