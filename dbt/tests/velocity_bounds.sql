select transaction_id from {{ ref('fact_transactions') }}
where prior_1h_count < 0 or prior_24h_count < prior_1h_count
   or prior_txn_count < prior_24h_count
