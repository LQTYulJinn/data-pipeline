select transaction_id, cast(event_time as timestamp) event_time, card_id, source_file,
       merchant_id, category, cast(amount as double) amount,
       cast(fraud_label as integer) fraud_label,
       cast(prior_txn_count as integer) prior_txn_count,
       cast(prior_1h_count as integer) prior_1h_count,
       cast(prior_24h_count as integer) prior_24h_count
from {{ ref('bronze_transactions') }}
