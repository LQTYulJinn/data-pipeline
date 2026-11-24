select transaction_id, event_time, cast(event_time as date) date_key, source_file,
       card_id, merchant_id, amount, fraud_label,
       prior_txn_count, prior_1h_count, prior_24h_count
from {{ ref('silver_transactions') }}
