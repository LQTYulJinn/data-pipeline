select card_id, min(event_time) first_seen_at, count(*) transaction_count
from {{ ref('silver_transactions') }} group by card_id
