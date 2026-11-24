select merchant_id, min(category) category, count(*) transaction_count
from {{ ref('silver_transactions') }} group by merchant_id
