select distinct cast(event_time as date) date_key,
       year(event_time) calendar_year, month(event_time) calendar_month, day(event_time) day_of_month
from {{ ref('silver_transactions') }}
