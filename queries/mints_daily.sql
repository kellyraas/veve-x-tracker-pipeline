select 
    date(date) as date
    ,wallets
    ,items
from mints_daily_agg
where date(date) = '{day}'