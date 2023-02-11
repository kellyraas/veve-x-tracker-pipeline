select 
    date(date) as date
    ,transfers
    ,sellers
    ,buyers
    ,items
    ,wallets as traders
from transfers_daily_agg
where date(date) = '{day}'