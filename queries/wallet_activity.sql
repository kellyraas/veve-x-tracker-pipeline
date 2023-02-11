select
    wallet
    ,date(date) as date
    ,sales
    ,purchases
    ,mints
from wallet_activity
where date(date) between '{start_date}' and '{end_date}'
and wallet IN ({list})