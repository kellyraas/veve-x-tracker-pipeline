select 
    wallet
    ,token_count
from dim_wallets
where wallet not in ({wallets_to_exclude})
order by 2 desc
limit '{limit}'

