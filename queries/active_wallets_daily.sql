with wallets as
(
select 
  	date(date) as date
    ,wallet
from wallet_activity
where date(date) = '{day}'
group by 1,2
)

select
w.date
,count(distinct w.wallet) as active
,sum(case when date(w.date) = date(first_active) then 1 else 0 end) as new 
from wallets as w
join dim_wallets as dw
on w.wallet = dw.wallet
group by 1