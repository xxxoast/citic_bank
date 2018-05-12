


create table shenyinlianwangluo_reserve_trade_all_debt as
SELECT * FROM citic_bank.reserve_trade_all_debt t
where 
t.origin_account_number = '7605110182600007411';

create table shenyinlianwangluo_reserve_trade_all_credit as
SELECT * FROM citic_bank.reserve_trade_all_credit t
where 
t.origin_account_number = '7605110182600007411';

################################################################
create table tmp_shenyinlianwangluo_everyday_credit_summary as 
select 
    t2.`date`,
    t2.total_turnover,
    t2.trade_count,
    t2.last_trade_index,
    t1.residual,
    t1.`timestamp`
from 
(
SELECT 
    date,
    sum(turnover) as total_turnover,
    max(`index`) as last_trade_index,
    count(1) as trade_count
FROM citic_bank.shenyinlianwangluo_reserve_trade_all_credit
group by `date`
order by `date` asc
) as t2
left join citic_bank.shenyinlianwangluo_reserve_trade_all_credit t1
on t2.last_trade_index = t1.`index`;

create table tmp_shenyinlianwangluo_everyday_debt_summary as 
select 
    t2.`date`,
    t2.total_turnover,
    t2.trade_count,
    t2.last_trade_index,
    t1.residual,
    t1.`timestamp`
from 
(
SELECT 
    date,
    sum(turnover) as total_turnover,
    max(`index`) as last_trade_index,
    count(1) as trade_count
FROM citic_bank.shenyinlianwangluo_reserve_trade_all_debt
group by `date`
order by `date` asc
) as t2
left join citic_bank.shenyinlianwangluo_reserve_trade_all_debt t1
on t2.last_trade_index = t1.`index`
###################################################################
create table tmp_shenyinlianwangluo_everyday_summary as
(
SELECT 
    t1.`date` as 'date',
    cast(round(t1.`total_turnover`) as SIGNED) as 'credit_turnover',
    t1.`trade_count` as 'credit_trade_count',
    cast(round(t1.residual) as SIGNED) as 'credit_residual',
    t1.`timestamp` as 'credit_last_timestamp',
    t1.`last_trade_index` as 'credit_last_trade_index',
    cast(round(t2.`total_turnover`) as SIGNED) as 'debt_turnover',
    t2.`trade_count` as 'debt_trade_count',
    cast(round(t2.residual) as SIGNED) as 'debt_residual',
    t2.`timestamp` as 'debt_last_timestamp',
    t2.`last_trade_index` as 'debt_last_trade_index'
FROM
    citic_bank.tmp_shenyinlianwangluo_everyday_credit_summary t1
left join
    citic_bank.tmp_shenyinlianwangluo_everyday_debt_summary t2
on 
    t1.`date` = t2.`date`
)
union
(
SELECT 
    t2.`date` as 'date',
    cast(round(t1.`total_turnover`) as SIGNED) as 'credit_turnover',
    t1.`trade_count` as 'credit_trade_count',
    cast(round(t1.residual) as SIGNED) as 'credit_residual',
    t1.`timestamp` as 'credit_last_timestamp',
    t1.`last_trade_index` as 'credit_last_trade_index',
    cast(round(t2.`total_turnover`) as SIGNED) as 'debt_turnover',
    t2.`trade_count` as 'debt_trade_count',
    cast(round(t2.residual) as SIGNED) as 'debt_residual',
    t2.`timestamp` as 'debt_last_timestamp',
    t2.`last_trade_index` as 'debt_last_trade_index'
FROM
    citic_bank.tmp_shenyinlianwangluo_everyday_credit_summary t1
right join
    citic_bank.tmp_shenyinlianwangluo_everyday_debt_summary t2
on 
    t1.`date` = t2.`date`
)
order by `date` asc
###################################################################
