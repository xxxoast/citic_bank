#备付金跨行转账

SELECT 
    t1.*,
    t2.account_type '出金账户类型',
    t3.account_type '入金账户类型'
FROM
    citic_bank.reserve_trade t1
left join 
    citic_bank.reserve t2
on
    t1.`账户` = t2.account_number
left join
    pingan.reserve_third_bank t3
on 
    t1.`对方账号` = t3.`account_number`
WHERE
    t1.`对方开户行` NOT LIKE '%中信%'
and 
    t1.`对方开户行` NOT LIKE '%银联%'
and 
    length(t1.`对方开户行`) > 1
and 
    t1.`借贷` = 1

#线下1、2类户在用状态的重复开立

CREATE TABLE duplicate_personal_account_offline AS 
(
    SELECT 
        t.`客户名` , 
        t.`证件号码` , 
        t.`联系方式` ,
        count(1) as '出现次数'
    FROM
        citic_bank.personal_account_second_offline t
    WHERE
        t.`状态` = '在用'
    and 
        t.`客户账户等级代码` = 1
    GROUP BY 
        t.`客户名` , t.`证件号码` , t.`联系方式`
    HAVING 
        COUNT(1) > 1
);

#3.线下1、2类户在用状态的重复开立明细情况

CREATE TABLE duplicate_personal_account_offline_details AS 
SELECT 
    t1.*
FROM
    citic_bank.personal_account_second_offline t1
right join
    citic_bank.duplicate_personal_account_offline t2
on 
    t1.`客户名` = t2.`客户名` 
AND
    t1.`证件号码` = t2.`证件号码` 
AND
    t1.`联系方式` = t2.`联系方式`

#4.线下1、2类户在用状态的重复开立明细情况日期格式转换

SELECT 
    *
FROM
    citic_bank.duplicate_personal_account_offline_details t
WHERE
    DATE_FORMAT(`开户日期`, '%Y%m%d') > 20170101
AND 
    t.`客户等级代码` = 1

#5.支付机构余额排序

select 
    a.`户名`,max(a.`账户余额`) as '最大余额'
    --,(select b.`户名` from citic_bank.reserve_trade b where b.`账户`=a.`账户` order by a.`账户余额` desc limit 1) as '户名'
from 
    citic_bank.reserve_trade a 
group by a.`账户`
order by max(a.`账户余额`)

#6.一般户未备案，用备案log不准确 

create table yibanhu_diff as
select
    t2.*,
    t3.`业务处理时间` as '备案日期'
from 
(
SELECT * FROM citic_bank.citic_company_account t1
where t1.`账户性质` = '一般存款账户'
) as t2 
left join citic_bank.pbc_company_account_log t3
on 
    t2.`账号` = t3.`账号`


SELECT 
    *,
    datediff(`备案日期`,`开户日期`) 
FROM
    citic_bank.yibanhu_diff
WHERE
    `状态` = '正常'
and
    isnull(`备案日期`)
order by 
    `开户日期`

#准确方式

SELECT 
    *
FROM
(
SELECT 
    *
FROM
    citic_bank.citic_company_account t
WHERE
    t.`状态` = '正常'
)AS t1
WHERE
t1.`账号` 
NOT IN 
(
SELECT 
    `账号`
FROM
    pbc_account_system_status
)

#7.基本户核准

create table jibenhu_hezhun as
select
    t2.*,
    t3.`开户许可证核准号` as '人行开户许可证核准号'
from 
(
SELECT * FROM citic_bank.citic_company_account
where `账户性质` = '基本存款账户'
and `状态` = '正常'
) as t2 
left join 
(
select * from pbc_account_system_status 
where `账户性质` = '基本存款账户'
and `状态` = '正常'
)as t3
on 
    t2.`账号` = t3.`账号`

#8.撤销账户信息

create table delete_exceed_limit as
(
SELECT 
    t3.*, t2.`销户日期` AS '人行销户日期'
FROM
(
    select * from 
    citic_bank.citic_account_delete_status t1
    where t1.`状态` = '销户'
) as t3
LEFT JOIN
    citic_bank.pbc_account_delete_status t2 ON cast(t3.`客户账号` as char(32)) =  cast(t2.`账号` as char(32))
)

#9.改变数据类型

alter table pbc_account_system_status
modify column `账号` 
varchar(64);

#10.加索引 

create index account_number_index on pbc_account_system_status (`账号`) ;

#11.销户未报备

SELECT *,datediff(`人行销户日期`,`销户日期`) as '延迟报备时间' FROM citic_bank.delete_exceed_limit
where isnull(`人行销户日期`)

#12.销户延迟报备

SELECT *,datediff(`人行销户日期`,`销户日期`) as '延迟报备时间' FROM citic_bank.delete_exceed_limit
where datediff(`人行销户日期`,`销户日期`) > 10
order by datediff(`人行销户日期`,`销户日期`) asc

#分离借贷
create table reserve_trade_all_credit as
select * from reserve_trade_all t
where cast(t.`direction` as unsigned) = 1;

create table reserve_trade_all_debt as
select * from reserve_trade_all t
where cast(t.`direction` as unsigned) = 2; 


create index reserve_trade_all_credit_account_index on reserve_trade_all_credit(`origin_account_number`);
create index reserve_trade_all_debt_account_index on reserve_trade_all_debt(`origin_account_number`);

#13.出金账号、金额透视表
create table tmp_credit_account_amount as 
SELECT  t.origin_account_number as '出金账号',
        sum(t.turnover) as '出金总额'
FROM citic_bank.reserve_trade_all_credit t
group by t.origin_account_number;

create table tmp_debt_account_amount as 
SELECT  t.origin_account_number as '入金账号',
        sum(t.turnover) as '入金总额'
FROM citic_bank.reserve_trade_all_debt t
group by t.origin_account_number;
#14.备付金流水，出入金统计
SELECT
    t1.`出金账号` as '账号',
    cast(round(t1.`出金总额` / 10000) as SIGNED) as '出金总额（万）',
    cast(round(t2.`入金总额` / 10000) as SIGNED) as '入金总额（万）',
    (t1.`出金总额` - t2.`入金总额`) as '期限内净出金总额',
    t3.account_name,
    t3.partner,
    t3.delegator,
    t3.account_branch,
    t3.account_type
FROM citic_bank.tmp_credit_account_amount t1
left join
    citic_bank.tmp_debt_account_amount t2
ON 
    t1.`出金账号` = t2.`入金账号`
left join
    citic_bank.reserve t3 
on
    t1.`出金账号` = t3.`account_number`
order by 
    t1.`出金总额` desc;
# 出入金差额
SELECT
    t1.`出金账号` as '账号',
    cast(round(t1.`出金总额` / 10000) as SIGNED) as '出金总额（万）',
    cast(round(t2.`入金总额` / 10000) as SIGNED) as '入金总额（万）',
    cast(round((t1.`出金总额` - t2.`入金总额`)/10000) as SIGNED) as '期限内净出金总额（万）',
    t3.account_name,
    t3.account_type,
    t3.partner,
    t3.delegator,
    t3.account_branch
FROM citic_bank.tmp_credit_account_amount t1
left join
    citic_bank.tmp_debt_account_amount t2
ON 
    t1.`出金账号` = t2.`入金账号`
left join
    citic_bank.reserve t3 
on
    t1.`出金账号` = t3.`account_number`
where 
    t3.account_type = '汇缴账户'
order by 
    -- t1.`出金总额` desc
    cast(round((t1.`出金总额` - t2.`入金总额`)/10000) as SIGNED) desc
#个别支付机构出入金表
create table ainongyizhan_reserve_trade_all_debt as
SELECT * FROM citic_bank.reserve_trade_all_debt t
where 
t.origin_account_number = '8110301012700042605'    
#支付机构每日汇总
create table tmp_ainongyizhan_everyday_credit_summary as 
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
FROM citic_bank.ainongyizhan_reserve_trade_all_credit
group by `date`
order by `date` asc
) as t2
left join citic_bank.ainongyizhan_reserve_trade_all_credit t1
on t2.last_trade_index = t1.`index`
#两表合并
create table tmp_ainongyizhan_everyday_summary as
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
    citic_bank.tmp_ainongyizhan_everyday_credit_summary t1
left join
    citic_bank.tmp_ainongyizhan_everyday_debt_summary t2
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
    citic_bank.tmp_ainongyizhan_everyday_credit_summary t1
right join
    citic_bank.tmp_ainongyizhan_everyday_debt_summary t2
on 
    t1.`date` = t2.`date`
)
order by `date` asc


    

    