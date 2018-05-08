#备付金跨行转账
'''
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
'''
#线下1、2类户在用状态的重复开立
'''
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
'''
#3.details
'''
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
'''
#日期格式转换
'''
SELECT 
    *
FROM
    citic_bank.duplicate_personal_account_offline_details t
WHERE
    DATE_FORMAT(`开户日期`, '%Y%m%d') > 20170101
AND 
    t.`客户等级代码` = 1
'''
#支付机构余额排序
'''
select 
    a.`户名`,max(a.`账户余额`) as '最大余额'
    --,(select b.`户名` from citic_bank.reserve_trade b where b.`账户`=a.`账户` order by a.`账户余额` desc limit 1) as '户名'
from 
    citic_bank.reserve_trade a 
group by a.`账户`
order by max(a.`账户余额`)
'''
#一般户未备案
'''
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
'''
'''
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
'''
#基本户核准
'''
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
'''