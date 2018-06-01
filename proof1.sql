#1.汇缴户日终未清0
create table citic_bank_proof.reserve_residual_every_day as
select 
		t3.*, t4.account_type 
from
(
select  t1.origin_account_number,
		t1.`date`,
        t1.origin_account_name,
        t2.residual as residual,
        t2.`index`
from 
		citic_bank.every_account_every_day_last_trade t1,
        citic_bank.reserve_trade_all t2
where 
		t1.`index` = t2.`index`
) as t3
left join citic_bank.reserve t4
on t3.origin_account_number = t4.account_number;
#2筛选汇缴户&日终未清0明细
create table citic_bank_proof.reserve_residual_every_day_detail as  
SELECT * FROM citic_bank_proof.reserve_residual_every_day 
where   account_type = '汇缴账户' and   residual > 0.1
#3汇缴户日终未清0汇总
SELECT 
    origin_account_number,
    origin_account_name,
    account_type,
    count(1) as '未清0天数',
    cast(avg(residual) as unsigned) as '未清0平均余额'
FROM
    citic_bank_proof.reserve_residual_every_day_detail
GROUP BY 
	origin_account_number
;
#4.示例 
SELECT * FROM citic_bank.reserve_trade_all 
where origin_account_number = '8110301012800262923' and `date` = '2018-01-02'
#5.过滤结果，排除干扰项
SELECT * FROM citic_bank_proof.reserve_residual_every_day_summary
where `未清0平均余额` > 10000 or `未清0天数` > 10

##############################################################################
