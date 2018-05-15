#跨行向非备付金户出金
create table citic_bank_proof.reserve_inter_bank_credit as
SELECT * FROM citic_bank.reserve_trade_all_credit
group by 
	opponent_account_name,
    opponent_bank_name
having 
	opponent_bank_name not like '%中信%'
and 
	opponent_account_name not like '%备付金%'
and 
	opponent_account_number <> '1100000000'
#过滤干扰
create table citic_bank_proof.reserve_inter_bank_credit_filtered as
select 
		t.*, t4.account_type 
from 
(
SELECT * FROM citic_bank_proof.reserve_inter_bank_credit
where 
	opponent_bank_name <> '长沙银杉路支行'
and
	left(opponent_account_number,4) <> '6217'
and 
	left(opponent_account_number,4) <> '6226'
and 
	left(opponent_account_number,4) <> '7440'
and 
	left(opponent_account_number,4) <> '7441'
and 
	left(opponent_account_number,4) <> '7442'
and 
	left(opponent_account_number,4) <> '4336'
and 
	turnover > 100
and
	opponent_account_name not like '%清算款%'
order by origin_account_number
) as t
left join citic_bank.reserve t4
on t.origin_account_number = t4.account_number
##分离汇缴收付
create table citic_bank_proof.reserve_inter_bank_credit_filtered_huijiao as
SELECT * FROM citic_bank_proof.reserve_inter_bank_credit_filtered
where account_type = '汇缴账户'
order by origin_account_number;

create table citic_bank_proof.reserve_inter_bank_credit_filtered_shoufu as
SELECT * FROM citic_bank_proof.reserve_inter_bank_credit_filtered
where account_type = '收付账户'
order by origin_account_number
##计算汇总情况
SELECT 
    origin_account_number,
    origin_account_name,
    count(1) as '出金次数',
    cast(avg(turnover) as unsigned) as '平均每笔出金额'
FROM
    citic_bank_proof.reserve_inter_bank_credit_filtered_huijiao
GROUP BY origin_account_number
#导出
SELECT * FROM citic_bank_proof.reserve_inter_bank_credit_filtered_shoufu
INTO OUTFILE "/tmp/shoufu.csv"
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n';

