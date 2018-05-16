#1.初步筛选
SELECT 
    origin_account_number,
    origin_account_name,
    count(1),
    sum(turnover),
    opponent_account_number,
    opponent_account_name,
    opponent_bank_name,
    abstract
FROM
    citic_bank.reserve_trade_all_credit
GROUP BY origin_account_number,opponent_account_number
HAVING opponent_account_name LIKE '%备付金%'