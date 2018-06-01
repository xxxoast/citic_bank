SELECT * FROM citic_bank.reserve_trade_all_debt
where opponent_account_name like '%财付通%'
and 
	origin_account_number <> '7440810182200001028' 
INTO OUTFILE "/tmp/caifutong转入.csv"
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n';

SELECT 
    origin_account_number,
    origin_account_name,
    COUNT(1),
    CAST(SUM(turnover) AS UNSIGNED) '交易量',
    opponent_account_number,
    opponent_account_name,
    opponent_bank_name
FROM
    citic_bank.reserve_trade_all_debt
WHERE
    opponent_account_name LIKE '%财付通%'
        AND origin_account_number <> '7440810182200001028'
GROUP BY origin_account_number
