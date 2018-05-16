SELECT * FROM citic_bank.reserve_trade_all_debt
where opponent_account_name like '%财付通%'
and 
	origin_account_number <> '7440810182200001028' 
INTO OUTFILE "/tmp/caifutong转入.csv"
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n';

