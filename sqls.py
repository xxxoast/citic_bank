#1. 备付金跨行转账
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
