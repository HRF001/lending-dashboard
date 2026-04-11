-- 总行数
SELECT COUNT(*) AS total_rows
FROM clean_lending_activity;

-- 各字段填充情况
SELECT
    COUNT(*) AS total,
    COUNT(matter_no) AS matter,
    COUNT(broker) AS broker,
    COUNT(lender) AS lender,
    COUNT(suburb_state) AS suburb,
    COUNT(principal_amount) AS principal,
    COUNT(settlement_date) AS settlement
FROM clean_lending_activity;

-- 检查重复记录
SELECT 
    source_file,
    matter_no,
    settlement_date,
    principal_amount,
    COUNT(*) as cnt
FROM clean_lending_activity
GROUP BY source_file, matter_no, settlement_date, principal_amount
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

-- 最近导入记录
SELECT *
FROM import_log
ORDER BY id DESC;

-- 总放款金额
SELECT SUM(principal_amount) AS total_principal
FROM clean_lending_activity;

-- 平均每笔金额
SELECT AVG(principal_amount) AS avg_principal
FROM clean_lending_activity;

-- 最大贷款
SELECT MAX(principal_amount) AS max_loan
FROM clean_lending_activity;

-- 最小贷款
SELECT MIN(principal_amount) AS min_loan
FROM clean_lending_activity;

-- broker 排名
SELECT 
    broker,
    COUNT(*) AS deal_count,
    SUM(principal_amount) AS total_principal
FROM clean_lending_activity
GROUP BY broker
ORDER BY total_principal DESC
LIMIT 10;

-- lender 排名
SELECT 
    lender,
    COUNT(*) AS deal_count,
    SUM(principal_amount) AS total_principal
FROM clean_lending_activity
GROUP BY lender
ORDER BY total_principal DESC
LIMIT 10;

-- suburb 热度
SELECT 
    suburb_state,
    COUNT(*) AS deal_count,
    SUM(principal_amount) AS total_principal
FROM clean_lending_activity
GROUP BY suburb_state
ORDER BY total_principal DESC
LIMIT 20;

-- 按日期趋势
SELECT 
    settlement_date,
    COUNT(*) AS deal_count,
    SUM(principal_amount) AS total_principal
FROM clean_lending_activity
GROUP BY settlement_date
ORDER BY settlement_date;

-- 有 shortfall 的记录
SELECT *
FROM clean_lending_activity
WHERE shortfall_amount IS NOT NULL;

-- shortfall 总金额
SELECT SUM(shortfall_amount)
FROM clean_lending_activity;

-- 随机看20条
SELECT *
FROM clean_lending_activity
LIMIT 20;