use CSVpower

select * from users

select * from retrieveinfo

select * from transactions

CREATE OR ALTER VIEW vw_FraudulentTransactions AS
SELECT
  t.id,
  t.from_card_id,
  t.to_card_id,
  t.amount,
  t.status,
  t.created_at,
  t.transaction_type,
  t.is_flagged,
  c.limit_amount,
  CASE
    WHEN t.is_flagged = 1 OR t.amount > c.limit_amount THEN 1
    ELSE 0
  END AS fraud_suspected
FROM transactions t
JOIN cards c
  ON t.from_card_id = c.id;
GO

SELECT * FROM vw_FraudulentTransactions
WHERE fraud_suspected = 1


CREATE OR ALTER VIEW vw_VIPUsers AS
SELECT
  u.id        AS user_id,
  u.name,
  u.email,
  u.total_balance,
  MAX(t.amount) AS max_transaction_amount,
  CASE
    WHEN MAX(t.amount) > 10000000
      OR u.total_balance > 500000000 THEN 1
    ELSE 0
  END AS is_vip_by_rule
FROM users u
LEFT JOIN transactions t
  ON u.id = t.from_card_id /* or via cards table if needed */
GROUP BY
  u.id,
  u.name,
  u.email,
  u.total_balance;
GO

SELECT * FROM vw_VIPUsers
WHERE is_vip_by_rule = 0


CREATE OR ALTER VIEW vw_blocked_users AS
SELECT 
    id AS user_id,
    name,
    email,
    phone_number,
    total_balance,
    invalid_email,
    invalid_phone
FROM users
WHERE 
    invalid_email = 1
    OR invalid_phone = 1
    OR total_balance <= 0;


SELECT * FROM vw_blocked_users




CREATE OR ALTER VIEW vw_DailyTransactionSummary AS
SELECT
  CAST(t.created_at AS DATE) AS txn_date,
  COUNT(*)                  AS total_transactions,
  SUM(t.amount)             AS total_amount,
  SUM(CASE WHEN t.is_flagged = 1 THEN 1 ELSE 0 END) AS flagged_transactions,
  SUM(CASE WHEN t.amount > c.limit_amount THEN 1 ELSE 0 END)  AS limit_violations
FROM transactions t
JOIN cards c
  ON t.from_card_id = c.id
GROUP BY
  CAST(t.created_at AS DATE)
