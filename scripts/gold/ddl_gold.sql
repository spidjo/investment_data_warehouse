/*
    This script creates the gold layer tables and view in the database.
	It creates business rady views with all the business rules.  Data ready to be used.
    It is safe to run this script multiple times.
    If the tables already exist, they will be dropped and recreated.
*/

USE investment_dw;
GO
-- table tbl_aum_snapshot
-- 
IF EXISTS 
    (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'gold.tbl_aum_snapshot') AND type IN (N'U'))
    BEGIN
        DROP TABLE gold.tbl_aum_snapshot;
    END
GO
CREATE TABLE gold.tbl_aum_snapshot
(
	snapshot_date DATE,
	account_id NVARCHAR(50),
	security_symbol NVARCHAR(20),
	total_investment_value DECIMAL(29, 2) 
)

GO

--- Create Fact and Dimensions Views.

-- v_dim_clients
CREATE OR ALTER VIEW gold.v_dim_clients AS
SELECT 
    client_id,
    client_type,
    first_name,
    last_name,
    date_of_birth,
    email,
    phone,
    address,
    country,
    CAST(client_onboarding_date AS DATE) AS client_onboarding_date,
    risk_rating,
    'CRM' AS source_system,
    dwh_load_timestamp AS created_at
FROM silver.tbl_crm_clients
WHERE dwh_record_status = 'valid';
GO

-- v_dim_accounts
CREATE OR ALTER VIEW gold.v_dim_accounts AS
SELECT 
   account_id,
   client_id,
   account_name,
   account_type,
   currency,
   [status] AS account_status,
   balance,
   CAST(opening_date AS DATE) AS opening_date,
   'Investment Mgmt' AS source_system,
   dwh_load_timestamp AS created_at 
FROM silver.tbl_ims_accounts
WHERE status != 'Closed' 
    AND dwh_record_status  = 'valid';
GO

-- v_dim_market_data
CREATE OR ALTER VIEW gold.v_dim_market_data AS
SELECT 
   symbol,
   market_date,
   opening AS opening_price,
   closing AS closing_price,
   high AS high_price,
   low AS low_price,
   volume,
   'Trading Platform' source_system,
   dwh_load_timestamp AS created_at
FROM silver.tbl_ff_market_data
WHERE dwh_record_status = 'valid';
GO

-- Create gold.v_fact_transactions
CREATE OR ALTER VIEW gold.v_fact_transactions AS 
SELECT 
    t.transaction_id,
    t.account_id,
    a.client_id,
    t.transaction_date,
    t.security_symbol,
    t.transaction_type,
    t.quantity,
    t.price,
    COALESCE(t.quantity,0) * COALESCE(t.price,0) AS transaction_value,
    GETDATE() AS load_date,
    'trading_platform' AS source_system
FROM silver.tbl_tp_transactions t
LEFT JOIN silver.tbl_ims_accounts a
ON t.account_id = a.account_id
WHERE a.status <> 'Closed'
    AND a.dwh_record_status = 'valid'
    AND t.dwh_record_status = 'valid'
GO

-- -- Create v_client_profile
CREATE OR ALTER VIEW gold.v_client_profile AS
WITH number_of_accounts AS
(
    SELECT 
        client_id,
        COUNT(*) AS total_accounts 
    FROM gold.v_dim_accounts
    -- WHERE account_status != 'closed'
    GROUP BY client_id
),
total_inv AS 
(
    SELECT a.client_id,
           SUM(t.transaction_value) AS total_investments
    FROM gold.v_fact_transactions t
    INNER JOIN gold.v_dim_accounts a
    ON t.account_id = a.account_id
    WHERE t.transaction_type = 'Buy'
    GROUP BY a.client_id
),
total_dispose AS 
(
    SELECT a.client_id,
           SUM(t.transaction_value) AS total_disposals
    FROM gold.v_fact_transactions t
    INNER JOIN gold.v_dim_accounts a
    ON t.account_id = a.account_id
    WHERE t.transaction_type = 'Sell'
    GROUP BY a.client_id  
),
total_divid AS 
(
    SELECT a.client_id,
           SUM(t.transaction_value) AS total_dividends
    FROM gold.v_fact_transactions t
    INNER JOIN gold.v_dim_accounts a
    ON t.account_id = a.account_id
    WHERE t.transaction_type = 'Dividend'
    GROUP BY a.client_id 
),
total_fee AS 
(
    SELECT a.client_id,
           SUM(t.transaction_value) AS total_fees
    FROM gold.v_fact_transactions t
    INNER JOIN gold.v_dim_accounts a
    ON t.account_id = a.account_id
    WHERE t.transaction_type = 'Fee'
    GROUP BY a.client_id  
), last_tran_date AS 
(
    SELECT a.client_id,
           CAST(MAX(t.transaction_date) AS DATE) AS last_transaction_date
    FROM gold.v_fact_transactions t
    INNER JOIN gold.v_dim_accounts a
    ON t.account_id = a.account_id
    GROUP BY a.client_id 
)
SELECT 
    c.client_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.address,
    c.client_onboarding_date,
    c.risk_rating,
    COALESCE(ta.total_accounts, 0) AS total_accounts,
    COALESCE(ti.total_investments, 0) AS total_investments,
    COALESCE(td.total_disposals, 0) AS total_disposals,
    COALESCE(tdv.total_dividends, 0) AS total_dividends,
    COALESCE(tf.total_fees, 0) AS total_fees,
    l.last_transaction_date,
     c.created_at ,
     SYSDATETIME() AS updated_at  
-- INTO gold.tbl_client_profile 
FROM gold.v_dim_clients c
LEFT JOIN number_of_accounts ta 
ON c.client_id = ta.client_id
LEFT JOIN total_inv ti
ON c.client_id = ti.client_id
LEFT JOIN total_dispose td
ON c.client_id = td.client_id
LEFT JOIN total_divid tdv
ON c.client_id = tdv.client_id
LEFT JOIN total_fee tf
ON c.client_id = tf.client_id
LEFT JOIN last_tran_date l
ON c.client_id = l.client_id

GO

-- --Create VIEW v_account_summary
-- -- Total_acc_buys 
CREATE OR ALTER VIEW gold.v_account_summary AS
WITH total_acc_buys AS
(
    SELECT 
        a.account_id,
        SUM(t.transaction_value) as total_buys
    FROM gold.v_fact_transactions t
    LEFT JOIN gold.v_dim_accounts a
    ON a.account_id = t.account_id
    WHERE t.transaction_type = 'Buy'
    GROUP BY a.account_id  -- 8516.98

), total_acc_sells AS
(
    SELECT 
        a.account_id,
        SUM(t.transaction_value) as total_sells
    FROM gold.v_fact_transactions t
    LEFT JOIN gold.v_dim_accounts a
    ON a.account_id = t.account_id
    WHERE t.transaction_type = 'Sell'
    GROUP BY a.account_id
), total_acc_fee AS 
(
    SELECT 
        a.account_id,
        SUM(t.transaction_value) as total_fees
    FROM gold.v_fact_transactions t
    LEFT JOIN gold.v_dim_accounts a
    ON a.account_id = t.account_id
    WHERE t.transaction_type = 'Fee'
    GROUP BY a.account_id
), total_acc_dividends AS 
(
    SELECT 
        a.account_id,
        SUM(t.transaction_value) as total_dividends
    FROM gold.v_fact_transactions t
    LEFT JOIN gold.v_dim_accounts a
    ON a.account_id = t.account_id
    WHERE t.transaction_type = 'Dividend'
    GROUP BY a.account_id
),total_trans AS 
(
    SELECT 
        a.account_id,
        COUNT(*) as total_transactions,
        CAST(MAX(t.transaction_date) AS DATE) AS last_transaction_date
    FROM gold.v_fact_transactions t
    LEFT JOIN gold.v_dim_accounts a
    ON a.account_id = t.account_id
    GROUP BY a.account_id
),position AS 
(
    SELECT 
        ft.account_id,
        SUM(CASE 
            WHEN ft.transaction_type = 'Buy' THEN ft.quantity * ft.price
            WHEN ft.transaction_type = 'Sell' THEN -1 * ft.quantity * ft.price
            ELSE 0
        END) AS total_investment_value
    FROM gold.v_fact_transactions ft
    INNER JOIN gold.v_dim_accounts da ON ft.account_id = da.account_id
    GROUP BY ft.account_id
)
SELECT 
    a.account_id,
    a.client_id,
    a.account_type,
    a.currency,
    a.account_status,
    CAST(a.opening_date AS DATE) AS opening_date,
    COALESCE(tb.total_buys,0) AS total_buys,
    COALESCE(ts.total_sells,0) AS total_sells,
    COALESCE(tf.total_fees,0) AS total_fees,
    COALESCE(td.total_dividends,0) AS total_dividends,
    COALESCE(tt.total_transactions,0) AS total_transactions,
    COALESCE(tb.total_buys,0) - COALESCE(ts.total_sells,0) AS net_investment,
    COALESCE(p.total_investment_value, 0) AS total_investment_value,
    tt.last_transaction_date,
    SYSDATETIME() AS created_at ,
    SYSDATETIME() AS updated_at 
FROM gold.v_dim_accounts a
LEFT JOIN total_acc_buys tb
  ON a.account_id = tb.account_id
LEFT JOIN total_acc_sells ts
  ON a.account_id = ts.account_id
LEFT JOIN total_acc_fee tf
  ON a.account_id = tf.account_id
LEFT JOIN total_acc_dividends td
  ON a.account_id = td.account_id
LEFT JOIN total_trans tt
  ON a.account_id = tt.account_id
LEFT JOIN position p
  ON a.account_id = p.account_id;

GO

--- Create v_client_investment_summary
CREATE OR ALTER VIEW gold.v_client_investment_summary AS
SELECT 
    cp.client_id,
    cp.first_name,
    cp.last_name,
    cp.risk_rating,
    COUNT(DISTINCT a.account_id) AS total_accounts,
    SUM(CASE WHEN t.transaction_type = 'Buy' THEN t.transaction_value ELSE 0 END) AS total_investment,
    SUM(CASE WHEN t.transaction_type = 'Sell' THEN t.transaction_value ELSE 0 END) AS total_withdrawal,
    SUM(t.transaction_value) AS net_cash_flow
FROM 
    gold.v_client_profile cp
LEFT JOIN 
    gold.v_account_summary a ON cp.client_id = a.client_id
LEFT JOIN v_fact_transactions t ON a.account_id = t.account_id

GROUP BY 
    cp.client_id, cp.first_name,cp.last_name, cp.risk_rating;

GO

CREATE OR ALTER VIEW gold.v_account_performance AS
SELECT 
    t.account_id,
    t.security_symbol,
    SUM(CASE WHEN t.transaction_type = 'Buy' THEN t.quantity ELSE 0 END) AS total_bought,
    SUM(CASE WHEN t.transaction_type = 'Sell' THEN t.quantity ELSE 0 END) AS total_sold,
    MAX(m.closing_price) AS latest_price,
    SUM(t.quantity * t.price) AS invested_amount
FROM 
    gold.v_fact_transactions t
LEFT JOIN 
    gold.v_dim_market_data m ON t.security_symbol = m.symbol AND t.transaction_date = m.market_date
GROUP BY 
    t.account_id, t.security_symbol;

GO

CREATE OR ALTER VIEW gold.v_security_volume_trends AS
SELECT 
    symbol,
    AVG(volume) AS avg_daily_volume,
    MAX(volume) AS peak_volume,
    MIN(volume) AS lowest_volume,
    COUNT(*) AS trading_days
FROM 
    gold.v_security_prices
GROUP BY 
    symbol;

GO

CREATE OR ALTER VIEW gold.v_daily_aum_snapshot AS
SELECT 
    snapshot_date,
    SUM(total_investment_value) AS total_aum
FROM 
    gold.tbl_aum_snapshot
GROUP BY 
    snapshot_date;

