
USE investment_dw;
/*This script loads data into the silver layer of the investment data warehouse.
   It processes data from the bronze layer, applying transformations and validations.
   The script handles clients, accounts, transactions, and market data.
*/

--- Let's start by Truncating the silver layer tables
PRINT 'Truncating silver layer tables...';
TRUNCATE TABLE silver.tbl_crm_clients;
PRINT 'Table silver.tbl_crm_clients truncated successfully.';
TRUNCATE TABLE silver.tbl_ims_accounts; 
PRINT 'Table silver.tbl_ims_accounts truncated successfully.';
TRUNCATE TABLE silver.tbl_tp_transactions;
PRINT 'Table silver.tbl_tp_transactions truncated successfully.';
TRUNCATE TABLE silver.tbl_ff_market_data;
PRINT 'Table silver.tbl_ff_market_data truncated successfully.';
PRINT 'Silver layer tables truncated successfully.';

--- Load data into silver.tbl_crm_clients
PRINT 'Loading data into silver.tbl_crm_clients...';
-- This script processes client data, ensuring proper formatting and validation.

DECLARE @start_time DATETIME = GETDATE();
DECLARE @batch_start_time DATETIME = GETDATE();
PRINT 'Loading tbl_crm_clients started at: ' + CONVERT(VARCHAR, @start_time, 120);

INSERT INTO silver.tbl_crm_clients (
      client_key,
      client_id,
      first_name,
      last_name,
      client_type,
      date_of_birth,
      email,
      phone,
      address,
      country,
      client_onboarding_date,
      risk_rating,
      dwh_load_timestamp,
      dwh_source_file,
      dwh_record_status
)
SELECT 
    100000 + ROW_NUMBER() OVER(ORDER BY client_id) AS client_key,
    client_id, 
    TRIM(
        REPLACE (
            REPLACE (
                REPLACE(
                    REPLACE(TRIM(first_name),'@', '')
                 ,'#','')
            ,'!','')
        ,'$','')
      )  AS first_name,
    TRIM(
        REPLACE (
            REPLACE (
                REPLACE(
                    REPLACE(TRIM(last_name),'@', '')
                 ,'#','')
            ,'!','')
        ,'$','')
      )  AS last_name,
    client_type, 
    CASE 
        WHEN ISDATE(date_of_birth) = 1
            THEN date_of_birth
        ELSE NULL
    END AS date_of_birth, 
    CASE 
        WHEN email NOT LIKE '%_@__%.__%'
            THEN 'unknown' 
        WHEN email IS NULL 
            THEN 'unknown'
        ELSE TRIM(email)
    END AS email, 
    COALESCE(phone, 'unknown') AS phone, 
    COALESCE (address, 'unknown') AS address, 
    CASE 
        WHEN country IS NULL THEN 'unknown'
        WHEN country = 'RSA' THEN 'South Africa'
        WHEN country = 'ZA' THEN 'South Africa'
        WHEN country = 'ZAF' THEN 'South Africa'
        WHEN country = 'BRA' THEN 'Brazil'
        WHEN country = 'CAN' THEN 'Canada'
        WHEN country = 'CHN' THEN 'China'
        WHEN country = 'CN' THEN 'China'
        WHEN country = 'JPN' THEN 'Japan'
        WHEN country = 'AUS' THEN 'Australia'
        WHEN country = 'US' THEN 'United States'
        WHEN country = 'USA' THEN 'United States'
        WHEN country = 'DEU' THEN 'Germany'
        WHEN country = 'UK' THEN 'United Kingdom'
        WHEN country = 'GBR' THEN 'United Kingdom'
        WHEN country = 'GB' THEN 'United Kingdom'
        WHEN country = 'ESP' THEN 'Spain'
        WHEN country = 'ITA' THEN 'Italy'
        WHEN country = 'FRA' THEN 'France'
        WHEN country = 'IN' THEN 'India'
        WHEN country = 'IND' THEN 'India'
        ELSE country
   END AS country, 
    client_onboarding_date,
    COALESCE(risk_rating, 'Medium') AS risk_rating,
    GETDATE() AS dwh_load_timestamp,
    'clients.csv' AS dwh_source_file,
    CASE 
        WHEN rn > 1
            THEN 'duplicate'
        WHEN first_name IS NULL OR first_name = ''
            THEN 'invalid'
        WHEN last_name IS NULL OR last_name = ''
            THEN 'invalid'
        WHEN email IS NULL OR email = ''
            THEN 'invalid'
        WHEN phone IS NULL OR phone = ''
            THEN 'invalid'
        WHEN client_onboarding_date IS NULL OR client_onboarding_date = ''
            THEN 'flagged'
        WHEN CAST(client_onboarding_date AS DATE) > GETDATE()
            THEN 'flagged'
        ELSE 'valid'
    END AS dwh_record_status
FROM (
        SELECT 
                ROW_NUMBER() OVER(PARTITION BY client_id ORDER BY client_id, client_onboarding_date DESC) rn,
                * 
        FROM bronze.tbl_crm_clients
) t 

PRINT 'tbl_crm_clients data loaded successfully.';
DECLARE @end_time DATETIME = GETDATE(); 
PRINT 'Loading tbl_crm_clients completed at: ' + CONVERT(VARCHAR, @end_time, 120);
PRINT 'Total time taken to load tbl_crm_clients: ' + CONVERT(VARCHAR, DATEDIFF(SECOND, @start_time, @end_time)) + ' seconds.';

    
--- Load data into silver.tbl_ims_accounts
PRINT '=========================';
PRINT 'Loading data into silver.tbl_ims_accounts...';
SET @start_time = GETDATE();
PRINT 'Loading tbl_ims_accounts started at: ' + CONVERT(VARCHAR, @start_time, 120);
INSERT INTO silver.tbl_ims_accounts (
    account_key,
    account_id,
    client_id,
    account_type,
    account_name,
    currency,
    status,
    balance,
    opening_date,
    dwh_load_timestamp,
    dwh_source_file,
    dwh_record_status,
    dwh_flag_suspect_value
)
SELECT 
    100000 + ROW_NUMBER() OVER(ORDER BY account_id) AS account_key,
    account_id, 
    client_id, 
    account_type, 
    COALESCE(TRIM(account_name), 'unknown') AS account_name,
    COALESCE(currency, 'ZAR') AS currency,
    CASE SUBSTRING([status],1,2)
        WHEN 'Ac' THEN 'Active'
        WHEN 'In' THEN 'Inactive'
        WHEN 'Cl' THEN 'Closed'
    END AS status,
    CASE 
        WHEN TRY_CAST(balance AS FLOAT) IS NOT NULL
            THEN CAST(balance AS FLOAT)
        ELSE 0
    END AS balance, 
    opening_date,
    GETDATE() AS dwh_load_timestamp,
    'accounts.csv' AS dwh_source_file,
    CASE 
        WHEN rn > 1
            THEN 'duplicate'    
        WHEN account_type IS NULL OR account_type = ''
            THEN 'flagged'
        WHEN currency IS NULL OR currency = ''
            THEN 'flagged'
        WHEN CAST(opening_date AS DATE) > GETDATE()
            THEN 'flagged'
        WHEN CAST(opening_date AS DATE) < (SELECT client_onboarding_date FROM silver.tbl_crm_clients 
                                            WHERE client_id = t.client_id AND dwh_record_status = 'valid')   
            THEN 'flagged'
        WHEN TRY_CAST(balance AS FLOAT) IS NULL
            OR CAST(balance AS FLOAT) = 0
            THEN 'flagged'
        ELSE 'valid'
    END AS dwh_record_status,
    CASE 
        WHEN TRY_CAST(balance AS FLOAT) IS NULL 
            OR CAST(balance AS FLOAT) = 0
        THEN 'True' 
        ELSE 'False'
    END AS dwh_flag_suspect_value
FROM (
        SELECT 
            ROW_NUMBER() OVER(PARTITION BY account_id ORDER BY account_id) rn,
            * 
        FROM bronze.tbl_ims_accounts
) t


PRINT 'tbl_ims_accounts data loaded successfully.';
SET @end_time = GETDATE();
PRINT 'Loading tbl_ims_accounts completed at: ' + CONVERT(VARCHAR, @end_time, 120);
PRINT 'Total time taken to load tbl_ims_accounts: ' + CONVERT(VARCHAR, DATEDIFF(SECOND, @start_time, @end_time)) + ' seconds.';

--- Load data into silver.tbl_tp_transactions
PRINT '=========================';
PRINT 'Loading data into silver.tbl_tp_transactions...';
SET @start_time = GETDATE();
PRINT 'Loading tbl_tp_transactions started at: ' + CONVERT(VARCHAR, @start_time, 120);

-- --- Load data into silver.tbl_tp_transactions
INSERT INTO silver.tbl_tp_transactions (
    transaction_key,
    transaction_id,
    account_id,
    transaction_type,
    security_symbol,
    quantity,
    price,
    amount,
    transaction_date,
    dwh_load_timestamp,
    dwh_source_file,
    dwh_record_status,
    dwh_flag_suspect_value
)
SELECT 
    100000 + ROW_NUMBER() OVER(ORDER BY transaction_id) AS transaction_key,
    transaction_id, 
    account_id, 
    CASE SUBSTRING(transaction_type, 1, 2)
        WHEN 'Tr' THEN 'Transfer'
        WHEN 'De' THEN 'Deposit'
        WHEN 'Wi' THEN 'Withdrawal'
    END AS transaction_type,
    CASE 
        WHEN security_symbol IS NULL OR security_symbol = ''
            THEN 'unknown'
        ELSE TRIM(security_symbol)
    END AS security_symbol,
    CASE 
        WHEN TRY_CAST(quantity AS FLOAT) IS NOT NULL
            THEN CAST(quantity AS FLOAT)
        ELSE 0
    END AS quantity,
    CASE 
        WHEN TRY_CAST(price AS FLOAT) IS NOT NULL
            THEN CAST(price AS FLOAT)
        ELSE 0
    END AS price,
    CASE
        WHEN TRY_CAST(amount AS FLOAT) IS NOT NULL
            THEN CAST(amount AS FLOAT)
        ELSE 0
    END AS amount,
    CASE 
        WHEN TRY_CAST(transaction_date AS DATE) IS NOT NULL
            THEN CAST(transaction_date AS DATE)
        ELSE NULL
    END AS transaction_date,
    GETDATE() AS dwh_load_timestamp,
    'transactions.csv' AS dwh_source_file,
    CASE 
        WHEN rn > 1
            THEN 'duplicate'
        WHEN TRY_CAST(transaction_date AS DATE) IS NOT NULL
              AND CAST(transaction_date AS DATE) 
              NOT BETWEEN (SELECT opening_date  FROM silver.tbl_ims_accounts 
                        WHERE account_id = t.account_id AND dwh_record_status != 'duplicate')
              AND (SELECT GETDATE())
            THEN 'invalid'
        WHEN TRY_CAST(transaction_date AS DATE) IS NULL
            THEN 'invalid'
        WHEN transaction_type IS NULL OR transaction_type = ''
            THEN 'invalid'
        WHEN security_symbol IS NULL OR security_symbol = ''
            THEN 'invalid'
        WHEN TRY_CAST(quantity AS FLOAT) IS NULL
            OR CAST(quantity AS FLOAT) = 0
            OR TRY_CAST(price AS FLOAT) IS NULL
            OR CAST(price AS FLOAT) = 0
            OR TRY_CAST(amount AS FLOAT) IS NULL
            OR CAST(amount AS FLOAT) = 0
            THEN 'flagged'
        ELSE 'valid'
    END AS dwh_record_status,
    CASE 
        WHEN TRY_CAST(quantity AS FLOAT) IS NULL
            OR CAST(quantity AS FLOAT) = 0
            OR TRY_CAST(price AS FLOAT) IS NULL
            OR CAST(price AS FLOAT) = 0
            OR TRY_CAST(amount AS FLOAT) IS NULL
            OR CAST(amount AS FLOAT) = 0
        THEN 'True' 
    END AS dwh_flag_suspect_value

FROM (
        SELECT 
            ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY transaction_id) rn,
            * 
        FROM bronze.tbl_tp_transactions
        
) t
WHERE rn = 1;

PRINT 'tbl_tp_transactions data loaded successfully.';
SET @end_time = GETDATE();
PRINT 'Loading tbl_tp_transactions completed at: ' + CONVERT(VARCHAR, @end_time, 120);
PRINT 'Total time taken to load tbl_tp_transactions: ' + CONVERT(VARCHAR, DATEDIFF(SECOND, @start_time, @end_time)) + ' seconds.';

--- load data into silver.tbl_ff_market_data

PRINT '=========================';
PRINT 'Loading data into silver.tbl_ff_market_data...';
SET @start_time = GETDATE();
PRINT 'Loading tbl_ff_market_data started at: ' + CONVERT(VARCHAR, @start_time, 120);

WITH fixed_symbols AS (
    SELECT DISTINCT
    REPLACE (
        REPLACE (
            REPLACE (
                REPLACE(symbol,'@', '')
             ,'#','')
        ,'!','')
    ,'$','') AS symbol, 
    CAST(market_date AS DATE) AS market_date, 
    opening, 
    high, 
    low, 
    closing, 
    CASE 
        WHEN TRY_CAST(volume AS FLOAT) IS NULL 
            THEN LAG(volume) OVER (PARTITION BY symbol ORDER BY market_date)
        ELSE CAST(volume AS FLOAT)
    END AS volume
    FROM bronze.tbl_ff_market_data
    WHERE TRY_CAST(market_date AS DATE) IS NOT NULL
), date_range AS (
    SELECT 
        MIN(market_date) AS start_date, 
        MAX(market_date) AS end_date
    FROM fixed_symbols
), symbols AS (
    SELECT DISTINCT symbol
    FROM fixed_symbols
), date_series AS (
    SELECT 
        DATEADD(DAY, n, (SELECT start_date FROM date_range)) AS market_date
    FROM (
        SELECT TOP (DATEDIFF(DAY, (SELECT start_date FROM date_range), (SELECT end_date FROM date_range)) + 1)
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
        FROM master.dbo.spt_values
    ) AS numbers
), market_data AS (
    SELECT 
        ds.market_date,
        s.symbol,
        CASE 
            WHEN CAST(f.opening AS FLOAT) = 0 
             OR f.opening IS NULL 
            THEN 
                COALESCE(LAG(f.opening) OVER (PARTITION BY s.symbol ORDER BY ds.market_date), 
                LAG(f.opening,2) OVER (PARTITION BY s.symbol ORDER BY ds.market_date),
                LAG(f.opening,3) OVER (PARTITION BY s.symbol ORDER BY ds.market_date))
            ELSE f.opening
        END AS opening,
        CASE 
            WHEN CAST(f.high AS FLOAT) = 0 
             OR f.high IS NULL 
            THEN 
                COALESCE(LAG(f.high) OVER (PARTITION BY s.symbol ORDER BY ds.market_date), 
                LAG(f.high,2) OVER (PARTITION BY s.symbol ORDER BY ds.market_date),
                LAG(f.high,3) OVER (PARTITION BY s.symbol ORDER BY ds.market_date))
            ELSE f.high
        END AS high,
        CASE 
            WHEN CAST(f.low AS FLOAT) = 0 
             OR f.low IS NULL 
            THEN 
                COALESCE(LAG(f.low) OVER (PARTITION BY s.symbol ORDER BY ds.market_date), 
                LAG(f.low,2) OVER (PARTITION BY s.symbol ORDER BY ds.market_date),
                LAG(f.low,3) OVER (PARTITION BY s.symbol ORDER BY ds.market_date))
            ELSE f.low
        END AS low,
        CASE 
            WHEN CAST(f.closing AS FLOAT) = 0 
             OR f.closing IS NULL 
            THEN 
                COALESCE(LAG(f.closing) OVER (PARTITION BY s.symbol ORDER BY ds.market_date), 
                LAG(f.closing,2) OVER (PARTITION BY s.symbol ORDER BY ds.market_date),
                LAG(f.closing,3) OVER (PARTITION BY s.symbol ORDER BY ds.market_date))
            ELSE f.closing
        END AS closing,
        CASE 
            WHEN CAST(f.volume AS FLOAT) = 0 
             OR f.volume IS NULL 
            THEN 
                COALESCE(LAG(f.volume) OVER (PARTITION BY s.symbol ORDER BY ds.market_date), 
                LAG(f.volume,2) OVER (PARTITION BY s.symbol ORDER BY ds.market_date),
                LAG(f.volume,3) OVER (PARTITION BY s.symbol ORDER BY ds.market_date))
            ELSE f.volume
        END AS volume
    FROM date_series ds
    CROSS JOIN symbols s
    LEFT JOIN fixed_symbols f ON ds.market_date = f.market_date AND s.symbol = f.symbol
)
INSERT INTO silver.tbl_ff_market_data (
    market_data_key,
    symbol,
    market_date,
    opening,
    closing,
    volume,
    high,
    low,
    dwh_load_timestamp,
    dwh_source_file,
    dwh_record_status
)

SELECT 
    10000 + ROW_NUMBER() OVER (ORDER BY market_date, symbol) AS market_data_key,
    symbol,
    market_date,
    opening,
    closing,
    volume,
    high,
    low,
    GETDATE() AS dwh_load_timestamp,
    'market_data.csv' AS dwh_source_file,
    'valid' AS dwh_record_status
FROM market_data
ORDER BY market_date, symbol;
PRINT 'tbl_ff_market_data data loaded successfully.';
SET @end_time = GETDATE();

PRINT 'Loading tbl_ff_market_data completed at: ' + CONVERT(VARCHAR, @end_time, 120);
PRINT 'Total time taken to load tbl_ff_market_data: ' + CONVERT(VARCHAR, DATEDIFF(SECOND, @start_time, @end_time)) + ' seconds.';
PRINT '=========================';
PRINT 'All silver layer data loaded successfully.';

PRINT 'Batch processing completed in: ' + CONVERT(VARCHAR, DATEDIFF(SECOND, @batch_start_time, GETDATE())) + ' seconds.';

