--- Check the data quality in the silver schema tables
--     PRINT '-------------------------------------------------';
--     PRINT 'Checking data quality in silver schema tables...';    
--     PRINT '-------------------------------------------------';
USE investment_dw;
-- Check the number of records in each table
PRINT '-------------------------------------------------';
PRINT 'Checking record counts in silver schema tables...';
PRINT '-------------------------------------------------';
DECLARE @table_name NVARCHAR(128),
        @schema_name NVARCHAR(128),
        @sql NVARCHAR(MAX);
SET @schema_name = 'silver';
DECLARE table_cursor CURSOR FOR
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = @schema_name;
OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @table_name;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'SELECT ''' + @table_name + ''' AS TableName, COUNT(*) AS RecordCount FROM ' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@table_name) + ';';
    PRINT @schema_name + '.' + @table_name;
    PRINT '-------------------------------------------------';
    PRINT @sql;
    EXEC sp_executesql @sql;

    FETCH NEXT FROM table_cursor INTO @table_name;
END;
CLOSE table_cursor;
DEALLOCATE table_cursor;    

-- Check the data quality in the silver schema tables
-- Duplicate client_id check
PRINT '-------------------------------------------------';
PRINT 'Checking for duplicate client_id values in tbl_crm_clients...';
PRINT '-------------------------------------------------';
SELECT 
    COUNT(*) AS C,
    client_id
FROM silver.tbl_crm_clients
GROUP BY client_id
HAVING COUNT(*) > 1;

SELECT *
FROM silver.tbl_crm_clients
WHERE dwh_record_status != 'valid';
-- Check for null or empty values in key columns
PRINT '-------------------------------------------------';
PRINT 'Checking for null or empty values in key columns of tbl_crm_clients...';
PRINT '-------------------------------------------------';

SELECT 
    *
FROM silver.tbl_crm_clients
WHERE   first_name != TRIM(first_name)
    OR last_name != TRIM(last_name)
    OR phone IS NULL
    OR phone = ''
    OR address IS NULL
    OR address = ''
    OR country IS NULL
    OR country = ''


-- Check for valid email formats
PRINT '-------------------------------------------------';
PRINT 'Checking for valid email formats in tbl_crm_clients...';
PRINT '-------------------------------------------------';
SELECT 
    *
FROM silver.tbl_crm_clients
WHERE email NOT LIKE '%_@__%.__%'
AND email != 'n/a';

-- Check for valid date formats in date_of_birth
PRINT '-------------------------------------------------';
PRINT 'Checking for valid date formats in date_of_birth in tbl_crm_clients...';
PRINT '-------------------------------------------------';
SELECT 
    *
FROM silver.tbl_crm_clients
WHERE date_of_birth IS NOT NULL
AND TRY_CAST(date_of_birth AS DATE) IS NULL;

-- Check client_type for valid values
PRINT '-------------------------------------------------';
PRINT 'Checking client_type for valid values in tbl_crm_clients...';
PRINT '-------------------------------------------------';
SELECT *
FROM silver.tbl_crm_clients
WHERE client_type NOT IN ('Institutional', 'Individual');



-- General data quality check
PRINT '-------------------------------------------------';  
PRINT 'Checking for general data quality issues in tbl_crm_clients...';
PRINT '-------------------------------------------------';
SELECT * FROM silver.tbl_crm_clients;

-- Check for duplicate account_id values in tbl_ims_accounts
PRINT '-------------------------------------------------';
PRINT 'Checking for duplicate account_id values in tbl_ims_accounts...';
PRINT '-------------------------------------------------';
SELECT 
    COUNT(*) AS C,
    account_id
FROM silver.tbl_ims_accounts
GROUP BY account_id
HAVING COUNT(*) > 1;


SELECT *
FROM silver.tbl_ims_accounts
WHERE dwh_record_status = 'duplicate';
-- Check for null or empty values in key columns of tbl_ims_accounts
PRINT '-------------------------------------------------';
PRINT 'Checking for null or empty values in key columns of tbl_ims_accounts...';
PRINT '-------------------------------------------------';
SELECT 
    *
FROM silver.tbl_ims_accounts
WHERE account_name IS NULL OR account_name = ''
    OR currency IS NULL OR currency = ''
    OR status IS NULL OR status = ''
    OR balance IS NULL OR TRY_CAST(balance AS FLOAT) IS NULL
    OR opening_date IS NULL OR opening_date = '';   


-- Check for valid account_type values
PRINT '-------------------------------------------------';
PRINT 'Checking for valid account_type values in tbl_ims_accounts...';
PRINT '-------------------------------------------------';
SELECT 
    *
FROM silver.tbl_ims_accounts
WHERE account_type NOT IN ('Savings', 'Retirement', 'Brokerage');

-- Check for valid currency codes
PRINT '-------------------------------------------------';
PRINT 'Checking for valid currency codes in tbl_ims_accounts...';
PRINT '-------------------------------------------------';
SELECT 
    *
FROM silver.tbl_ims_accounts
WHERE currency NOT IN ('USD', 'EUR', 'ZAR');

SELECT DISTINCT currency
FROM silver.tbl_ims_accounts;

-- Check for valid status values
PRINT '-------------------------------------------------';
PRINT 'Checking for valid status values in tbl_ims_accounts...';
PRINT '-------------------------------------------------';
SELECT 
    *
FROM silver.tbl_ims_accounts
WHERE status NOT IN ('Active', 'Inactive', 'Closed');

-- General data quality check for tbl_ims_accounts
PRINT '-------------------------------------------------';
PRINT 'Checking for general data quality issues in tbl_ims_accounts...';
PRINT '-------------------------------------------------';
SELECT * FROM silver.tbl_ims_accounts;


-- Check for duplicate transaction_id values in tbl_tp_transactions
PRINT '-------------------------------------------------';  
PRINT 'Checking for duplicate transaction_id values in tbl_tp_transactions...';
PRINT '-------------------------------------------------';
SELECT 
    COUNT(*) AS C,
    transaction_id
FROM silver.tbl_tp_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;

SELECT *
FROM silver.tbl_tp_transactions
WHERE dwh_record_status = 'duplicate';

-- Check for null or empty values in key columns of tbl_tp_transactions
PRINT '-------------------------------------------------';  
PRINT 'Checking for null or empty values in key columns of tbl_tp_transactions...';
SELECT 
    *
FROM silver.tbl_tp_transactions
WHERE transaction_id IS NULL OR transaction_id = ''
    OR account_id IS NULL OR account_id = ''
    OR transaction_type IS NULL OR transaction_type = ''
    OR security_symbol IS NULL OR security_symbol = ''
    OR quantity IS NULL OR TRY_CAST(quantity AS FLOAT) IS NULL
    OR price IS NULL OR TRY_CAST(price AS FLOAT) IS NULL
    OR amount IS NULL OR TRY_CAST(amount AS FLOAT) IS NULL
    OR transaction_date IS NULL OR transaction_date = '';


-- Check for valid transaction_type values
PRINT '-------------------------------------------------';
PRINT 'Checking for valid transaction_type values in tbl_tp_transactions...';
PRINT '-------------------------------------------------';
SELECT *
FROM silver.tbl_tp_transactions
WHERE transaction_type NOT IN ('Deposit', 'Withdrawal', 'Transfer');

SELECT DISTINCT transaction_type
FROM silver.tbl_tp_transactions;

-- Check for valid security_symbol values
PRINT '-------------------------------------------------';
PRINT 'Checking for valid security_symbol values in tbl_tp_transactions...';
PRINT '-------------------------------------------------';
SELECT *
FROM silver.tbl_tp_transactions
WHERE security_symbol NOT LIKE '[A-Z]%'
    OR security_symbol = '';

-- Check for valid quantity, price, and amount values
PRINT '-------------------------------------------------';
PRINT 'Checking for valid quantity, price, and amount values in tbl_tp_transactions...';
SELECT *
FROM silver.tbl_tp_transactions
WHERE TRY_CAST(quantity AS FLOAT) IS NULL OR CAST(quantity AS FLOAT) = 0
    OR TRY_CAST(price AS FLOAT) IS NULL OR CAST(price AS FLOAT) = 0
    OR TRY_CAST(amount AS FLOAT) IS NULL OR CAST(amount AS FLOAT) = 0;

-- Check for valid transaction_date values
PRINT '-------------------------------------------------';
PRINT 'Checking for valid transaction_date values in tbl_tp_transactions...';
SELECT *
FROM silver.tbl_tp_transactions
WHERE TRY_CAST(transaction_date AS DATE) IS NULL
    OR transaction_date < '1900-01-01'
    OR transaction_date > GETDATE();

-- General data quality check for tbl_tp_transactions
PRINT '-------------------------------------------------';
PRINT 'Checking for general data quality issues in tbl_tp_transactions...';
SELECT * FROM silver.tbl_tp_transactions;


-- Check for duplicate market_data_key values in tbl_ff_market_data
PRINT '-------------------------------------------------';
PRINT 'Checking for duplicate market_data_key values in tbl_ff_market_data...';
PRINT '-------------------------------------------------';
SELECT 
    COUNT(*) AS C,
    market_data_key
FROM silver.tbl_ff_market_data
GROUP BY market_data_key
HAVING COUNT(*) > 1;

SELECT 
    market_date,
    symbol,
    COUNT(*) AS C
FROM silver.tbl_ff_market_data
GROUP BY market_date, symbol
HAVING COUNT(*) > 1;

-- Check for null or empty values in key columns of tbl_ff_market_data
PRINT '-------------------------------------------------';
PRINT 'Checking for null or empty values in key columns of tbl_ff_market_data...';
SELECT 
    *
FROM silver.tbl_ff_market_data
WHERE market_data_key IS NULL OR market_data_key = 0
    OR symbol IS NULL OR symbol = ''
    OR market_date IS NULL OR market_date = ''
    OR opening IS NULL OR TRY_CAST(opening AS FLOAT) IS NULL
    OR closing IS NULL OR TRY_CAST(closing AS FLOAT) IS NULL
    OR volume IS NULL OR TRY_CAST(volume AS FLOAT) IS NULL
    OR high IS NULL OR TRY_CAST(high AS FLOAT) IS NULL
    OR low IS NULL OR TRY_CAST(low AS FLOAT) IS NULL;


-- Check SYMBOL for valid values
PRINT '-------------------------------------------------';
PRINT 'Checking for valid symbol values in tbl_ff_market_data...';
PRINT '-------------------------------------------------';
SELECT *
FROM silver.tbl_ff_market_data
WHERE symbol NOT LIKE '[A-Z]%'
    OR symbol = '';

SELECT DISTINCT symbol
FROM silver.tbl_ff_market_data;

-- Check for valid market_date values
PRINT '-------------------------------------------------';
PRINT 'Checking for valid market_date values in tbl_ff_market_data...';
SELECT *
FROM silver.tbl_ff_market_data
WHERE TRY_CAST(market_date AS DATE) IS NULL
    OR market_date < '1900-01-01'
    OR market_date > GETDATE();

-- Check for valid opening, closing, volume, high, and low values
PRINT '-------------------------------------------------';
PRINT 'Checking for valid opening, closing, volume, high, and low values in tbl_ff_market_data...';
SELECT *
FROM silver.tbl_ff_market_data
WHERE TRY_CAST(opening AS FLOAT) IS NULL OR CAST(opening AS FLOAT) = 0
    OR TRY_CAST(closing AS FLOAT) IS NULL OR CAST(closing AS FLOAT) = 0
    OR TRY_CAST(volume AS FLOAT) IS NULL OR CAST(volume AS FLOAT) = 0
    OR TRY_CAST(high AS FLOAT) IS NULL OR CAST(high AS FLOAT) = 0
    OR TRY_CAST(low AS FLOAT) IS NULL OR CAST(low AS FLOAT) = 0;

-- General data quality check for tbl_ff_market_data
PRINT '-------------------------------------------------';
PRINT 'Checking for general data quality issues in tbl_ff_market_data...';
SELECT * FROM silver.tbl_ff_market_data;    
-- Final message
PRINT '-------------------------------------------------';  
PRINT 'Data quality checks completed successfully in silver schema tables.';
PRINT '-------------------------------------------------';