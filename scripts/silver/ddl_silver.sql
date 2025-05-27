/*
    This script creates the silver layer tables for the investment data warehouse.
    The silver layer is used for more refined data that has been processed from the silver layer.
*/

USE investment_dw;
GO

IF EXISTS 
    (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'silver.tbl_crm_clients') AND type IN (N'U'))
    BEGIN
        DROP TABLE silver.tbl_crm_clients;
    END

GO

CREATE TABLE silver.tbl_crm_clients
(
    client_key INT,
    client_id NVARCHAR(50),
    client_type NVARCHAR(50),
    first_name NVARCHAR(50),
    last_name NVARCHAR(50),
    date_of_birth DATE,
    email NVARCHAR(100),
    phone NVARCHAR(40),
    address NVARCHAR(200),
    country NVARCHAR(50),
    client_onboarding_date DATETIME2(2),
    risk_rating NVARCHAR(20),
    dwh_load_timestamp DATETIME2(2),
    dwh_source_file NVARCHAR(255),
    dwh_record_status NVARCHAR(50)
)
GO

IF EXISTS 
    (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'silver.tbl_ims_accounts') AND type IN (N'U'))
    BEGIN
        DROP TABLE silver.tbl_ims_accounts;
    END
GO

CREATE TABLE silver.tbl_ims_accounts
(
    account_key INT,
    account_id NVARCHAR(50),
    account_name NVARCHAR(100),
    client_id NVARCHAR(50),
    account_type NVARCHAR(50),
    currency NVARCHAR(10),
    status NVARCHAR(20),
    balance DECIMAL(18, 2),
    opening_date DATETIME2(2),
    dwh_load_timestamp DATETIME2(2),
    dwh_source_file NVARCHAR(255),
    dwh_record_status NVARCHAR(20),
    dwh_flag_suspect_value NVARCHAR(50)
)
GO

IF EXISTS 
    (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'silver.tbl_tp_transactions') AND type IN (N'U'))
    BEGIN
        DROP TABLE silver.tbl_tp_transactions;
    END
GO

CREATE TABLE silver.tbl_tp_transactions
(
    transaction_key INT,
    transaction_id VARCHAR(50),
    account_id NVARCHAR(50),
    transaction_type NVARCHAR(50),
    security_symbol NVARCHAR(20),
    quantity INT,
    price DECIMAL(18, 2),
    amount DECIMAL(18, 2),
    transaction_date DATETIME2(2),
    dwh_load_timestamp DATETIME2(2),
    dwh_source_file NVARCHAR(255),
    dwh_record_status NVARCHAR(20),
    dwh_flag_suspect_value NVARCHAR(50)
)
GO

IF EXISTS 
    (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'silver.tbl_ff_market_data') AND type IN (N'U'))
    BEGIN
        DROP TABLE silver.tbl_ff_market_data;
    END
GO

CREATE TABLE silver.tbl_ff_market_data
(
    market_data_key INT,
    symbol NVARCHAR(20),
    makert_date DATE,
    opening DECIMAL(18,2),
    closing DECIMAL(18,2),
    volume INT,
    high DECIMAL(18,2),
    low DECIMAL(18,2),
    dwh_load_timestamp DATETIME2(2),
    dwh_source_file NVARCHAR(255),
    dwh_record_status NVARCHAR(20)
)
GO
