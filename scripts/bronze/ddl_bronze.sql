/*
    This script creates the bronze layer tables in the database.
    It is safe to run this script multiple times.
    If the tables already exist, they will be dropped and recreated.
*/

USE investment_dw;
GO

IF EXISTS 
    (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'bronze.tbl_crm_clients') AND type IN (N'U'))
    BEGIN
        DROP TABLE bronze.tbl_crm_clients;
    END

GO

CREATE TABLE bronze.tbl_crm_clients
(
    client_id NVARCHAR(50),
    client_type NVARCHAR(50),
    first_name NVARCHAR(50),
    last_name NVARCHAR(50),
    date_of_birth NVARCHAR(50),
    email NVARCHAR(100),
    phone NVARCHAR(40),
    address NVARCHAR(200),
    country NVARCHAR(50),
    client_onboarding_date NVARCHAR(50),
    risk_rating NVARCHAR(50)
)
GO

IF EXISTS 
    (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'bronze.tbl_ims_accounts') AND type IN (N'U'))
    BEGIN
        DROP TABLE bronze.tbl_ims_accounts;
    END
GO

CREATE TABLE bronze.tbl_ims_accounts
(
    account_id NVARCHAR(50),
    account_name NVARCHAR(100),
    client_id NVARCHAR(50),
    account_type NVARCHAR(50),
    currency NVARCHAR(10),
    status NVARCHAR(20),
    balance NVARCHAR(50),
    opening_date NVARCHAR(50)
)
GO

IF EXISTS 
    (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'bronze.tbl_tp_transactions') AND type IN (N'U'))
    BEGIN
        DROP TABLE bronze.tbl_tp_transactions;
    END
GO

CREATE TABLE bronze.tbl_tp_transactions
(
    transaction_id VARCHAR(50),
    account_id NVARCHAR(50),
    transaction_type NVARCHAR(50),
    security_symbol NVARCHAR(20),
    quantity NVARCHAR(50),
    price NVARCHAR(50),
    amount NVARCHAR(50),
    transaction_date NVARCHAR(50)
)
GO

IF EXISTS 
    (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'bronze.tbl_ff_market_data') AND type IN (N'U'))
    BEGIN
        DROP TABLE bronze.tbl_ff_market_data;
    END
GO

CREATE TABLE bronze.tbl_ff_market_data
(
    symbol NVARCHAR(20),
    market_date NVARCHAR(50),
    opening NVARCHAR(50),
    closing NVARCHAR(50),
    volume NVARCHAR(50),
    high NVARCHAR(50),
    low NVARCHAR(50)
)
GO
