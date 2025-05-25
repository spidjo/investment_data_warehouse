/*
    This script creates the bronze layer tables in the database.
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
    date_of_birth DATE,
    email NVARCHAR(100),
    phone NVARCHAR(40),
    address NVARCHAR(200),
    country NVARCHAR(50),
    created_at DATE
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
    balance DECIMAL(18, 2),
    created_at DATETIME2(2)
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
    quantity INT,
    price DECIMAL(18, 2),
    amount DECIMAL(18, 2),
    timestamp DATETIME2(2)
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
    makert_date DATE,
    opening DECIMAL(18,2),
    closeing DECIMAL(18,2),
    volume INT,
    high DECIMAL(18,2),
    low DECIMAL(18,2)
)
GO
