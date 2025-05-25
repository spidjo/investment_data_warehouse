-- This script checks if the database 'investment_dw' exists, and if it does, it drops the database.
USE master;
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'investment_dw')
BEGIN
    DROP DATABASE investment_dw;
END

CREATE DATABASE investment_dw;
GO

USE investment_dw;
GO
-- Create schemas for the data warehouse
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
BEGIN
    DROP SCHEMA bronze;
END
GO

CREATE SCHEMA bronze;
GO

IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
BEGIN
    DROP SCHEMA silver;
END
GO

CREATE SCHEMA silver;
GO

IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    DROP SCHEMA gold;
END 
GO

CREATE SCHEMA gold;
