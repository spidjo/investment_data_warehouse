/*
=================================================================================
-- Project: Data Warehouse Project
================================================================================
Created by: Siphiwo Lumkwana
=================================================================================
Create Database and Schemas
=================================================================================
Script purpose:
-- This script is designed to create a new Data Warehouse database and set up the necessary schemas.
-- It first checks if the database already exists and drops it if necessary.
-- It then creates the new database and sets up three schemas: Bronze, Silver, and Gold.
-- The Bronze schema is used for raw data ingestion, the Silver schema is for cleaned and transformed data,
-- and the Gold schema is for aggregated and business-ready data.
-- The script is designed to be run in a SQL Server environment and uses T-SQL commands.
-- The script is structured to ensure that the database is created in a clean state,
-- and that the necessary schemas are set up for the data warehouse project.

WARNING:
-- This script will drop the existing DataWarehousePrj database if it exists.
-- Ensure that you have a backup of any important data before running this script.

*/
-- Set the database context to master
USE master;
GO
-- Drop the existing Datawarehouse database
IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehousePrj')
BEGIN
    ALTER DATABASE DataWarehousePrj SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehousePrj;
END;
GO

-- Create the new Datawarehouse database
CREATE DATABASE DataWarehousePrj;
GO

-- Use the new Datawarehouse database
USE DataWarehousePrj;
GO


-- Create the 3 Schemas (Bronze, Silver, Gold)
CREATE SCHEMA Bronze;
GO
CREATE SCHEMA Silver;
GO
CREATE SCHEMA Gold;
GO

