
/*
    Description: Procedure to load data into the bronze schema.
                This procedure performs the following tasks:
                    1. Truncates existing tables in the bronze schema.
                    2. Loads data from CSV files into the bronze schema tables.
                    3. Handles errors during the data loading process.
    Warning:    This procedure will truncate existing tables in the bronze schema.
                Ensure that you have backups or that you are okay with losing existing data before running this procedure.
    Example Usage: EXEC bronze.sp_bronze_load_data
*/

USE investment_dw;
GO

CREATE OR ALTER PROCEDURE bronze.sp_bronze_load_data
AS

BEGIN
    BEGIN TRY
        PRINT '=====================================';
        PRINT 'Loading data into the bronze schema...';
        PRINT '=====================================';

        -- Declare the path to the source data and other variables
        DECLARE 
            @file_path NVARCHAR(MAX) = 'C:\Users\siphi\MSSQL\Data\',
            @start_time DATETIME,
            @batch_start_time DATETIME;
        --first truncate the table
        SET @batch_start_time = GETDATE();
        SET @start_time = GETDATE();
        PRINT ' >> Truncating Table: tbl_crm_clients table...';
        TRUNCATE TABLE bronze.tbl_crm_clients;
        -- then bulk insert the data
        -- from the csv file into the bronze schema
        PRINT ' >> Inserting data into: tbl_crm_clients table...';
        DECLARE @sql NVARCHAR(MAX) = N'
        BULK INSERT bronze.tbl_crm_clients
        FROM ''' + @file_path + 'clients.csv''
        WITH
        (
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''\n'',
            FORMAT = ''CSV'',
            FIELDQUOTE = ''"'',
            FIRSTROW = 2,
            MAXERRORS = 0,
            TABLOCK
        );';
        EXEC sp_executesql @sql;

        PRINT ' >> Data load completed in: ' + CAST(DATEDIFF(SECOND, @start_time, GETDATE()) AS NVARCHAR(10)) + ' seconds.';

        -- then truncate the table tbl_ims_accounts
        PRINT ' >> Truncating Table: tbl_ims_accounts table...';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.tbl_ims_accounts;
        -- then bulk insert the data
        PRINT ' >> Inserting data into: tbl_ims_accounts table...';
        SET @sql = N'
        BULK INSERT bronze.tbl_ims_accounts
        FROM ''' + @file_path + 'accounts.csv''
        WITH
        (
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''\n'',
            FORMAT = ''CSV'',
            FIELDQUOTE = ''"'',
            FIRSTROW = 2,
            MAXERRORS = 0,
            TABLOCK
        );';
        EXEC sp_executesql @sql;
        PRINT ' >> Data load completed in: ' + CAST(DATEDIFF(SECOND, @start_time, GETDATE()) AS NVARCHAR(10)) + ' seconds.';

        -- then truncate the table tbl_tp_transactions
        PRINT ' >> Truncating Table: tbl_tp_transactions table...';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.tbl_tp_transactions;
        -- then bulk insert the data
        PRINT ' >> Inserting data into: tbl_tp_transactions table...';
        SET @sql = N'
        BULK INSERT bronze.tbl_tp_transactions
        FROM ''' + @file_path + 'transactions.csv''
        WITH
        (
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''\n'',
            FORMAT = ''CSV'',
            FIELDQUOTE = ''"'',
            FIRSTROW = 2,
            MAXERRORS = 0,
            TABLOCK
        );';
        EXEC sp_executesql @sql;
        PRINT ' >> Data load completed in: ' + CAST(DATEDIFF(SECOND, @start_time, GETDATE()) AS NVARCHAR(10)) + ' seconds.';

        -- then truncate the table tbl_ff_market_data
        PRINT ' >> Truncating Table: tbl_ff_market_data table...';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.tbl_ff_market_data;
        -- then bulk insert the data
        PRINT ' >> Inserting data into: tbl_ff_market_data tabl    e...';
        SET @sql = N'
        BULK INSERT bronze.tbl_ff_market_data
        FROM ''' + @file_path + 'market_data.csv''
        WITH
        (
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''\n'',
            FORMAT = ''CSV'',
            FIELDQUOTE = ''"'',
            FIRSTROW = 2,
            MAXERRORS = 0,
            TABLOCK
        );';
        EXEC sp_executesql @sql;
        PRINT ' >> Data load completed in: ' + CAST(DATEDIFF(SECOND, @start_time, GETDATE()) AS NVARCHAR(10)) + ' seconds.';
        
        PRINT '-------------------------------------------------';
        PRINT ' >> Batch load completed in: ' + CAST(DATEDIFF(SECOND, @batch_start_time, GETDATE()) AS NVARCHAR(10)) + ' seconds.';
        

    END TRY
    BEGIN CATCH
        PRINT 'Error occurred while loading data into bronze schema.';
        PRINT ERROR_MESSAGE();
    END CATCH;

END