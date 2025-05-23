
/*
  Script Name: sp_load_bronze.sql
  Description: This script creates a stored procedure to load data into the bronze schema
  Script Purpose: The purpose of this script is to load data into the bronze schema
  The bronze schema is the first layer of the data lakehouse architecture
  The bronze schema is used to store raw data from various source systems
  The data is in CSV format'

  This procedure does not accept any parameters.

  Usage:
  EXEC bronze.sp_load_bronze;
*/

CREATE OR ALTER PROCEDURE bronze.sp_load_bronze
AS

BEGIN
    BEGIN TRY
        PRINT '=====================================';
        PRINT 'Loading data into the bronze schema...';
        PRINT '=====================================';

        PRINT '-------------------------------------------------';
        PRINT 'Loading CRM data into the bronze schema...';
        PRINT '-------------------------------------------------';
        -- Declare the path to the source data and other variables
        DECLARE 
            @crm_path NVARCHAR(MAX) = 'C:\Users\siphi\OneDrive\Documents\Hustle\DB\MS SQL Course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm',
            @erp_path NVARCHAR(MAX) = 'C:\Users\siphi\OneDrive\Documents\Hustle\DB\MS SQL Course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp',
            @start_time DATETIME,
            @end_time DATETIME,
            @batch_start_time DATETIME,
            @batch_end_time DATETIME;
        --first truncate the table
        SET @batch_start_time = GETDATE();
        SET @start_time = GETDATE();
        PRINT ' >> Truncating Table: crm_cust_info table...';
        TRUNCATE TABLE bronze.crm_cust_info;
        -- then bulk insert the data
        -- from the csv file into the bronze schema
        PRINT ' >> Inserting data into: crm_cust_info table...';
        DECLARE @sql NVARCHAR(MAX) = N'
        BULK INSERT bronze.crm_cust_info
        FROM ''' + @crm_path + '\cust_info.csv''
        WITH
        (
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''\n'',
            FIRSTROW = 2,
            MAXERRORS = 0,
            TABLOCK 
        );';
        EXEC sp_executesql @sql;

        SET @end_time = GETDATE();
        PRINT ' >> Data load completed in: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds.';


        -- then truncate the table crm_prd_info
        PRINT ' >> Truncating Table: crm_prd_info table...';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_prd_info;
        -- then bulk insert the data
        PRINT ' >> Inserting data into: crm_prd_info table...';
        SET @sql = N'
        BULK INSERT bronze.crm_prd_info
        FROM ''' + @crm_path + '\prd_info.csv''
        WITH
        (
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''\n'',
            FIRSTROW = 2,
            MAXERRORS = 0,
            TABLOCK
        );';
        EXEC sp_executesql @sql;
        SET @end_time = GETDATE();
        PRINT ' >> Data load completed in: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds.';

        -- then truncate the table crm_sales_details
        PRINT ' >> Truncating Table: crm_sales_details table...';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;
        -- then bulk insert the data
        PRINT ' >> Inserting data into: crm_sales_details table...';
        SET @sql = N'
        BULK INSERT bronze.crm_sales_details
        FROM ''' + @crm_path + '\sales_details.csv''
        WITH
        (
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''\n'',
            FIRSTROW = 2,
            MAXERRORS = 0,
            TABLOCK
        );';
        EXEC sp_executesql @sql;
        SET @end_time = GETDATE();
        PRINT ' >> Data load completed in: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds.';

        PRINT '-------------------------------------------------';
        PRINT 'Loading ERP data into the bronze schema...';
        PRINT '-------------------------------------------------';

        -- then truncate the table erp_cust_az12
        PRINT ' >> Truncating Table: erp_cust_az12 table...';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_cust_az12;
        -- then bulk insert the data
        PRINT ' >> Inserting data into: erp_cust_az12 table...';
        SET @sql = N'
        BULK INSERT bronze.erp_cust_az12
        FROM ''' + @erp_path + '\cust_az12.csv''
        WITH
        (
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''\n'',
            FIRSTROW = 2,
            MAXERRORS = 0,
            TABLOCK
        );';
        EXEC sp_executesql @sql;
        SET @end_time = GETDATE();
        PRINT ' >> Data load completed in: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds.';

        -- then truncate the table erp_loc_a101
        PRINT ' >> Truncating Table: erp_loc_a101 table...';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_loc_a101;
        -- then bulk insert the data
        PRINT ' >> Inserting data into: erp_loc_a101 table...';
        SET @sql = N'
        BULK INSERT bronze.erp_loc_a101
        FROM ''' + @erp_path + '\loc_a101.csv''
        WITH
        (
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''\n'',
            FIRSTROW = 2,
            MAXERRORS = 0,
            TABLOCK
        );';
        EXEC sp_executesql @sql;
        SET @end_time = GETDATE();
        PRINT ' >> Data load completed in: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds.';

        -- then truncate the table erp_px_cat_g1v2
        PRINT ' >> Truncating Table: erp_px_cat_g1v2 table...';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        -- then bulk insert the data
        PRINT ' >> Inserting data into: erp_px_cat_g1v2 tabl    e...';
        SET @sql = N'
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM ''' + @erp_path + '\px_cat_g1v2.csv''
        WITH
        (
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''\n'',
            FIRSTROW = 2,
            MAXERRORS = 0,
            TABLOCK
        );';
        EXEC sp_executesql @sql;
        SET @end_time = GETDATE();
        PRINT ' >> Data load completed in: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds.';
        SET @batch_end_time = GETDATE();
        PRINT '-------------------------------------------------';
        PRINT ' >> Batch load completed in: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(10)) + ' seconds.';
        

    END TRY
    BEGIN CATCH
        PRINT 'Error occurred while loading data into bronze schema.';
        PRINT ERROR_MESSAGE();
    END CATCH;

/*
    -- Check the number of records in each table
    -- to confirm that the data was inserted successfully
    PRINT '-------------------------------------------------';
    PRINT 'Checking record counts in bronze schema tables...';
    PRINT '-------------------------------------------------';

    DECLARE @table_name NVARCHAR(128),
            @schema_name NVARCHAR(128);

    SET @schema_name = 'bronze';
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
    DEALLOCATE table_cursor; */
END
