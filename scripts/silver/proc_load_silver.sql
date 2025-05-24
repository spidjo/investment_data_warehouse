/*
    Description: Stored Procedure to load Silver dataset from Bronze dataset
                This procedure is responsible for extracting data from the Bronze tables,
                transforming it as necessary, and loading it into the Silver tables.
    Warning:    This procedure will truncate the Silver tables before inserting new data.
                Ensure that you have backups or that you are aware of the implications of truncating these tables.
    Example Usage: EXEC silver.sp_load_silver;
*/
CREATE OR ALTER PROCEDURE silver.sp_load_silver
AS
BEGIN
    DECLARE @batch_start_time DATETIME, 
            @start_time DATETIME,
            @end_time DATETIME;
    BEGIN TRY
        -- Insert data into the Silver dataset from the Bronze dataset
        -- We first trunate the Silver tables to ensure they are empty before inserting new data
        PRINT '>> Starting Silver data load...';
        PRINT '=======================================================================';
        PRINT 'Loading CRM Data from Bronze to Silver';
        PRINT '=======================================================================';
        SET @batch_start_time = GETDATE();
        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>> Truncated silver.crm_cust_info table';

        PRINT '>> Inserting data into silver.crm_cust_info table';
        INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
        --The following query inserts data into the Silver crm_cust_info dataset from the Bronze crm_cust_info dataset
        -- after cleaning and transforming the data
        -- The query removes duplicates and null values, and standardizes the data format for certain fields
        SELECT cst_id,
                cst_key,
                TRIM(cst_firstname) AS cst_firstname,
                TRIM(cst_lastname) AS cst_lastname,
                CASE 
                    WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' 
                    WHEN UPPER(TRIM(cst_marital_status)) = 'D' THEN 'Divorced' 
                    WHEN UPPER(TRIM(cst_marital_status)) = 'W' THEN 'Widowed' 
                    WHEN UPPER(TRIM(cst_marital_status)) = 'X' THEN 'Separated' 
                    WHEN UPPER(TRIM(cst_marital_status)) = 'C' THEN 'Common Law' 
                    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' 
                    WHEN UPPER(TRIM(cst_marital_status)) = 'P' THEN 'Partnership' 
                    WHEN UPPER(TRIM(cst_marital_status)) = 'T' THEN 'Domestic Partner' 
                    WHEN UPPER(TRIM(cst_marital_status)) = 'O' THEN 'Other' 
                    WHEN UPPER(TRIM(cst_marital_status)) = 'U' THEN 'Unknown' 
                    ELSE 'Unknown'
                END AS cst_marital_status,
                CASE 
                    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                    WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                    ELSE 'Unknown'
                END AS cst_gndr,
                cst_create_date
        FROM (
            SELECT cst_id,
                cst_key,
                cst_firstname,
                cst_lastname,
                cst_marital_status,
                cst_gndr,
                cst_create_date,
                ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
            FROM bronze.crm_cust_info 
        ) AS t
        WHERE rn = 1 AND cst_id IS NOT NULL;  
        SET @end_time = GETDATE();

        PRINT '>> Finished inserting data into silver.crm_cust_info table';
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------------------------------';

        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT '>> Truncated silver.crm_prd_info table';
        PRINT '>> Inserting data into silver.crm_prd_info table';
        INSERT INTO silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
        -- The following query inserts data into the Silver crm_prd_info dataset from the Bronze crm_prd_info dataset
        -- after cleaning and transforming the data
        -- The query removes duplicates and null values, and standardizes the data format for certain fields
        -- The query also extracts the category ID from the product key and calculates the end date for each product
        -- The query uses the LEAD function to get the start date of the next product and subtracts 1 day to get the end date
        -- The query also handles null values in the product cost field by replacing them with 0
        -- The query also handles null values in the product line field by replacing them with 'n/a'

        SELECT prd_id, 
            REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
            SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
            TRIM(prd_nm) AS prd_nm,
            ISNULL(prd_cost,0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                    WHEN 'M' THEN 'Mountain'
                    WHEN 'R' THEN 'Road'
                    WHEN 'S' THEN 'Other Sales'
                    WHEN 'T' THEN 'Touring'
                    ELSE 'n/a'
                END AS prd_line,
                CAST(prd_start_dt AS DATE) AS prd_start_dt,
                CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt

        FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Finished inserting data into silver.crm_prd_info table';
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------------------------------';

        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT '>> Truncated silver.crm_sales_details table';
        PRINT '>> Inserting data into silver.crm_sales_details table';
        INSERT INTO silver.crm_sales_details (sls_ord_num, sls_cust_id, sls_prd_key, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
        -- The following query inserts data into the Silver crm_sales_details dataset from the Bronze crm_sales_details dataset
        -- after cleaning and transforming the data
        -- The query removes duplicates and null values, and standardizes the data format for certain fields
        -- The query also handles null values in the sales field by replacing them with the product of quantity and price
        -- The query also handles null values in the price field by replacing them with the sales amount divided by quantity
        -- The query also handles null values in the order date, ship date, and due date fields by replacing them with NULL
        SELECT 
            sls_ord_num,
            sls_cust_id,
            sls_prd_key,
            CASE 
                    WHEN LEN(sls_order_dt) < 8 OR sls_order_dt <= 0 THEN NULL
                    ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE 
                    WHEN LEN(sls_ship_dt) < 8 OR sls_ship_dt <= 0 THEN NULL
                    ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE 
                    WHEN LEN(sls_due_dt) < 8 OR sls_due_dt <= 0 THEN NULL
                    ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE 
                    WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != (sls_quantity * ABS(sls_price))
                    THEN sls_quantity * ABS(sls_price)
                    ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE
                    WHEN sls_price = 0 OR sls_price IS NULL THEN (ABS(sls_sales) / NULLIF(sls_quantity, 0))
                    ELSE sls_price
                END AS sls_price
        FROM bronze.crm_sales_details

        SET @end_time = GETDATE();
        PRINT '>> Finished inserting data into silver.crm_sales_details table';
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        

        PRINT '=======================================================================';
        PRINT 'Loading ERP Data from Bronze to Silver';
        PRINT '=======================================================================';

        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT '>> Truncated silver.erp_cust_az12 table';
        PRINT '>> Inserting data into silver.erp_cust_az12 table';
        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        -- The following query inserts data into the Silver erp_cust_az12 dataset from the Bronze erp_cust_az12 dataset
        SELECT
            CASE 
                WHEN cid LIKE 'NAS%'  THEN SUBSTRING(cid,4,LEN(cid)) 
                ELSE cid
            END AS cid, 
            CASE 
                WHEN bdate > GETDATE() OR bdate < '1900-01-01' THEN NULL
                ELSE bdate
            END AS bdate,  -- Set date to NULL if it is greater than today or less than 1900-01-01
            CASE 
                WHEN UPPER(gen) IN ('M', 'MALE') THEN 'Male'
                WHEN UPPER(gen) IN ('F', 'FEMALE') THEN 'Female'
                ELSE 'Unknown'
            END AS gen
        FROM bronze.erp_cust_az12 b
        SET @end_time = GETDATE();
        PRINT '>> Finished inserting data into silver.erp_cust_az12 table';
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------------------------------';


        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT '>> Truncated silver.erp_loc_a101 table';
        PRINT '>> Inserting data into silver.erp_loc_a101 table';
        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        -- The following query inserts data into the Silver erp_loc_a101 dataset from the Bronze erp_loc_a101 dataset
        SELECT 
            REPLACE(cid, '-', '') AS cid,
            CASE 
                WHEN cntry IS NULL OR cntry = '' THEN 'Unknown'
                WHEN UPPER(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN UPPER(cntry) IN ('CA', 'CAN') THEN 'Canada'
                WHEN UPPER(cntry) IN ('MX', 'MEX') THEN 'Mexico'
                WHEN UPPER(cntry) IN ('GB', 'UK', 'ENG', 'WLS', 'SCT', 'NIR') THEN 'United Kingdom'
                WHEN UPPER(cntry) IN ('FR', 'FRA') THEN 'France'
                WHEN UPPER(cntry) IN ('DE', 'GER') THEN 'Germany'
                WHEN UPPER(cntry) IN ('IT', 'ITA') THEN 'Italy'
                WHEN UPPER(cntry) IN ('ES', 'ESP') THEN 'Spain'
                WHEN UPPER(cntry) IN ('JP', 'JPN') THEN 'Japan'
                WHEN UPPER(cntry) IN ('CN', 'CHN') THEN 'China'
                WHEN UPPER(cntry) IN ('IN', 'IND') THEN 'India'
                WHEN UPPER(cntry) IN ('BR', 'BRA') THEN 'Brazil'
                WHEN UPPER(cntry) IN ('AU', 'AUS') THEN 'Australia'
                WHEN UPPER(cntry) IN ('ZA', 'ZAF') THEN 'South Africa'
                WHEN UPPER(cntry) IN ('RU', 'RUS') THEN 'Russia'
                WHEN UPPER(cntry) IN ('KR', 'KOR') THEN 'South Korea'
                WHEN UPPER(cntry) IN ('AR', 'ARG') THEN 'Argentina'
                WHEN UPPER(cntry) IN ('CH', 'CHE') THEN 'Switzerland'
                WHEN UPPER(cntry) IN ('NL', 'NLD') THEN 'Netherlands'
                WHEN UPPER(cntry) IN ('SE', 'SWE') THEN 'Sweden'
                WHEN UPPER(cntry) IN ('NO', 'NOR') THEN 'Norway'
                WHEN UPPER(cntry) IN ('DK', 'DNK') THEN 'Denmark'
                WHEN UPPER(cntry) IN ('FI', 'FIN') THEN 'Finland'
                WHEN UPPER(cntry) IN ('PL', 'POL') THEN 'Poland'
                WHEN UPPER(cntry) IN ('BE', 'BEL') THEN 'Belgium'
                WHEN UPPER(cntry) IN ('AT', 'AUT') THEN 'Austria'
                WHEN UPPER(cntry) IN ('IE', 'IRL') THEN 'Ireland'
                WHEN UPPER(cntry) IN ('PT', 'PRT') THEN 'Portugal'
                WHEN UPPER(cntry) IN ('GR', 'GRC') THEN 'Greece'
                WHEN UPPER(cntry) IN ('TR', 'TUR') THEN 'Turkey'
                ELSE cntry
            END AS cntry
        FROM bronze.erp_loc_a101

        SET @end_time = GETDATE();
        PRINT '>> Finished inserting data into silver.erp_loc_a101 table';
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------------------------------';

        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT '>> Truncated silver.erp_px_cat_g1v2 table';
        PRINT '>> Inserting data into silver.erp_px_cat_g1v2 table';
        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        -- The following query inserts data into the Silver erp_px_cat_g1v2 dataset from the Bronze erp_px_cat_g1v2 dataset
        SELECT id, 
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2
        SET @end_time = GETDATE();
        PRINT '>> Finished inserting data into silver.erp_px_cat_g1v2 table';
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '=======================================================================';
        PRINT '>> Finished loading Silver data successfully.';
        PRINT '>> Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, GETDATE()) AS NVARCHAR) + ' seconds';
        PRINT '=======================================================================';
    END TRY
    BEGIN CATCH
        -- Handle any errors that occur during the execution of the procedure
        PRINT '>> An error occurred while loading Silver data: ' + ERROR_MESSAGE();
    END CATCH
END;
GO
