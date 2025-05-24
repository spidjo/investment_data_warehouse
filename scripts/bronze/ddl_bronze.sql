/*
   Create Bronze Schema Tables
    Description: This script creates the necessary tables in the bronze schema of the Data Warehouse project.
    Warning:     This script will drop existing tables if they already exist, so use with caution.
*/

-- Change the database context to the Data Warehouse project
-- Ensure you have the correct database context before running this script
USE DataWarehousePrj;
-- Create the table in bronze schema if it doesn't exist
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;  -- Check if the table exists
-- Drop the table if it exists
-- Create the table in bronze schema
CREATE TABLE bronze.crm_cust_info (
    cst_id INT, 
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gender NVARCHAR(50),
    cst_create_date DATE
);

-- Create the table in bronze schema if it doesn't exist
-- Check if the table exists
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;  -- Drop the table if it exists
-- Create the table in bronze schema        
CREATE TABLE bronze.crm_prd_info(
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);

-- Create the table in bronze schema if it doesn't exist
-- Check if the table exists
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details; -- Drop the table if it exists
-- Create the table in bronze schema
CREATE TABLE bronze.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

-- Create the table in bronze schema if it doesn't exist
-- Check if the table exists
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12; -- Drop the table if it exists
-- Create the table in bronze schema
CREATE TABLE bronze.erp_cust_az12 (
    cid NVARCHAR(50),
    BDATE DATE,
    GEN NVARCHAR(50)
);

-- Create the table in bronze schema if it doesn't exist
-- Check if the table exists
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101; -- Drop the table if it exists
-- Create the table in bronze schema
CREATE TABLE bronze.erp_loc_a101 (
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
);

-- Create the table in bronze schema if it doesn't exist
-- Check if the table exists
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2; -- Drop the table if it exists
-- Create the table in bronze schema
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id  NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
);
