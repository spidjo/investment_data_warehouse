/*
      This script creates or alters views in the 'gold' schema for a data warehouse.
      It includes dimensions and a fact table (Star Schema).
      The views are built on top of existing silver schema tables.

      Purpose: To provide a consolidated view of customer, product, and sales data for reporting and analysis.

*/

-- Create dimension dim_customers

CREATE OR ALTER VIEW gold.dim_customers AS
      SELECT 
            ROW_NUMBER() OVER (ORDER BY c.cst_id) AS customer_key,
            c.cst_id AS customer_id,
            c.cst_key AS customer_number,
            c.cst_firstname AS first_name,
            c.cst_lastname AS last_name,
            l.cntry AS country,
            c.cst_marital_status AS marital_status,
            CASE 
                  WHEN cst_gndr = 'Unknown' THEN  -- CRM is the source of truth for gender
                        COALESCE(ec.gen, 'Unknown')
                  ELSE cst_gndr 
            END  AS gender,
            ec.bdate AS birth_date,
            c.cst_create_date AS create_date      
      FROM silver.crm_cust_info c  --18484
      LEFT JOIN   silver.erp_cust_az12 ec 
      ON          c.cst_key = ec.cid 
      LEFT JOIN   silver.erp_loc_a101 l
      ON          c.cst_key = l.cid 


-- Create dimension dim_products
CREATE OR ALTER VIEW gold.dim_products AS
      SELECT 
            ROW_NUMBER() OVER (ORDER BY p.prd_id) AS product_key,
            p.prd_id AS product_id,
            p.prd_key AS product_number,
            p.prd_nm AS product_name,
            p.cat_id AS category_id,
            px.cat AS category,
            px.subcat AS subcategory,
            px.maintenance AS maintenance,
            p.prd_cost AS cost,
            p.prd_line AS product_line,
            p.prd_start_dt AS start_date
      FROM silver.crm_prd_info p
      LEFT JOIN silver.erp_px_cat_g1v2 px 
      ON p.cat_id = px.id
      WHERE p.prd_end_dt IS NULL;  -- Only active products



-- Create fact table fact_sales
CREATE OR ALTER VIEW gold.fact_sales AS 
      SELECT 
            sd.sls_ord_num AS order_number, 
            p.product_key,
            c.customer_key,
            sd.sls_order_dt AS order_date,
            sd.sls_ship_dt AS ship_date,
            sd.sls_due_dt AS due_date,
            sd.sls_sales AS sales_amount,
            sd.sls_quantity AS quantity,
            sd.sls_price AS price
      FROM silver.crm_sales_details sd   
      LEFT JOIN gold.dim_customers c
      ON sd.sls_cust_id = c.customer_id
      LEFT JOIN gold.dim_products p
      ON sd.sls_prd_key = p.product_number;
