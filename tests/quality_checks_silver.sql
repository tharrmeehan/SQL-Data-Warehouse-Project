/*
====================================================================================
Data Quality Checks (Silver Layer
====================================================================================
Script Purpose:
The goal of this script is to find common data quality issues (Data Exploration), these can be fixed directly while inserting.

It performs the following actions:
    - Truncates the silver tables before loading data.
    - Uses the 'INSERT' command to insert cleaned data into silver tables.
    - Uses different methods to clean up every column, from changing data types, truncation and much more!

Parameters:
    None. This stored procedure does not accept any parameters or return any values.

Usage Example:
    call silver.load_silver();
====================================================================================

*/


---------------------------------------------
-- CRM CUSTOMER INFORMATION
---------------------------------------------

-- Check for unwanted Spaces
-- Expectations: No Results
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- CHeck for NULLS or Negative Numbers
-- Expectations: No Results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0
   OR prd_cost IS NULL;

-- Check for Invalid Data Orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-----------------------------------------
-- Recheck Silver Quality
-- Null Check / Duplicate Check
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1
    OR cst_id IS NULL;

-- Check for unwanted Spaces
SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

-- CHeck for NULLS or Negative Numbers
-- Expectations: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0
   OR prd_cost IS NULL;

---------------------------------------------
-- CRM PRODUCT INFORMATION
---------------------------------------------

--- Data Overview ---
SELECT prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
    from bronze.crm_prd_info;

SELECT prd_id, COUNT(*)
    from bronze.crm_prd_info
    GROUP BY prd_id
    HAVING COUNT(*) > 1
    OR prd_id IS NULL;

-- Date Rearrangement
SELECT prd_id,
    prd_key,
    prd_nm,
    prd_start_dt,
    prd_end_dt,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt_test
    FROM bronze.crm_prd_info
    WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

---------------------------------------------
-- CRM SALES INFORMATION
---------------------------------------------

--- Check for Invalid Dates (Negatives or Zeros) ---
SELECT sls_order_dt
    FROM silver.crm_sls_details
    WHERE sls_order_dt IS NULL
    OR sls_order_dt < DATE '1900-01-01'
    OR sls_order_dt > DATE '2050-01-01';

--- Check for Invalid Date Orders (Order date can't be later than shipping/due date) ---
SELECT *
    FROM silver.crm_sls_details
    WHERE sls_order_dt > sls_ship_dt
    OR sls_order_dt > sls_due_dt;

--- Check Data consistency: between sales, quantity and price ---
--- >> Sales = Quantity * Price
--- >> Values must not be Null, zero or negative
SELECT sls_sales,
    sls_quantity,
    sls_price
    FROM silver.crm_sls_details
    WHERE sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL
    OR sls_quantity IS NULL
    OR sls_price IS NULL
    OR sls_sales <= 0
    OR sls_quantity <= 0
    OR sls_price <= 0;

SELECT *
    from silver.crm_sls_details;

---------------------------------------------
-- ERP Customer Information
---------------------------------------------

-- Identify out-of-range date values

SELECT DISTINCT erp_dt
    FROM silver.erp_cust
    WHERE erp_dt < '1924-01-01'
    OR erp_dt > NOW();

-- Data Standardization & Consistency
SELECT DISTINCT erp_gndr,
    CASE
    WHEN UPPER(TRIM (erp_gndr)) IN ('F', 'FEMALE') THEN 'Female'
    WHEN UPPER(TRIM (erp_gndr)) IN ('M', 'MALE') THEN 'Male'
    ELSE 'n/a'
END as erp_gndr
    FROM silver.erp_cust;

---------------------------------------------
-- ERP Location Information
---------------------------------------------

-- Data Standardization & Consistency
SELECT DISTINCT erp_cntry
    FROM bronze.erp_loc
    ORDER BY erp_cntry;

-- Final Check
SELECT *
    FROM silver.erp_loc;

---------------------------------------------
-- ERP Category Information
---------------------------------------------

-- Check for unwanted Spaces
SELECT *
    from bronze.erp_px_cat
    WHERE erp_px_cat.erp_maintenance != TRIM (erp_maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT erp_px_cat.erp_cat FROM bronze.erp_px_cat;