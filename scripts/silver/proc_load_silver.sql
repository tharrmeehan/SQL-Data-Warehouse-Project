---------------------------------------------
-- CRM CUSTOMER INFORMATION
---------------------------------------------

-- Check for Nulls or Duplicates
INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_material_status, cst_gndr,
                                  cst_create_date)
SELECT cst_id,
       cst_key,
       TRIM(cst_firstname) AS cst_firstname,
       TRIM(cst_lastname)  AS cst_lastname,
       CASE
           WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
           WHEN UPPER(TRIM(cst_material_status)) = 'M' then 'Married'
           ELSE 'n/a'
           END                cst_material_status,
       CASE
           WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
           WHEN UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
           ELSE 'n/a'
           END                cst_gndr,
       cst_create_date
FROM (SELECT *, row_number() over (partition by cst_id order by cst_create_date DESC) as flag_last
      FROM bronze.crm_cust_info
      WHERE cst_id IS NOT NULL) as "*fl"
WHERE flag_last = 1;


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

/*
 Issues found:
     prd_id: None
     prd_key: None
     prd_nm: None
     prd_cost: NULL values
     prd_line: Descriptive value names instead of short forms.
     prd_start_dt: None
     prd_end_dt: NULL values
 */

SELECT prd_id, COUNT(*)
from bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1
    OR prd_id IS NULL;

INSERT INTO silver.crm_prd_info(prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
SELECT prd_id,
       REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')                                            AS cat_id,
       SUBSTRING(prd_key, 7, LENGTH(prd_key))                                                 AS prd_key,
       prd_nm,
       COALESCE(prd_cost, 0)                                                                  AS prd_cost,
       CASE UPPER(TRIM(prd_line))
           WHEN 'M' Then 'Mountain'
           WHEN 'R' Then 'Road'
           WHEN 'S' Then 'Other Sales'
           WHEN 'T' Then 'Touring'
           ELSE 'n/a'
           END                                                                                AS prd_line,
       CAST(prd_start_dt AS DATE)                                                             AS prd_start_dt,
       CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LENGTH(prd_key)) IN (SELECT sls_prd_key FROM bronze.crm_sls_details);

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

--- Data Overview ---
INSERT INTO silver.crm_sls_details(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt,
                                   sls_sales, sls_quantity, sls_price)
SELECT sls_ord_num,
       sls_prd_key,
       sls_cust_id,
       CASE
           WHEN sls_order_dt = 0 OR length(sls_order_dt::text) != 8 THEN NULL
           ELSE TO_DATE(sls_order_dt::text, 'YYYYMMDD') END as sls_order_dt,
       CASE
           WHEN sls_ship_dt = 0 OR length(sls_ship_dt::text) != 8 THEN NULL
           ELSE TO_DATE(sls_ship_dt::text, 'YYYYMMDD') END  as sls_ship_dt,
       CASE
           WHEN sls_due_dt = 0 OR length(sls_due_dt::text) != 8 THEN NULL
           ELSE TO_DATE(sls_due_dt::text, 'YYYYMMDD') END   as sls_due_dt,
       CASE
           WHEN sls_sales IS NULL OR sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
               THEN sls_quantity * ABS(sls_price)
           ELSE sls_sales
           END                                              AS sls_sales,
       sls_quantity,
       CASE
           WHEN sls_price IS NULL OR sls_price <= 0
               THEN sls_sales / NULLIF(sls_quantity, 0)
           ELSE sls_price
           END                                              AS sls_price
FROM bronze.crm_sls_details;

/*
 Issues found:
        sls_ord_num: None
        sls_prd_key: None
        sls_cust_id: None
        sls_order_dt: Change Datatype from Integer from to Date
        sls_ship_dt: Change Datatype from Integer from to Date
        sls_due_dt: Change Datatype from Integer from to Date
        sls_sales:
        sls_quantity:
        sls_price:
 */

--- Check for Invalid Dates (Negatives or Zeros) ---
SELECT NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM silver.crm_sls_details
WHERE sls_order_dt <= 0
   OR length(sls_order_dt) != 8
   OR sls_order_dt > 2050 - 01 - 01
   OR sls_order_dt < 1900 - 01 - 01;

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
INSERT INTO silver.erp_cust(erp_cid, erp_dt, erp_gndr)
SELECT CASE WHEN erp_cid LIKE 'NAS%' THEN SUBSTRING(erp_cid, 4, LENGTH(erp_cid)) ELSE erp_cid END AS erp_cid,
       CASE
           WHEN erp_dt > NOW() THEN NULL
           ELSE erp_dt
           END                                                                                    AS erp_dt,

       CASE
           WHEN UPPER(TRIM(erp_gndr)) IN ('F', 'FEMALE') THEN 'Female'
           WHEN UPPER(TRIM(erp_gndr)) IN ('M', 'MALE') THEN 'Male'
           ELSE 'n/a' END                                                                         as erp_gndr
from bronze.erp_cust;

-- Identify out-of-range date values

SELECT DISTINCT erp_dt
FROM silver.erp_cust
WHERE erp_dt < '1924-01-01'
   OR erp_dt > NOW();

-- Data Standardization & Consistency
SELECT DISTINCT erp_gndr,
                CASE
                    WHEN UPPER(TRIM(erp_gndr)) IN ('F', 'FEMALE') THEN 'Female'
                    WHEN UPPER(TRIM(erp_gndr)) IN ('M', 'MALE') THEN 'Male'
                    ELSE 'n/a' END as erp_gndr
FROM silver.erp_cust;

---------------------------------------------
-- ERP Location Information
---------------------------------------------

INSERT INTO silver.erp_loc (erp_cid, erp_cntry)
SELECT REPLACE(erp_cid, '-', '')    AS erp_cid,  -- Remove unnecessary characters
       CASE
           WHEN TRIM(erp_cntry) = 'DE' THEN 'Germany'
           WHEN TRIM(erp_cntry) IN ('USA', 'US') THEN 'United States'
           WHEN TRIM(erp_cntry) = '' OR erp_cntry IS NULL THEN 'n/a'
           ELSE TRIM(erp_cntry) END AS erp_cntry -- Normalize and Handle missing or blank country codes
FROM bronze.erp_loc;

-- Data Standardization & Consistency
SELECT DISTINCT erp_cntry
FROM bronze.erp_loc
ORDER BY erp_cntry;

-- Final Check
SELECT *
FROM silver.erp_loc

