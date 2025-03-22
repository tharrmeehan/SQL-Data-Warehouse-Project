/*
====================================================================================
Stored Procedure: Load Bronze Layer (Bronze -> Silver)
====================================================================================
Script Purpose:
This stored procedure loads data from the 'bronze' schema, cleans up the data and inserts the cleaned up data into the 'silver' schema.
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

create procedure silver.load_silver()
    language plpgsql
as
$$
DECLARE
    start_time       timestamp;
    end_time         timestamp;
    seconds_diff     INTEGER;
    start_time_batch timestamp;
    end_time_batch   timestamp;
BEGIN
    -- Copy data to docker instance and load data from there (docker cp Documents/Coding/Databases_SQL/Datawarehouse_Project/datasets/ d817fda6fed6:/home/dwh_project)
    -- CRM
    RAISE NOTICE '=================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '=================================================';

    RAISE NOTICE '-------------------------------------------------';
    RAISE NOTICE 'Modifying CRM Tables';
    RAISE NOTICE '-------------------------------------------------';

    ---------------------------------------------
-- CRM CUSTOMER INFORMATION
---------------------------------------------

    start_time := NOW();
    start_time_batch := NOW();
-- Truncate Table so no duplicates appear
    RAISE NOTICE '>> Truncating Table: crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
-- Insert into table
    RAISE NOTICE '>> Inserting Data Into: crm_cust_info';
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

    end_time := NOW();

    seconds_diff := EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>> Time taken for operation: % seconds', seconds_diff;

    ---------------------------------------------
-- CRM PRODUCT INFORMATION
---------------------------------------------

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

    start_time := NOW();
-- Truncate Table so no duplicates appear
    RAISE NOTICE '>> Truncating Table: crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
-- Insert into table
    RAISE NOTICE '>> Inserting Data Into: crm_prd_info';
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

    end_time := NOW();

    seconds_diff := EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>> Time taken for operation: % seconds', seconds_diff;


    ---------------------------------------------
-- CRM SALES INFORMATION
---------------------------------------------

--- Data Overview ---

    start_time := NOW();

-- Truncate Table so no duplicates appear
    RAISE NOTICE '>> Truncating Table: crm_sls_details';
    TRUNCATE TABLE silver.crm_sls_details;
-- Insert into table
    RAISE NOTICE '>> Inserting Data Into: crm_sls_details';
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

    end_time :=  NOW();

    seconds_diff := EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>> Time taken for operation: % seconds', seconds_diff;

    ---------------------------------------------
-- ERP Customer Information
---------------------------------------------

    start_time := NOW();
-- Truncate Table so no duplicates appear
    RAISE NOTICE '>> Truncating Table: erp_cust';
    TRUNCATE TABLE silver.erp_cust;
-- Insert into table
    RAISE NOTICE '>> Inserting Data Into: erp_cust';
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

    end_time :=  NOW();

    seconds_diff := EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>> Time taken for operation: % seconds', seconds_diff;

    ---------------------------------------------
-- ERP Location Information
---------------------------------------------

        start_time := NOW();

-- Truncate Table so no duplicates appear
    RAISE NOTICE '>> Truncating Table: erp_loc';
    TRUNCATE TABLE silver.erp_loc;
-- Insert into table
    RAISE NOTICE '>> Inserting Data Into: erp_loc';
    INSERT INTO silver.erp_loc (erp_cid, erp_cntry)
    SELECT REPLACE(erp_cid, '-', '')    AS erp_cid,  -- Remove unnecessary characters
           CASE
               WHEN TRIM(erp_cntry) = 'DE' THEN 'Germany'
               WHEN TRIM(erp_cntry) IN ('USA', 'US') THEN 'United States'
               WHEN TRIM(erp_cntry) = '' OR erp_cntry IS NULL THEN 'n/a'
               ELSE TRIM(erp_cntry) END AS erp_cntry -- Normalize and Handle missing or blank country codes
    FROM bronze.erp_loc;

    end_time :=  NOW();

    seconds_diff := EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>> Time taken for operation: % seconds', seconds_diff;

    ---------------------------------------------
-- ERP Category Information
---------------------------------------------

    start_time := NOW();


-- Truncate Table so no duplicates appear
    RAISE NOTICE '>> Truncating Table: erp_px_cat';
    TRUNCATE TABLE silver.erp_px_cat;
-- Insert into table
    RAISE NOTICE '>> Inserting Data Into: erp_px_cat';
    INSERT INTO silver.erp_px_cat(erp_id, erp_cat, erp_subcat, erp_maintenance)
    SELECT erp_id, erp_cat, erp_subcat, erp_maintenance
    FROM bronze.erp_px_cat;

    end_time :=  NOW();

    seconds_diff := EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>> Time taken for operation: % seconds', seconds_diff;

    seconds_diff := EXTRACT(EPOCH FROM end_time_batch - start_time_batch);
    RAISE NOTICE '>> Time taken for entire batch: % seconds', seconds_diff;

END;
$$;
alter procedure silver.load_silver() owner to postgres;