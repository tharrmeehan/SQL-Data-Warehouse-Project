/*
====================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
====================================================================================
Script Purpose:
This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the 'COPY' command to load data from csv Files to bronze tables.

Parameters:
    None. This stored procedure does not accept any parameters or return any values.

Usage Example:
    call bronze.load_bronze();
====================================================================================

*/

create procedure load_bronze()
    language plpgsql
as
$$
    DECLARE
        start_time timestamp;
        end_time timestamp;
        seconds_diff INTEGER;
        start_time_batch timestamp;
        end_time_batch timestamp;
BEGIN
    -- Copy data to docker instance and load data from there (docker cp Documents/Coding/Databases_SQL/Datawarehouse_Project/datasets/ d817fda6fed6:/home/dwh_project)
    -- CRM
    RAISE NOTICE '=================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '=================================================';

    RAISE NOTICE '-------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '-------------------------------------------------';

    -- Customer Info
    start_time :=  NOW();
    start_time_batch := NOW();
    RAISE NOTICE '>> Truncating Table: crm_cust_info';
    TRUNCATE TABLE crm_cust_info;

    RAISE NOTICE '>> Inserting Data Into: crm_cust_info';
    COPY crm_cust_info
    FROM '/home/dwh_project/datasets/source_crm/cust_info.csv'
    DELIMITER ','
    CSV HEADER;
    end_time :=  NOW();

    seconds_diff := EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>> Time taken for operation: % seconds', seconds_diff;

    -- Product Info
    start_time :=  NOW();
    RAISE NOTICE '>> Truncating Table: crm_prd_info';
    TRUNCATE TABLE crm_prd_info;

    RAISE NOTICE '>> Inserting Data Into: crm_prd_info';
    COPY crm_prd_info
    FROM '/home/dwh_project/datasets/source_crm/prd_info.csv'
    DELIMITER ','
    CSV HEADER;
    end_time :=  NOW();

    seconds_diff := EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>> Time taken for operation: % seconds', seconds_diff;

    -- Sales Details
    start_time :=  NOW();
    RAISE NOTICE '>> Truncating Table: crm_sls_details';
    TRUNCATE TABLE crm_sls_details;

    RAISE NOTICE '>> Inserting Data Into: crm_sls_details';
    COPY crm_sls_details
    FROM '/home/dwh_project/datasets/source_crm/sales_details.csv'
    DELIMITER ','
    CSV HEADER;
    end_time :=  NOW();

    seconds_diff := EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>> Time taken for operation: % seconds', seconds_diff;

    -- ERP
    RAISE NOTICE '-------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '-------------------------------------------------';
    -- Customer Info
    start_time :=  NOW();
    RAISE NOTICE '>> Truncating Table: erp_cust';
    TRUNCATE TABLE erp_cust;

    RAISE NOTICE '>> Inserting Data Into: erp_cust';
    COPY erp_cust
    FROM '/home/dwh_project/datasets/source_erp/CUST_AZ12.csv'
    DELIMITER ','
    CSV HEADER;
    end_time :=  NOW();

    seconds_diff := EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>> Time taken for operation: % seconds', seconds_diff;

    -- Location Data
    start_time :=  NOW();
    RAISE NOTICE '>> Truncating Table: erp_loc';
    TRUNCATE TABLE erp_loc;

    RAISE NOTICE '>> Inserting Data Into: erp_loc';
    COPY erp_loc
    FROM '/home/dwh_project/datasets/source_erp/LOC_A101.csv'
    DELIMITER ','
    CSV HEADER;
    end_time :=  NOW();

    seconds_diff := EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>> Time taken for operation: % seconds', seconds_diff;

    -- PX Category Data
    start_time :=  NOW();
    RAISE NOTICE '>> Truncating Table: erp_px_cat';
    TRUNCATE TABLE erp_px_cat;

    RAISE NOTICE '>> Inserting Data Into: erp_px_cat';
    COPY erp_px_cat
    FROM '/home/dwh_project/datasets/source_erp/PX_CAT_G1V2.csv'
    DELIMITER ','
    CSV HEADER;

    end_time :=  NOW();
    end_time_batch := NOW();

    seconds_diff := EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>> Time taken for operation: % seconds', seconds_diff;

    seconds_diff := EXTRACT(EPOCH FROM end_time_batch - start_time_batch);
    RAISE NOTICE '>> Time taken for entire batch: % seconds', seconds_diff;
END;$$;

alter procedure load_bronze() owner to postgres;

