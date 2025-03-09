/*
==
Create Database and Schemas
==
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists.
    If the database exists, it is dropped and recreated.
    Additionally, the script sets up three schemas within the database: 'bronze', 'silver' and 'gold'.

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists.
    All data in the database will be permanently deleted. Proceed with caution
    and ensure you have proper backups before running this script.
*/

-- Create Table for Bronze Schema containing Customer Information from a sample CRM

CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_material_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
);

CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(1),
    prd_start_dt DATE,
    prd_end_dt DATE
);

CREATE TABLE bronze.crm_sls_details (
    sls_ord_num VARCHAR(10),
    sls_prd_key VARCHAR(15),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

-- Create Table for Bronze Schema containing Customer Information from a sample ERP

CREATE TABLE bronze.erp_cust (
    erp_cid VARCHAR(30),
    erp_dt DATE,
    erp_gndr VARCHAR(7)
);

CREATE TABLE bronze.erp_loc (
    erp_cid VARCHAR(30),
    erp_cntry VARCHAR(20)
);

CREATE TABLE bronze.erp_px_cat (
    erp_id VARCHAR(30),
    erp_cat VARCHAR(30),
    erp_subcat VARCHAR(30),
    erp_maintenance VARCHAR(5)
);