/*
====================================================================================
DDL_Script: Create Gold Views
====================================================================================
Script Purpose:
    This script creates views for the Gold Layer in the data warehouse.
    The gold layer represents the final layer of our medallion architecture.
    It includes fact and dimension tables.

    Each view performs transformations and combines data from the silver layer to produce
    clean, enriched and business-ready data.

Usage:
    - The views can be directly queried for analytics and reporting
====================================================================================

*/

CREATE VIEW gold.dim_customers AS
SELECT row_number() over (ORDER BY cst_id)                                                   AS customer_key,
       ci.cst_id                                                                             AS customer_id,
       ci.cst_key                                                                            AS customer_number,
       ci.cst_firstname                                                                      AS first_name,
       ci.cst_lastname                                                                       AS last_name,
       la.erp_cntry                                                                          AS country,
       ci.cst_material_status                                                                AS martial_status,
       CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr ELSE COALESCE(ca.erp_gndr, 'n/a') END AS gender,
       ca.erp_dt                                                                             AS birthdate,
       ci.cst_create_date                                                                    AS create_date
FROM silver.crm_cust_info AS ci
         LEFT JOIN silver.erp_cust AS ca
                   ON ci.cst_key = ca.erp_cid
         LEFT JOIN silver.erp_loc AS la ON ci.cst_key = la.erp_cid;

CREATE VIEW gold.dim_products AS
(
SELECT ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
       pn.prd_id                                                AS product_id,
       pn.prd_key                                               AS product_number,
       pn.prd_nm                                                AS product_name,
       pn.cat_id                                                AS category_id,
       pc.erp_cat                                               AS category,
       pc.erp_subcat                                            AS subcategory,
       pc.erp_maintenance                                       AS maintenance,
       pn.prd_cost                                              AS product_cost,
       pn.prd_line                                              AS product_line,
       pn.prd_start_dt                                          AS start_date
FROM silver.crm_prd_info AS pn
         LEFT JOIN silver.erp_px_cat AS pc
                   ON pn.cat_id = pc.erp_id
WHERE prd_end_dt IS NULL);

CREATE VIEW gold.fact_sales AS
(
SELECT sd.sls_ord_num  AS order_number,
       pr.product_key,
       cu.customer_key,
       sd.sls_order_dt AS order_date,
       sd.sls_ship_dt  AS shipping_date,
       sd.sls_due_dt   AS due_date,
       sd.sls_quantity AS quantity,
       sd.sls_price    AS price,
       sd.sls_sales    AS sales_amount
FROM silver.crm_sls_details AS sd
         LEFT JOIN gold.dim_products as pr
                   ON sd.sls_prd_key = pr.product_number
         LEFT JOIN gold.dim_customers AS cu
                   ON sd.sls_cust_id = cu.customer_id)