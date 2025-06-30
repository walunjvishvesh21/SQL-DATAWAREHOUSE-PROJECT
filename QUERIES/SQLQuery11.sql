------GOLD LAYER QUERIES ----------

---- joining customer crm and erp------
create view gold.dim_customers as 
SELECT 
row_number() over (order by (cst_id)) as customer_key,
cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
ci.cst_material_status as marital_status,
la.cntry as country,
case when ci.cst_gndr != 'n/a' then ci.cst_gndr
      else coalesce(ca.gen , 'n/a')
end as gender,

ci.cst_create_date as create_date,
ca.bdate as birthdate

FROM SILVER.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid




------joining product erp and crm---------
create view gold.dim_products as 

SELECT
    row_number() over (order by pn.prd_start_dt,pn.prd_key) as product_key ,
    pn.prd_id AS product_id,

    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL --Filter out all historical data





------------creating gold fact sales------------
CREATE VIEW gold.fact_sales AS
SELECT
  sd.sls_ord_num AS order_number,
  pr.product_key,
  cu.customer_key,
  sd.sls_order_dt AS order_date,
  sd.sls_ship_dt AS shipping_date,
  sd.sls_due_dt AS due_date,
  sd.sls_sales AS sales_amount,
  sd.sls_quantity AS quantity,
  sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr 
  ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu 
  ON sd.sls_cust_id = cu.customer_id;
