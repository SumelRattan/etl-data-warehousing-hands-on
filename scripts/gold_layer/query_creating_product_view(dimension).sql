
CREATE VIEW gold.dim_products AS 
SELECT 
ROW_NUMBER() OVER (ORDER BY pr.prd_start_dt, pr.prd_key) AS product_key,
pr.prd_id AS product_id,
pr.prd_key AS product_number,
pr.prd_nm AS product_name,
pr.cat_id AS category_id,
px.cat AS category,
px.subcat AS subcategory,
px.maintenance,
pr.prd_line AS product_line,
pr.prd_cost AS cost,
pr.prd_start_dt AS product_start_date
FROM silver.crm_prd_info pr
LEFT JOIN silver.erp_px_cat_g1v2 px
ON pr.cat_id = px.id
WHERE pr.prd_end_dt IS NULL       -- filter out all historical data


-- CHECK For duplicates

SELECT product_number, COUNT(*) FROM (
SELECT 
pr.prd_id AS product_id,
pr.cat_id AS category_number,
pr.prd_key AS product_number,
pr.prd_nm AS product_name,
pr.prd_cost AS product_cost,
pr.prd_line AS product_line,
pr.prd_start_dt AS product_start_date,
px.cat AS category,
px.subcat AS subcategory,
px.maintenance
FROM silver.crm_prd_info pr
LEFT JOIN silver.erp_px_cat_g1v2 px
ON pr.cat_id = px.id
WHERE pr.prd_end_dt IS NULL       -- filter out all historical data
)t GROUP BY product_number 
HAVING COUNT(*) > 1

SELECT * FROM gold.dim_products;
SELECT * FROM silver.erp_px_cat_g1v2;
