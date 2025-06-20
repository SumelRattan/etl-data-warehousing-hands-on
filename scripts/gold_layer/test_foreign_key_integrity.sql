SELECT * FROM gold.dim_customer

SELECT * FROM gold.dim_products

SELECT * FROM gold.fact_sales

SELECT * FROM silver.crm_sales_details

-------FORIEGN KEY INTEGRITY (Dimensions)
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customer c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE c.customer_key IS NULL