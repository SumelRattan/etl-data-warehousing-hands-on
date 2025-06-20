CREATE VIEW gold.dim_customer AS 
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	cl.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr    -- CRM is the master of gender info
		 ELSE COALESCE(ca.gen, 'n/a')                  -- COALESCE Function to replace NULL values with n/a
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci    -- ci as alias
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 cl
ON ci.cst_key = cl.cid


-- AFTER JOINs, check if any duplicates were introduced by the join logic
SELECT cst_id, COUNT(*) FROM 
(SELECT 
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gndr,
ca.bdate,
CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr    -- CRM is the master of gender info
	 ELSE COALESCE(ca.gen, 'n/a')                   -- COALESCE Function to replace NULL values with n/a
END AS new_gen,
cl.cntry
FROM silver.crm_cust_info ci    -- ci as alias
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 cl
ON ci.cst_key = cl.cid
) t GROUP BY cst_id HAVING COUNT(*) > 1


-- DATA INTEGRATION Customer table
SELECT DISTINCT
ci.cst_gndr,
ca.gen,
CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr    -- CRM is the master of gender info
	 ELSE COALESCE(ca.gen, 'n/a')                   -- COALESCE Function to replace NULL values with n/a
END AS new_gen
FROM silver.crm_cust_info ci    -- ci as alias
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 cl
ON ci.cst_key = cl.cid 
ORDER BY 1,2

SELECT * FROM silver.crm_cust_info;
SELECT * FROM silver.erp_cust_az12;
SELECT * FROM silver.erp_loc_a101;