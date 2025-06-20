-- Check for NULLS or Duplicates in Primary Key

SELECT cst_id,
COUNT(*) FROM silver.crm_cust_info			-- bronze/silver
GROUP BY cst_id	
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Check for unwanted spaces
SELECT cst_firstname
FROM silver.crm_cust_info			-- bronze/silver
WHERE cst_firstname != TRIM(cst_firstname)

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Check for data inconsistency
SELECT DISTINCT cst_gndr
from silver.crm_cust_info			-- bronze/silver

-- Removing duplicates (Window function)

FROM (												-- removing duplicates 
	SELECT *,							
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
) t 
WHERE flag_last = 1;	

-- Check for NULLS or Negative numbers
SELECT prd_cost
FROM silver.crm_prd_info				-- bronze/silver
WHERE prd_cost < 0 OR prd_cost IS NULL


-- Check for start and end dates
SELECT * FROM silver.crm_prd_info -- bronze/silver
WHERE prd_end_dt < prd_start_dt     

--- End date should always be larger than start date, hence 
--- End date = Start date of the Next record - 1 
--- We can use Lead() window function to access the next record (next date) within our table 

SELECT prd_id,
prd_key,
prd_nm,
prd_start_dt, 
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')