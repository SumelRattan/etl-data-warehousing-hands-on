INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)
-- DATA CLEANING STEPS / PRE-PROCESSING
SELECT 
cst_id, 
cst_key, 
TRIM(cst_firstname) AS cst_firstname, --removing unwanted spaces
TRIM(cst_lastname) AS cst_lastname,	  --removing unwanted spaces

CASE 
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' -- data normalization/standardization 
	WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'  -- Normalize marital_status to readable format 
	ELSE 'n/a'												  -- handling null values
END cst_marital_status,

CASE 
	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'	-- data normalization/standardization 
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'	-- Normalize gender to readable format
	ELSE 'n/a'										-- handling null values
END cst_gndr,
cst_create_date
FROM (												-- removing duplicates 
	SELECT *,							
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
) t 
WHERE flag_last = 1;								-- data filtering 

--WHERE cst_id = 29466;


-- Check for unwanted spaces 



---Standardization and consistency 
/* CASE
	WHEN cst_marital_status = 'M' THEN 'Married'
	WHEN cst_marital_status = 'S' THEN 'Single'
	ELSE 'n/a' 
END cst_marital_status

SELECT * FROM bronze.crm_cust_info; */

SELECT * FROM silver.crm_cust_info