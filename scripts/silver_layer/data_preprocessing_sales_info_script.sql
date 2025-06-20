IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prod_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prod_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity, 
	sls_price
)
SELECT sls_ord_num
      ,sls_prod_key
      ,sls_cust_id,
      CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	  END AS sls_order_dt,
	   CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	   END AS sls_ship_dt,
      CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	  END AS sls_due_dt,
      CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)         -- ABS() converts negatives to positives
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	  END AS sls_sales,
	  sls_quantity,
	 CASE WHEN sls_price IS NULL OR sls_price <=0 
		THEN sls_sales/NULLIF(sls_quantity, 0)
		ELSE sls_price
	 END AS sls_price
  FROM bronze.crm_sales_details




  -- Preprocessing Checks

  -- Check for invalid dates
  SELECT 
  NULLIF(sls_order_dt, 0) sls_order_dt
  FROM bronze.crm_sales_details
  WHERE sls_order_dt <= 0 
  OR LEN(sls_order_dt) != 8 
  OR sls_order_dt > 20500101 
  OR sls_order_dt < 19000101

  SELECT * FROM bronze.crm_sales_details
  WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt


  --- Busines rule 
  /* 
  Sales = Quantity * Price
  Negative, zeros, Nulls are not Allowed!
  */

  SELECT DISTINCT
  sls_sales,
  sls_quantity,
  sls_price
  FROM bronze.crm_sales_details
  WHERE 
  sls_sales != sls_quantity * sls_price
  OR sls_sales IS NULL OR sls_quantity IS NULL or sls_price IS NULL
  OR sls_sales <=0 OR sls_quantity <= 0 OR sls_price <= 0 
  ORDER BY sls_sales, sls_quantity,sls_price

  -- Rules 
  -- If Sales is negative, zero or null, derive it using Quanity and Price 
  -- If Price is zero or null, derive it using Sales and Quantity
  -- If Price is negative, convert it into positive value

  SELECT DISTINCT
  sls_sales AS old_sales,
  sls_quantity,
  sls_price AS old_price,

  CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)         -- ABS() converts negatives to positives
	THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
  END AS sls_sales,

  CASE WHEN sls_price IS NULL OR sls_price <=0 
	THEN sls_sales/NULLIF(sls_quantity, 0)
	ELSE sls_price
  END AS sls_price



  FROM bronze.crm_sales_details
  WHERE 
  sls_sales != sls_quantity * sls_price
  OR sls_sales IS NULL OR sls_quantity IS NULL or sls_price IS NULL
  OR sls_sales <=0 OR sls_quantity <= 0 OR sls_price <= 0 
  ORDER BY sls_sales, sls_quantity,sls_price


  SELECT * FROM silver.crm_sales_details