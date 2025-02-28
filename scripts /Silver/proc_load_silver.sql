/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATE, @end_time DATE, @batch_start_time DATE, @batch_end_time DATE
	BEGIN TRY
		SET @batch_start_time = GETDATE();
    		PRINT '===============================================';
    		PRINT 'Loading The Silver Layer';
    		PRINT '===============================================';
		
		PRINT '-----------------------------------------------';
    		PRINT 'Loading CRM Tables';
    		PRINT '-----------------------------------------------';
		SET @start_time = GETDATE();
		PRINT 'Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>>Insert Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normlize marital status values to readable format
			CASE
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				ELSE 'n/a'
			END AS cst_gndr,  -- Normlize gender values to readable format
			cst_create_date
		FROM(
			SELECT 
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t where flag_last = 1;  -- Select the most recent records per customer
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '-----------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>>Insert Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract Category ID
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,		   -- Extract Product key
			prd_nm,
			COALESCE(prd_cost, 0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				 WHEN 'M' THEN 'Mountain'
				 WHEN 'R' THEN 'Road'
				 WHEN 'T' THEN 'Touring'
				 WHEN 'S' THEN 'Other Sales'
				 ELSE 'n/a'
			END AS prd_line, -- Map product line codes to descriptive values
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				 LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1
				 AS DATE
			) AS prd_end_dt -- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '-----------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>>Insert Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE WHEN sls_sales != sls_quantity * ABS(sls_price) OR sls_sales IS NULL OR sls_sales <= 0
					THEN sls_quantity * ABS(sls_price)
				 ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0
					THEN sls_sales/NULLIF(sls_quantity,0)
				 ELSE sls_price
			END AS sls_price -- Derive price if original value is invalid
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '-----------------------------------';

		PRINT '-----------------------------------------------';
    		PRINT 'Loading ERP Tables';
    		PRINT '-----------------------------------------------';
		SET @start_time = GETDATE();
		PRINT 'Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>>Insert Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12
			(cid, bdate, gen)
		SELECT 
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
			END AS cid, -- Remove 'NAS' prefix if present
			CASE WHEN bdate > GETDATE() THEN NULL
				 ELSE bdate
			END AS bdate, -- Set future birthdates to null
			CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				 ELSE 'n/a'
			END AS gen -- Normaize gender values and handle unknown cases
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '-----------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>>Insert Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101
			(cid,cntry)
		SELECT 
			REPLACE(cid, '-', '') AS cid,
			CASE WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
				 WHEN UPPER(TRIM(cntry)) ='DE' THEN 'Germany'
				 WHEN cntry IS NULL OR TRIM(cntry) = ''  THEN 'n/a'
				 ELSE TRIM(cntry)
			END AS cnrty -- Normalize ans Handle missing or blank country code
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '-----------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>>Insert Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2
			(id, cat, subcat, maintenance)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '-----------------------------------';

		SET @batch_end_time = GETDATE();
    		PRINT '=================================================';
    		PRINT 'Loading Silver Layer is Completed';
    		PRINT '  -Total Load Duration: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS VARCHAR) + ' seconds';
    		PRINT '=================================================';
	END TRY
	BEGIN CATCH
		PRINT '================================================';
		PRINT 'Error Occured During Loading Silver Layer';
		PRINT '================================================';
		PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT 'ERROR STATE: ' + CAST(ERROR_STATE() AS VARCHAR);
	END CATCH
END
