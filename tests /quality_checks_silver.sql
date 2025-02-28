/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ============================================
-- Checking: "crm_cust_info"
-- ============================================
-- Check For Nulls or Duplicates in Primary key
-- Expectation : No Result
SELECT 
  cst_id,
  count(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1 or cst_id is null;

-- Check Unwanted Spaces
-- Expectation : No Result
SELECT
  cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Data Standardization & Consistency
SELECT DISTINCT
  cst_gndr
FROM silver.crm_cust_info;

-- ============================================
-- Checking: "crm_prd_info"
-- ============================================  
-- Check For Nulls or Duplicates in Primary key
-- Expectation : No Result 
SELECT
  prd_id,
  COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check Unwanted Spaces
-- Expectation : No Result
SELECT
  prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for Nulls or Negative Numbers
SELECT
  prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Checg for Invaild Data Orders
SELECT 
  prd_id,
  prd_nm,
  prd_cost,
  prd_start_dt,
  prd_end_dt
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

-- ============================================
-- Checking: "crm_sales_details"
-- ============================================
-- Check for invalid date orders
Select
  *
FROM silver.crm_sales_details
where sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Check data consistency: between sales, quantity, and price
--> sales = quantity * price
--> values must not be nulls, zeros, or negative
SELECT
  sls_sales,
  sls_quantity,
  sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL 
   OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales,sls_quantity,sls_price;

-- ============================================
-- Checking: "erp_cust_az12"
-- ============================================
-- Check for unwanted spaces
-- Expectation : no result
SELECT 
  cid
FROM silver.erp_cust_az12
WHERE cid != TRIM(cid);

-- Identify out_of_rang bdate
SELECT 
  bdate
FROM silver.erp_cust_az12
WHERE bdate >= GETDATE();

-- Data standardization & consistency
SELECT DISTINCT
  gen
FROM silver.erp_cust_az12;

-- ============================================
-- Checking: "erp_loc_a101"
-- ============================================
-- Check for unwanted spaces
-- Expectation : no result
SELECT
  cid
FROM silver.erp_loc_a101
WHERE cid != TRIM(cid) OR cntry != TRIM(cntry);

-- Data standardization & consistency
SELECT DISTINCT
  cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ============================================
-- Checking: "erp_px_cat_g1v2"
-- ============================================
-- Check for unwanted spaces
-- Expectation : no result
SELECT
  id,
  cat,
  subcat,
  maintenance
FROM silver.erp_px_cat_g1v2
WHERE id != TRIM(id) OR cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-- Data standardization & consistency
SELECT DISTINCT
  maintenance
FROM silver.erp_px_cat_g1v2;
