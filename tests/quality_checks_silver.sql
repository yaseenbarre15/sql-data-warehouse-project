/* ========================================================================================================

Quality Checks

========================================================================================================
Script Purpose:

This script was designed to account for various data quality, accuracy and standardization issues across the 'silver' schema. 
It includes a check for:
1) Unwanted Spaces in string fields,
2) Nulls or Duplicate entries
3)Invalid Date Ranges and Orders
4)Data consistency between fields

Usage Notes: Run these checks after data loading Silver layer
            Investigate and resolve any discrepancies found during checks
            Apply any table or column to these formulas depending on the need. 
========================================================================================================
*/





-- CHECKING FOR UNWANTED SPACES .. NEEDS TO HAVE NO RESULTS
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) 

--DATA STANDARDIZATION & CONSISTENCY
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

--CHECK FOR DUPLICATES
SELECT 
cst_id,
COUNT (*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT (*) > 1 OR cst_id IS NULL 


--Check for NULLS or Negative Numbers
SELECT prd_cost
FROM silver.crm_product_info
WHERE prd_cost <0 OR prd_cost IS NULL

--Check for Invalid Date Orders
SELECT *
FROM silver.crm_product_info
WHERE prd_end_dt < prd_start_dt

--Check for Invalid Dates
SELECT 
NULLIF(sls_due_dt, 0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
OR LEN(sls_due_dt) != 8
OR sls_due_dt >20500101
OR sls_due_dt <19000101

--Check for Invalid Date Orders
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
--order date must always be smaller than shipping or due date. 

--Check Data Consistency; Between Sales, Quantity, and Price
--> Sales = Quantity  * Price
--> Values must NOT be Negative, NULL, or Zero
SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price) 
THEN sls_quantity * ABS(sls_price)
ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0 
THEN sls_sales / NULLIF(sls_quantity, 0)
ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price
