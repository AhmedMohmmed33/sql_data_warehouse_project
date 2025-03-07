/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
        DECLARE @start_time DATE, @end_time DATE, @batch_start_time DATE, @batch_end_time DATE
  	BEGIN TRY
    	        SET @batch_start_time = GETDATE();
    		PRINT '===============================================';
    		PRINT 'Loading The Bronze Layer';
    		PRINT '===============================================';
    
    		PRINT '-----------------------------------------------';
    		PRINT 'Loading CRM Tables';
    		PRINT '-----------------------------------------------';
    
    		SET @start_time = GETDATE();
    		PRINT '>>Truncating Table: bronze.crm_cust_info';
    		TRUNCATE TABLE bronze.crm_cust_info;
    
    		PRINT '>>Inserting Data Into: bronze.crm_cust_info';
    		BULK INSERT bronze.crm_cust_info
    		FROM 'D:\SQL_DWH\Project\datasets\source_crm\cust_info.csv'
    		WITH(
    			FIRSTROW = 2,
    			FIELDTERMINATOR = ',',
    			TABLOCK
    		);
    		SET @end_time = GETDATE();
    		PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds';
    		PRINT '------------------------';
    
    		SET @start_time = GETDATE();
    		PRINT '>>Truncating Table: bronze.crm_prd_info';
    		TRUNCATE TABLE bronze.crm_prd_info;
    
    		PRINT '>>Inserting Data Into: bronze.crm_prd_info';
    		BULK INSERT bronze.crm_prd_info
    		FROM 'D:\SQL_DWH\Project\datasets\source_crm\prd_info.csv'
    		WITH(
    			FIRSTROW = 2,
    			FIELDTERMINATOR = ',',
    			TABLOCK
    		);
    		SET @end_time = GETDATE();
    		PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds';
    		PRINT '------------------------';
    
    		SET @start_time = GETDATE();
    		PRINT '>>Truncating Table: bronze.crm_sales_details';
    		TRUNCATE TABLE bronze.crm_sales_details;
    
    		PRINT '>>Inserting Data Into: bronze.crm_sales_details';
    		BULK INSERT bronze.crm_sales_details
    		FROM 'D:\SQL_DWH\Project\datasets\source_crm\sales_details.csv'
    		WITH(
    			FIRSTROW = 2,
    			FIELDTERMINATOR = ',',
    			TABLOCK
    		);
    		SET @end_time = GETDATE();
    		PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds';
    		PRINT '------------------------';
    
    		PRINT '-----------------------------------------------';
    		PRINT 'Loading ERP Tables';
    		PRINT '-----------------------------------------------';
    
    		SET @start_time = GETDATE();
    		PRINT '>>Truncating Table: bronze.erp_cust_az12';
    		TRUNCATE TABLE bronze.erp_cust_az12;
    
    		PRINT 'Insert Data Into: bronze.erp_cust_az12';
    		BULK INSERT bronze.erp_cust_az12
    		FROM 'D:\SQL_DWH\Project\datasets\source_erp\CUST_AZ12.csv'
    		WITH(
    			FIRSTROW = 2,
    			FIELDTERMINATOR = ',',
    			TABLOCK
    		);
    		SET @end_time = GETDATE();
    		PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds';
    		PRINT '------------------------';
    
    		SET @start_time = GETDATE();
    		PRINT '>>Truncating Table: bronze.erp_loc_a101';
    		TRUNCATE TABLE bronze.erp_loc_a101;
    
    		PRINT 'Insert Data Into: bronze.erp_loc_a101';
    		BULK INSERT bronze.erp_loc_a101
    		FROM 'D:\SQL_DWH\Project\datasets\source_erp\LOC_A101.csv'
    		WITH(
    			FIRSTROW = 2,
    			FIELDTERMINATOR = ',',
    			TABLOCK
    		);
    		SET @end_time = GETDATE();
    		PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds';
    		PRINT '------------------------';
    
    		SET @start_time = GETDATE();
    		PRINT '>>Truncating Table: bronze.erp_px_cat_g1v2';
    		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    
    		PRINT 'Insert Data Into: bronze.erp_px_cat_g1v2';
    		BULK INSERT bronze.erp_px_cat_g1v2
    		FROM 'D:\SQL_DWH\Project\datasets\source_erp\PX_CAT_G1V2.csv'
    		WITH(
    			FIRSTROW = 2,
    			FIELDTERMINATOR = ',',
    			TABLOCK
    		);
    		SET @end_time = GETDATE();
    		PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds';
    		PRINT '------------------------';
    
    		SET @batch_end_time = GETDATE();
    		PRINT '=================================================';
    		PRINT 'Loading Bronze Layer is Completed';
    		PRINT '  -Total Load Duration: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS VARCHAR) + ' seconds';
    		PRINT '=================================================';
  	END TRY
  	BEGIN CATCH
    		PRINT '================================================';
    		PRINT 'Error Occured During Loading Bronze Layer';
    		PRINT '================================================';
    		PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
    		PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS VARCHAR);
    		PRINT 'ERROR STATE: ' + CAST(ERROR_STATE() AS VARCHAR);
  	END CATCH
END
