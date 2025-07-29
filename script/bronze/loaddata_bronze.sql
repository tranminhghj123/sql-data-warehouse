/*
Inseting all data from requiremented csv file into 
corresponding tables by using COPY method

===============================
================================
Warning: +) this query will automatically elminate all 
data from all table in bronze schema and replaces them
with the original ones from cvs file.
+) before runing query, moving all the requirement csv file
to same folder with postgreSQL.

*/


CREATE OR REPLACE PROCEDURE load_all_bronze_data()
LANGUAGE plpgsql
AS $$
declare 
	start_time date;
	end_time date;
	start_time_batch date;
	end_time_batch date;
BEGIN
    BEGIN
		start_time_batch := now();
        
		raise notice '===============';
		raise notice 'loading bronze layer';
		raise notice '===============';

		raise notice 'loading CRM table';
		raise notice '===============';
		
		-- Customer Info
		start_time := now();
		raise notice 'truncate all table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
		raise notice 'importing data into bronze.crm_cust_info';
        COPY bronze.crm_cust_info(
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        FROM '/Library/PostgreSQL/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
        WITH (
            FORMAT csv,
            HEADER true,
            DELIMITER ','
        );
		end_time := now();
		raise notice 'load durtion in second: %',end_time - start_time;
		raise notice '=======================';
		
        -- Product Info
		start_time := now();
		raise notice 'truncate table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
		raise notice 'importing data into bronze.crm_prd_info';
        COPY bronze.crm_prd_info(
            prd_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        FROM '/Library/PostgreSQL/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
        WITH (
            FORMAT csv,
            HEADER true,
            DELIMITER ','
        );

		end_time := now();
		raise notice 'load durtion in second: %',end_time - start_time;
		raise notice '=======================';

        -- Sales Details
		start_time := now();
		raise notice ' truncate table: bronze.crm_sales_details ';
        TRUNCATE TABLE bronze.crm_sales_details;
		raise notice ' truncate table: bronze.crm_sales_details ';
        COPY bronze.crm_sales_details(
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
        FROM '/Library/PostgreSQL/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
        WITH (
            FORMAT csv,
            HEADER true,
            DELIMITER ','
        );
		end_time := now();
		raise notice 'load durtion in second: %',end_time - start_time;
		raise notice '=======================';

		raise notice '==================';
		raise notice 'Loading ERP tables';
		raise notice '==================';
		
        -- ERP Customer AZ12
		start_time := now();
		raise notice 'truncate table: bronze.erp_cust_az12';
		raise notice 'importing data for bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        COPY bronze.erp_cust_az12(
            cid,
            bdate,
            gen
        )
        FROM '/Library/PostgreSQL/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
        WITH (
            FORMAT csv,
            HEADER true,
            DELIMITER ','
        );

		end_time := now();
		raise notice 'load durtion in second: %',end_time - start_time;
		raise notice '=======================';

        -- ERP Location A101
		start_time := now();
		raise notice 'truncate table: bronze.erp_loc_a101';
		raise notice 'importind data for bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        COPY bronze.erp_loc_a101(
            cid,
            cntry
        )
        FROM '/Library/PostgreSQL/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
        WITH (
            FORMAT csv,
            HEADER true,
            DELIMITER ','
        );
		end_time := now();
		raise notice 'load durtion in second: %',end_time - start_time;
		raise notice '=======================';

        -- ERP PX Cat G1V2
		start_time := now();
		
		raise notice 'truncate table:bronze.erp_px_cat_g1v2';
		raise notice 'import data from bronze.erp_px_cat_g1v2';
		
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        COPY bronze.erp_px_cat_g1v2(
            id,
            cat,
            subcat,
            maintenance
        )
        FROM '/Library/PostgreSQL/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
        WITH (
            FORMAT csv,
            HEADER true,
            DELIMITER ','
        );

		end_time := now();
		raise notice 'load durtion in second: %',end_time - start_time;
		raise notice '=======================';

		end_time_batch := now();
		raise notice 'duration for whole process in second: %', end_time_batch - start_time_batch;
		
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Failed loading bronze layer: %', SQLERRM;
    END;
END;
$$;
