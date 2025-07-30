/*
Goal: 
	+) Cleaning the data from bronze layers tables for matching 
	the drawn relationship sketched between different tables
	+) Inserting cleaned data into silver layers
===============================================
===============================================
WARNING:
	+) Running this querry will automatically eliminate all 
	the data in silver layer and replace them with the original
	cleaned data from bronze layers tables.
*/
-- insert cleaned data into crm_cust_info_silver

CREATE or replace PROCEDURE load_all_silver_data()
LANGUAGE plpgsql
as $$
DECLARE
	start_time date;
	end_time date;
	start_time_batch date;
	end_time_batch date;
BEGIN
	begin
		raise notice '==========================';
		raise notice 'load data for silver layer';
		raise notice '==========================';
	
		raise notice 'inserting data into table crm_cust_info_silver';
		start_time_batch := now();
		start_time := now();
		truncate silver.crm_cust_info;
		Insert INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)
		select 
			cst_id,
			cst_key,
			trim(cst_firstname) as cst_firstname,
			trim(cst_lastname) as cst_lastname,
			case lower(trim(cst_marital_status))
				when 's' then 'single' 
				when 'm' then 'married'
				else 'n/a'
			end cst_marital_status ,
			case 
				when lower(trim(cst_gndr)) = 'f' then 'female'
				when lower(trim(cst_gndr)) = 'm' then 'male'
				else 'n/a'
			end cst_gndr,
			cst_create_date
		from(select 
			*,
			row_number() over(partition by cst_id 
			order by cst_create_date DESC) as flag_last
		
		from bronze.crm_cust_info)t
		where flag_last =1;
		end_time := now();
		raise notice 'time for inserting process in second:%', end_time - start_time;
		raise notice '=====================================';
	
		
		-- insert cleaned data into crm_prd_info_silver
		raise notice 'inserting data into table crm_prd_info_silver';
		start_time := now();
		truncate silver.crm_prd_info;
		insert into silver.crm_prd_info(
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
			-- seperate categeory id into categoey ID and product key
			replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
			SUBSTRING(prd_key,7,length(prd_key)) as prd_key,
			prd_nm,
			
			coalesce(prd_cost, 0) as prd_cost,
			
			-- rename syntax for product line
			case Upper(trim(prd_line))
				when 'M' then 'mountain'
				WHEN 'R' then 'road'
				when 'S' then 'other_sales'
				when 'T' then 'touring'
				ELSE 'n/a'
		
			end as prd_line,
		
			-- correct the relationship betwwen start and end date
			prd_start_dt,
			lead(prd_start_dt) over(partition by prd_key
			order by prd_start_dt) -1 as prd_end_dt
		
		from bronze.crm_prd_info;
		end_time := now();
		raise notice 'time for inserting process in second:%', end_time - start_time;
		raise notice '=====================================';
	
	
		
		-- insert cleaned data into crm_sales_details_silver
		raise notice 'insertinf data into crm_sales_details_silver';
		start_time := now();
		TRUNCATE silver.crm_sales_details;
		
		insert into silver.crm_sales_details(
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
			-- correct the type of order, ship, due_date data
			case 
				when sls_order_dt =0 or length(sls_order_dt :: text) !=8 then null
				else sls_order_dt:: varchar :: date
		
			end sls_order_dt,
			case 
				when sls_ship_dt =0 or length(sls_ship_dt :: text) !=8 then null
				else sls_ship_dt:: varchar :: date
		
			end sls_ship_dt,
			case 
				when sls_due_dt =0 or length(sls_due_dt :: text) !=8 then null
				else sls_due_dt:: varchar :: date
		
			end sls_due_dt,
		
			-- fixing the relationship between sale, price and quantity
			-- removing unrespected data for sale, price data(negative)
			case 
				WHEN sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
				THEN sls_quantity * abs(sls_price)
				else sls_sales
		
			end sls_sales,
			sls_quantity,
			case 
				when sls_price is null or sls_price <= 0 
				THEN sls_sales/ nullif(sls_quantity,0)
				else sls_price
			end sls_price
		
		from bronze.crm_sales_details;
		end_time := now();
		raise notice 'time for inserting process in second:%', end_time - start_time;
		raise notice '=====================================';
		
		-- insert cleaned data into erp_cust_az12_silver
		raise notice 'inserting data into erp_cust_az12_silver';
		start_time := now();
		truncate silver.erp_cust_az12;
		
		insert into silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		select 
			-- eliminate unecessary index for key column
			case 
				when cid like 'NAS%' then substring(cid, 4, length(cid))
				else cid
			end cid,
		
			-- eliminate birthdate that is not realistic
			case 
				when bdate > now() then Null
				else bdate
			end bdate,
		
			case 
				when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
				when upper(trim(gen)) in ('M', 'MALE') then 'Male'
				else 'n/a'
			end gen
		
		from bronze.erp_cust_az12;
		end_time := now();
		raise notice 'time for inserting process in second:%', end_time - start_time;
		raise notice '=====================================';
	
		
		-- insert cleaned data into erp_loc_a101_silver
		raise notice 'inserting data into table erp_loc_a101_silver';
		start_time := now();
		truncate silver.erp_loc_a101;
		
		insert into silver.erp_loc_a101(
			cid,
			cntry
		)
		SELECT
			replace(cid,'-','') as cid,
			case 
				when trim(cntry) = 'DE' then 'Germany'
				when trim(cntry) in ('USA','US') then 'United States'
				when trim(cntry) = '' or trim(cntry) is null then 'n/a'
				else trim(cntry)
			end cntry
		from bronze.erp_loc_a101 ;
		
		-- insert cleaned data into erp_px_cat_g1v2_silver
		truncate silver.erp_px_cat_g1v2;
		insert into silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)
		select 
			id,
			cat,
			subcat,
			maintenance
		
		from bronze.erp_px_cat_g1v2;
		end_time_batch := now();
		end_time:= now();
		raise notice 'time for inserting process in second:%', end_time - start_time;
		raise notice '=====================================';
		raise notice 'time for loading all table in second %', end_time_batch - start_time_batch;
	exception
		when others then
		RAISE NOTICE 'Failed loading bronze layer: %', SQLERRM;
    END;
end;

$$
