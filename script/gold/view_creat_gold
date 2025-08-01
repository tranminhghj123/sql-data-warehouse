/*
======================================================
Purpose: 
+) Creating a data model(star schema) in the gold layer 
for business viewers and analyzing attempting 
+) Each view is a combination of data from silver layers
+) each view could be query directly
=======================================================

*/

-- create customer dimension vies
create or REPLACE view gold.dim_customer as (
select 
	row_number() over(order by ci.cst_id) as customer_key,
	ci.cst_id as customer_id, -- foreign key
	ci.cst_key as customer_number,
	ci.cst_firstname as firstname,
	ci.cst_lastname as lastname,
	ci.cst_marital_status martial_status,
	ca.bdate  as birthdate,
	case 
		when ci.cst_gndr != 'n/a' then ci.cst_gndr
		else coalesce(ca.gen, 'n/a')
	end as gender,
	la.cntry as country,
	ci.cst_create_date as createdate
	
from silver.crm_cust_info as ci

left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid

left join silver.erp_loc_a101 la 
on ci.cst_key = la.cid);

-- create products dimension view
create or replace view gold.dim_products as(
select 
	row_number() over(order by pi.prd_start_dt) as product_key,
	pi.prd_id AS product_id,
	pi.prd_key as product_number, -- foreign key
	pi.prd_nm asproduct_name,
	pi.cat_id category_id,	
	cg.cat as category,
	cg.subcat as subcategory,
	cg.maintenance,
	pi.prd_cost as product_cost,
	pi.prd_line as product_line,
	pi.prd_start_dt as start_date
from silver.crm_prd_info as pi

left join silver.erp_px_cat_g1v2 as cg
on pi.cat_id = cg.id

where pi.prd_end_dt is null); -- filtering the current information of products

-- create fact view
create or replace view gold.fact_sales as (
select 
	cd.sls_ord_num as order_number,
	gp.product_key,
	gc.customer_id,
	cd.sls_order_dt as orderdate,
	cd.sls_ship_dt as shipdate,
	cd.sls_due_dt as duedate,
	cd.sls_sales as sales,
	cd.sls_quantity as quantity,
	cd.sls_price as price
	

from silver.crm_sales_details as cd

left join gold.dim_products as gp
on cd.sls_prd_key = gp.product_number

left join gold.dim_customer as gc
on cd.sls_cust_id = gc.customer_id);



