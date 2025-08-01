/*
Goal: 
+) Applying for testing result on gold layer
+) Inspecting errors in the connection between Fact table 
and dim_customer and dim_product(2 other dimension)
+) Ensuring all the customer and product in fact table
matching with other dimension.
*/

-- finding the missing connection to dim_customer
SELECT COUNT(*) AS missing_customer_join
FROM gold.fact_sales
WHERE customer_id IS NULL; -- return 0

--finding the missing conection to dim_prodcuts
SELECT COUNT(*) AS missing_product_join
FROM gold.fact_sales
WHERE product_key IS NULL; -- return 0

-- Sales that failed to match a customer
SELECT *
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customer dc ON fs.customer_id = dc.customer_id
WHERE dc.customer_id IS NULL;-- return  none 
