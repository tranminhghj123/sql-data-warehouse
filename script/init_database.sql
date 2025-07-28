/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/



-- Drop and recreate the 'DataWarehouse' database
DO
$$
BEGIN
   IF EXISTS (
      SELECT 1 FROM pg_database WHERE datname = 'datawarehouse'
   ) THEN
      -- Terminate all other connections to the database
      REVOKE CONNECT ON DATABASE datawarehouse FROM public;
      SELECT pg_terminate_backend(pg_stat_activity.pid)
      FROM pg_stat_activity
      WHERE pg_stat_activity.datname = 'datawarehouse'
        AND pid <> pg_backend_pid();

      DROP DATABASE datawarehouse;
   END IF;
END
$$;

-- Create the 'DataWarehouse' database
CREATE DATABASE datawarehouse;


-- Create Schemas
drop schema if exists bronze;
CREATE SCHEMA bronze;

drop schema if exists silver;
CREATE SCHEMA silver;

drop schema if exists gold;
CREATE SCHEMA gold;
