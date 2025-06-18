-- This script creates a new database named DataWarehouse and sets up schemas for bronze, silver, and gold layers
--  without checking if it already exists.


USE master;

CREATE DATABASE DataWarehouse;

USE DataWarehouse;

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO 
CREATE SCHEMA gold;
