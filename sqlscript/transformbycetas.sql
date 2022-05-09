--CREATE EXTERNAL TABLE AS SELECT in serverless SQL pool
--When using Azure Synapse serverless SQL pool, CREATE EXTERNAL TABLE AS SELECT is used to create an external table, 
--which exports the query results to Azure Storage Blob or Azure Data Lake Storage Gen2, as shown in the following syntax.

CREATE EXTERNAL TABLE [ [database_name  . [ schema_name ] . ] | schema_name . ] table_name
    WITH (
        LOCATION = 'path_to_folder',  
        DATA_SOURCE = external_data_source_name,  
        FILE_FORMAT = external_file_format_name  
)
    AS <select_statement>  
[;]

<select_statement> ::=  
    [ WITH <common_table_expression> [ ,...n ] ]  
    SELECT <select_criteria>
    
--The ORDER BY clause in SELECT part of the statement is not supported for CETAS. 
--You need to have permissions to list folder content and write to the LOCATION folder for CETAS to work.    

-- you might need to execute following statement if you never did so on current database
-- CREATE MASTER KEY

CREATE DATABASE SCOPED CREDENTIAL destination_credential
WITH IDENTITY='SHARED ACCESS SIGNATURE',  
	SECRET = '' -- fill in your SAS key that will be used for CETAS destination
GO

CREATE EXTERNAL DATA SOURCE destination_ds
WITH
(    
	LOCATION         = 'https://<storage>.<dfs>.core.windows.net/<container>' – replace path to match root path for your CETAS destination
     , CREDENTIAL = destination_credential
)
GO

CREATE EXTERNAL FILE FORMAT parquet_file_format
WITH
(  
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)
GO

-- use CETAS to export select statement with OPENROWSET result to  storage
CREATE EXTERNAL TABLE aggregated_data
WITH (
    LOCATION = 'aggregated_data/',
    DATA_SOURCE = destination_ds,  
    FILE_FORMAT = parquet_file_format
)  
AS
SELECT decennialTime, stateName, SUM(population) AS population
FROM
    OPENROWSET(BULK 'https://azureopendatastorage.blob.core.windows.net/censusdatacontainer/release/us_population_county/year=*/*.parquet',
    FORMAT='PARQUET') AS [r]
GROUP BY decennialTime, stateName
GO

-- you can query the newly created external table
SELECT * FROM aggregated_data
