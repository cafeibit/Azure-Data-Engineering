
--Read from the Customer Table by Spark Scala within Databricks Notebooks
--Next, use the Synapse Connector to read data from the Customer Table.
--Use the read to define a tempory table that can be queried.

%%spark
cacheDir = f"wasbs://{containerName}@{storageAccount}.blob.core.windows.net/cacheDir"

tableName = "dbo.DimCustomer"

customerDF = (spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcURI)
  .option("tempDir", cacheDir)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", tableName)
  .load())

customerDF.createOrReplaceTempView("customer_data")

%%sql
select count(*) from customer_data

describe customer_data

select CustomerKey, CustomerAlternateKey 
from customer_data 
limit 10;


--In a situation in which we may be merging many new customers into this table,
--we can imagine that we may have issues with uniqueness with regard to the `CustomerKey`.
--Let us redefine `CustomerAlternateKey` for stronger uniqueness using a [UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier).
--To do this we will define a UDF and use it to transform the `CustomerAlternateKey` column. 
--Once this is done, we will write the updated Customer Table to a Staging table.

%%spark

import uuid

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

uuidUdf = udf(lambda : str(uuid.uuid4()), StringType())
customerUpdatedDF = customerDF.withColumn("CustomerAlternateKey", uuidUdf())
display(customerUpdatedDF)


--Use the Polybase Connector to Write to the Staging Table
%%spark

(customerUpdatedDF.write
  .format("com.databricks.spark.sqldw")
  .mode("overwrite")
  .option("url", jdbcURI)
  .option("forward_spark_azure_storage_credentials", "true")
  .option("dbtable", tableName + "Staging")
  .option("tempdir", cacheDir)
  .save())


--Read and Display Changes from Staging Table

%%spark

customerTempDF = (spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcURI)
  .option("tempDir", cacheDir)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", tableName + "Staging")
  .load())

customerTempDF.createOrReplaceTempView("customer_temp_data")

%%sql
select CustomerKey, CustomerAlternateKey from customer_temp_data limit 10;



--Use data loading best practices in Azure Synapse Analytics 
-- Simplify ingestion with the Copy Activity

-- Replace YOURACCOUNT with the name of your ADLS Gen2 account.

CREATE EXTERNAL DATA SOURCE ABSS
WITH
( TYPE = HADOOP,
    LOCATION = 'abfss://wwi-02@YOURACCOUNT.dfs.core.windows.net'
);

CREATE EXTERNAL FILE FORMAT [ParquetFormat]
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)
GO

-- To create the external file format and external data table.

CREATE SCHEMA [wwi_external];
GO

CREATE EXTERNAL TABLE [wwi_external].Sales
    (
        [TransactionId] [nvarchar](36)  NOT NULL,
        [CustomerId] [int]  NOT NULL,
        [ProductId] [smallint]  NOT NULL,
        [Quantity] [smallint]  NOT NULL,
        [Price] [decimal](9,2)  NOT NULL,
        [TotalAmount] [decimal](9,2)  NOT NULL,
        [TransactionDate] [int]  NOT NULL,
        [ProfitAmount] [decimal](9,2)  NOT NULL,
        [Hour] [tinyint]  NOT NULL,
        [Minute] [tinyint]  NOT NULL,
        [StoreId] [smallint]  NOT NULL
    )
WITH
    (
        LOCATION = '/sale-small%2FYear%3D2019',  
        DATA_SOURCE = ABSS,
        FILE_FORMAT = [ParquetFormat]  
    )  
GO

--

INSERT INTO [wwi_staging].[SaleHeap]
SELECT *
FROM [wwi_external].[Sales]

-- Simplify ingestion with the COPY activity

TRUNCATE TABLE wwi_staging.SaleHeap;
GO

-- Replace YOURACCOUNT with the workspace default storage account name.
COPY INTO wwi_staging.SaleHeap
FROM 'https://YOURACCOUNT.dfs.core.windows.net/wwi-02/sale-small%2FYear%3D2019'
WITH (
    FILE_TYPE = 'PARQUET',
    COMPRESSION = 'SNAPPY'
)
GO

SELECT COUNT(1) FROM wwi_staging.SaleHeap(nolock)

-- load data from a public storage account. 
-- Here the COPY statement's defaults match the format of the line item csv file.

COPY INTO dbo.[lineitem] 
FROM 'https://unsecureaccount.blob.core.windows.net/customerdatasets/folder1/lineitem.csv'

-- This example loads files specifying a column list with default values.

--Note when specifying the column list, input field numbers start from 1
COPY INTO test_1 (Col_one default 'myStringDefault' 1, Col_two default 1 3)
FROM 'https://myaccount.blob.core.windows.net/myblobcontainer/folder1/'
WITH (
    FILE_TYPE = 'CSV',
    CREDENTIAL=(IDENTITY= 'Storage Account Key', SECRET='<Your_Account_Key>'),
	--CREDENTIAL should look something like this:
    --CREDENTIAL=(IDENTITY= 'Storage Account Key', SECRET='x6RWv4It5F2msnjelv3H4DA80n0PQW0daPdw43jM0nyetx4c6CpDkdj3986DX5AHFMIf/YN4y6kkCnU8lb+Wx0Pj+6MDw=='),
    FIELDQUOTE = '"',
    FIELDTERMINATOR=',',
    ROWTERMINATOR='0x0A',
    ENCODING = 'UTF8',
    FIRSTROW = 2
)

-- The following example loads files that use the line feed as a row terminator such as a UNIX output. 
-- This example also uses a SAS key to authenticate to Azure blob storage.

COPY INTO test_1
FROM 'https://myaccount.blob.core.windows.net/myblobcontainer/folder1/'
WITH (
    FILE_TYPE = 'CSV',
    CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='<Your_SAS_Token>'),
	--CREDENTIAL should look something like this:
    --CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='?sv=2018-03-28&ss=bfqt&srt=sco&sp=rl&st=2016-10-17T20%3A14%3A55Z&se=2021-10-18T20%3A19%3A00Z&sig=IEoOdmeYnE9%2FKiJDSHFSYsz4AkNa%2F%2BTx61FuQ%2FfKHefqoBE%3D'),
    FIELDQUOTE = '"',
    FIELDTERMINATOR=';',
    ROWTERMINATOR='0X0A',
    ENCODING = 'UTF8',
    DATEFORMAT = 'ymd',
	MAXERRORS = 10,
	ERRORFILE = '/errorsfolder',--path starting from the storage container
	IDENTITY_INSERT = 'ON'
)

-- The data has the following fields: Date, NorthAmerica, SouthAmerica, Europe, Africa, and Asia. 
-- They must process this data and store it in Synapse Analytics.

CREATE TABLE [wwi_staging].DailySalesCounts
    (
        [Date] [int]  NOT NULL,
        [NorthAmerica] [int]  NOT NULL,
        [SouthAmerica] [int]  NOT NULL,
        [Europe] [int]  NOT NULL,
        [Africa] [int]  NOT NULL,
        [Asia] [int]  NOT NULL
    )
GO

-- Replace <PrimaryStorage> with the workspace default storage account name.
COPY INTO wwi_staging.DailySalesCounts
FROM 'https://YOURACCOUNT.dfs.core.windows.net/wwi-02/campaign-analytics/dailycounts.txt'
WITH (
    FILE_TYPE = 'CSV',
    FIELDTERMINATOR='.',
    ROWTERMINATOR=','
)
GO

SELECT * FROM [wwi_staging].DailySalesCounts
ORDER BY [Date] DESC

-- Attempt to load using PolyBase
-- The row delimiter in delimited-text files must be supported by Hadoop's LineRecordReader. 
-- That is, it must be either \r, \n, or \r\n. These delimiters are not user-configurable.

This is an example of where COPY's flexibility gives it an advantage over PolyBase.

CREATE EXTERNAL FILE FORMAT csv_dailysales
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = '.',
        DATE_FORMAT = '',
        USE_TYPE_DEFAULT = False
    )
);
GO

CREATE EXTERNAL TABLE [wwi_external].DailySalesCounts
    (
        [Date] [int]  NOT NULL,
        [NorthAmerica] [int]  NOT NULL,
        [SouthAmerica] [int]  NOT NULL,
        [Europe] [int]  NOT NULL,
        [Africa] [int]  NOT NULL,
        [Asia] [int]  NOT NULL
    )
WITH
    (
        LOCATION = '/campaign-analytics/dailycounts.txt',  
        DATA_SOURCE = ABSS,
        FILE_FORMAT = csv_dailysales
    )  
GO
INSERT INTO [wwi_staging].[DailySalesCounts]
SELECT *
FROM [wwi_external].[DailySalesCounts]



--Set-up dedicated data load accounts within Azure Synapse Analytics to
--optimize load performance and maintain concurrency as required 
--by managing the available resource slots available within the dedicated SQL Pool.

-- Connect to master
CREATE LOGIN loader WITH PASSWORD = 'a123STRONGpassword!';

-- Connect to the SQL pool
CREATE USER loader FOR LOGIN loader;
GRANT ADMINISTER DATABASE BULK OPERATIONS TO loader;
GRANT INSERT ON <yourtablename> TO loader;
GRANT SELECT ON <yourtablename> TO loader;
GRANT CREATE TABLE TO loader;
GRANT ALTER ON SCHEMA::dbo TO loader;

CREATE WORKLOAD GROUP DataLoads
WITH ( 
    MIN_PERCENTAGE_RESOURCE = 100
    ,CAP_PERCENTAGE_RESOURCE = 100
    ,REQUEST_MIN_RESOURCE_GRANT_PERCENT = 100
    );

CREATE WORKLOAD CLASSIFIER [wgcELTLogin]
WITH (
        WORKLOAD_GROUP = 'DataLoads'
    ,MEMBERNAME = 'loader'
);

--Exercise - implement workload management
--Create a workload classifier to add importance to certain queries
--Lab 08 - Execute Data Analyst and CEO Queries
----First, let's confirm that there are no queries currently being run by users logged in workload01 or workload02

SELECT s.login_name, r.[Status], r.Importance, submit_time, 
start_time ,s.session_id FROM sys.dm_pdw_exec_sessions s 
JOIN sys.dm_pdw_exec_requests r ON s.session_id = r.session_id
WHERE s.login_name IN ('asa.sql.workload01','asa.sql.workload02') and Importance
is not NULL AND r.[status] in ('Running','Suspended') 
--and submit_time>dateadd(minute,-2,getdate())
ORDER BY submit_time ,s.login_name

--To see what happened to all the queries we just triggered as they flood the system.

SELECT s.login_name, r.[Status], r.Importance, submit_time, start_time ,s.session_id FROM sys.dm_pdw_exec_sessions s 
JOIN sys.dm_pdw_exec_requests r ON s.session_id = r.session_id
WHERE s.login_name IN ('asa.sql.workload01','asa.sql.workload02') and Importance
is not NULL AND r.[status] in ('Running','Suspended') and submit_time>dateadd(minute,-2,getdate())
ORDER BY submit_time ,status

--To give our asa.sql.workload01 user queries priority by implementing the Workload Importance feature.

IF EXISTS (SELECT * FROM sys.workload_management_workload_classifiers WHERE name = 'CEO')
BEGIN
    DROP WORKLOAD CLASSIFIER CEO;
END
CREATE WORKLOAD CLASSIFIER CEO
  WITH (WORKLOAD_GROUP = 'largerc'
  ,MEMBERNAME = 'asa.sql.workload01',IMPORTANCE = High);

--

IF EXISTS (SELECT * FROM sys.workload_management_workload_classifiers WHERE name = 'CEO')
BEGIN
    DROP WORKLOAD CLASSIFIER CEO;
END
CREATE WORKLOAD CLASSIFIER CEO
  WITH (WORKLOAD_GROUP = 'largerc'
  ,MEMBERNAME = 'asa.sql.workload01',IMPORTANCE = High);

--

SELECT s.login_name, r.[Status], r.Importance, submit_time, start_time ,s.session_id FROM sys.dm_pdw_exec_sessions s 
JOIN sys.dm_pdw_exec_requests r ON s.session_id = r.session_id
WHERE s.login_name IN ('asa.sql.workload01','asa.sql.workload02') and Importance
is not NULL AND r.[status] in ('Running','Suspended') and submit_time>dateadd(minute,-2,getdate())
ORDER BY submit_time ,status desc

--Reserve resources for specific workloads through workload isolation

IF NOT EXISTS (SELECT * FROM sys.workload_management_workload_groups where name = 'CEODemo')
BEGIN
    Create WORKLOAD GROUP CEODemo WITH  
    ( MIN_PERCENTAGE_RESOURCE = 50        -- integer value
    ,REQUEST_MIN_RESOURCE_GRANT_PERCENT = 25 --  
    ,CAP_PERCENTAGE_RESOURCE = 100
    )
END

--

IF NOT EXISTS (SELECT * FROM sys.workload_management_workload_classifiers where  name = 'CEODreamDemo')
BEGIN
    Create Workload Classifier CEODreamDemo with
    ( Workload_Group ='CEODemo',MemberName='asa.sql.workload02',IMPORTANCE = BELOW_NORMAL);
END

--

SELECT s.login_name, r.[Status], r.Importance, submit_time,
start_time ,s.session_id FROM sys.dm_pdw_exec_sessions s
JOIN sys.dm_pdw_exec_requests r ON s.session_id = r.session_id
WHERE s.login_name IN ('asa.sql.workload02') and Importance
is not NULL AND r.[status] in ('Running','Suspended')
ORDER BY submit_time, status

--

SELECT s.login_name, r.[Status], r.Importance, submit_time,
start_time ,s.session_id FROM sys.dm_pdw_exec_sessions s
JOIN sys.dm_pdw_exec_requests r ON s.session_id = r.session_id
WHERE s.login_name IN ('asa.sql.workload02') and Importance
is not NULL AND r.[status] in ('Running','Suspended')
ORDER BY submit_time, status

--IF  EXISTS (SELECT * FROM sys.workload_management_workload_classifiers where group_name = 'CEODemo')
BEGIN
    Drop Workload Classifier CEODreamDemo
    DROP WORKLOAD GROUP CEODemo
    --- Creates a workload group 'CEODemo'.
        Create  WORKLOAD GROUP CEODemo WITH  
    (MIN_PERCENTAGE_RESOURCE = 26 -- integer value
        ,REQUEST_MIN_RESOURCE_GRANT_PERCENT = 3.25 -- factor of 26 (guaranteed more than 8 concurrencies)
    ,CAP_PERCENTAGE_RESOURCE = 100
    )
    --- Creates a workload Classifier 'CEODreamDemo'.
    Create Workload Classifier CEODreamDemo with
    (Workload_Group ='CEODemo',MemberName='asa.sql.workload02',IMPORTANCE = BELOW_NORMAL);
END

--

SELECT s.login_name, r.[Status], r.Importance, submit_time,
start_time ,s.session_id FROM sys.dm_pdw_exec_sessions s
JOIN sys.dm_pdw_exec_requests r ON s.session_id = r.session_id
WHERE s.login_name IN ('asa.sql.workload02') and Importance
is  not NULL AND r.[status] in ('Running','Suspended')
ORDER BY submit_time, status


--Monitor query execution

-- Monitor active queries
SELECT *
FROM sys.dm_pdw_exec_requests
WHERE status not in ('Completed','Failed','Cancelled')
  AND session_id <> session_id()
ORDER BY submit_time DESC;

-- Find top 10 queries longest running queries
SELECT TOP 10 *
FROM sys.dm_pdw_exec_requests
ORDER BY total_elapsed_time DESC;

-- Query with Label
SELECT *
FROM sys.tables
OPTION (LABEL = 'My Query')
;

-- Find a query with the Label 'My Query'
-- Use brackets when querying the label column, as it it a key word
SELECT  *
FROM    sys.dm_pdw_exec_requests
WHERE   [label] = 'My Query';

-- Find the distribution run times for a SQL step.
-- Replace request_id and step_index with values from Step 1 and 3.

SELECT * FROM sys.dm_pdw_sql_requests
WHERE request_id = 'QID####' AND step_index = 2;

-- Find the SQL Server execution plan for a query running on a specific SQL pool or control node.
-- Replace distribution_id and spid with values from previous query.

DBCC PDW_SHOWEXECUTIONPLAN(1, 78);

-- Find information about all the workers completing a Data Movement Step.
-- Replace request_id and step_index with values from Step 1 and 3.

SELECT * FROM sys.dm_pdw_dms_workers
WHERE request_id = 'QID####' AND step_index = 2;

-- Find the SQL Server estimated plan for a query running on a specific SQL pool Compute or control node.
-- Replace distribution_id and spid with values from previous query.

DBCC PDW_SHOWEXECUTIONPLAN(55, 238);


--Column level security in Azure Synapse Analytics

GRANT <permission> [ ,...n ] ON
    [ OBJECT :: ][ schema_name ]. object_name [ ( column [ ,...n ] ) ] // specifying the column access
    TO <database_principal> [ ,...n ]
    [ WITH GRANT OPTION ]
    [ AS <database_principal> ]
<permission> ::=
    SELECT
  | UPDATE
<database_principal> ::=
      Database_user // specifying the database user
    | Database_role // specifying the database role 
    | Database_user_mapped_to_Windows_User
    | Database_user_mapped_to_Windows_Group
    
    --Example
    
    CREATE TABLE Membership
  (MemberID int IDENTITY,
   FirstName varchar(100) NULL,
   SSN char(9) NOT NULL,
   LastName varchar(100) NOT NULL,
   Phone varchar(12) NULL,
   Email varchar(100) NULL);
   
   GRANT SELECT ON Membership(MemberID, FirstName, LastName, Phone, Email) TO TestUser;
   
   SELECT * FROM Membership;

-- Msg 230, Level 14, State 1, Line 12
-- The SELECT permission was denied on the column 'SSN' of the object 'Membership', database 'CLS_TestDW', schema 'dbo'.

--run in master
CREATE LOGIN Manager WITH PASSWORD = '<user_password>'
GO
CREATE LOGIN Sales1 WITH PASSWORD = '<user_password>'
GO
CREATE LOGIN Sales2 WITH PASSWORD = '<user_password>'
GO

--run in master and your SQL pool database
CREATE USER Manager FOR LOGIN Manager;  
CREATE USER Sales1  FOR LOGIN Sales1;  
CREATE USER Sales2  FOR LOGIN Sales2 ;

CREATE TABLE Sales  
    (  
    OrderID int,  
    SalesRep sysname,  
    Product varchar(10),  
    Qty int  
    );
    
    INSERT INTO Sales VALUES (1, 'Sales1', 'Valve', 5);
INSERT INTO Sales VALUES (2, 'Sales1', 'Wheel', 2);
INSERT INTO Sales VALUES (3, 'Sales1', 'Valve', 4);
INSERT INTO Sales VALUES (4, 'Sales2', 'Bracket', 2);
INSERT INTO Sales VALUES (5, 'Sales2', 'Wheel', 5);
INSERT INTO Sales VALUES (6, 'Sales2', 'Seat', 5);
-- View the 6 rows in the table  
SELECT * FROM Sales;

CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<user_password>';

CREATE DATABASE SCOPED CREDENTIAL msi_cred WITH IDENTITY = 'Managed Service Identity';

CREATE EXTERNAL DATA SOURCE ext_datasource_with_abfss WITH (TYPE = hadoop, LOCATION = 'abfss://<file_system_name@storage_account>.dfs.core.windows.net', CREDENTIAL = msi_cred);

CREATE EXTERNAL FILE FORMAT MSIFormat  WITH (FORMAT_TYPE=DELIMITEDTEXT);
  
CREATE EXTERNAL TABLE Sales_ext WITH (LOCATION='<your_table_name>', DATA_SOURCE=ext_datasource_with_abfss, FILE_FORMAT=MSIFormat, REJECT_TYPE=Percentage, REJECT_SAMPLE_VALUE=100, REJECT_VALUE=100)
AS SELECT * FROM sales;

GRANT SELECT ON Sales_ext TO Sales1;  
GRANT SELECT ON Sales_ext TO Sales2;  
GRANT SELECT ON Sales_ext TO Manager;

CREATE SCHEMA Security;  
GO  
  
CREATE FUNCTION Security.fn_securitypredicate(@SalesRep AS sysname)  
    RETURNS TABLE  
WITH SCHEMABINDING  
AS  
    RETURN SELECT 1 AS fn_securitypredicate_result
WHERE @SalesRep = USER_NAME() OR USER_NAME() = 'Manager';

CREATE SECURITY POLICY SalesFilter_ext
ADD FILTER PREDICATE Security.fn_securitypredicate(SalesRep)
ON dbo.Sales_ext  
WITH (STATE = ON);

SELECT * FROM Sales_ext;

ALTER SECURITY POLICY SalesFilter_ext  
WITH (STATE = OFF);

DROP USER Sales1;
DROP USER Sales2;
DROP USER Manager;

DROP SECURITY POLICY SalesFilter_ext;
DROP TABLE Sales;
DROP EXTERNAL TABLE Sales_ext;
DROP EXTERNAL DATA SOURCE ext_datasource_with_abfss ;
DROP EXTERNAL FILE FORMAT MSIFormat;
DROP DATABASE SCOPED CREDENTIAL msi_cred; 
DROP MASTER KEY;

DROP LOGIN Sales1;
DROP LOGIN Sales2;
DROP LOGIN Manager;
