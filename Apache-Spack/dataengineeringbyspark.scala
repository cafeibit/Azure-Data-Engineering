//To use Spark's scala to work on data enginnering tasks

import com.microsoft.spark.sqlanalytics.utils.Constants
import org.apache.spark.sql.SqlAnalyticsConnector._

/** 
By using Azure Active Directory to transfer data to and from an Apache Spark pool 
and a dedicated SQL pool attached within the workspace you have created for your Azure Synapse Analytics account.
To read data from a dedicated SQL pool, by using the Read API. 
The Read API works for Internal tables (Managed Tables) and External Tables in the dedicated SQL pool.
*/

val df = spark.read.sqlanalytics("<DBName>.<Schema>.<TableName>")

/**
To write data to a dedicated SQL Pool by using the Write API. 
The Write API creates a table in the dedicated SQL pool. 
Then, it invokes Polybase to load the data into the table that was created. 
*/

df.write.sqlanalytics("<DBName>.<Schema>.<TableName>", <TableType>)

/**
To use a SQL pool external table, you need to have an EXTERNAL DATA SOURCE and an EXTERNAL FILE FORMAT that exists on the pool.

--For an external table, you need to pre-create the data source and file format in dedicated SQL pool using SQL queries:
CREATE EXTERNAL DATA SOURCE <DataSourceName>
WITH
  ( LOCATION = 'abfss://...' ,
    TYPE = HADOOP
  ) ;

CREATE EXTERNAL FILE FORMAT <FileFormatName>
WITH (  
    FORMAT_TYPE = PARQUET,  
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'  
);

*/

/**
It is not necessary to create an EXTERNAL CREDENTIAL object 
if you are using Azure AD pass-through authentication from the storage account.
Need to be a member of the "Storage Blob Data Contributor" role on the storage account. 
The next step is to use the df.write command within Scala with DATA_SOURCE, FILE_FORMAT, 
and the sqlanalytics command in a similar way to writing data to an internal table.
*/

df.write.
    option(Constants.DATA_SOURCE, <DataSourceName>).
    option(Constants.FILE_FORMAT, <FileFormatName>).
    sqlanalytics("<DBName>.<Schema>.<TableName>", Constants.EXTERNAL)


/**
In order to establish and transfer data to a dedicated SQL pool 
that is outside of the workspace without Azure AD, you would have to use SQL Authentication.
To read data from a dedicated SQL pool outside your workspace without Azure AD, you use the Read API. 
The Read API works for Internal tables (Managed Tables) and External Tables in the dedicated SQL pool.
*/

val df = spark.read.
option(Constants.SERVER, "samplews.database.windows.net").
option(Constants.USER, <SQLServer Login UserName>).
option(Constants.PASSWORD, <SQLServer Login Password>).
sqlanalytics("<DBName>.<Schema>.<TableName>")

/**
In order to write data to a dedicated SQL Pool, you use the Write API. 
The Write API creates the table in the dedicated SQL pool, 
and then uses Polybase to load the data into the table that was created.
*/

df.write.
option(Constants.SERVER, "samplews.database.windows.net").
option(Constants.USER, <SQLServer Login UserName>).
option(Constants.PASSWORD, <SQLServer Login Password>).
sqlanalytics("<DBName>.<Schema>.<TableName>", <TableType>)

/**
Exercise: Integrate SQL and spark pools in Azure Synapse Analytics

The Azure Synapse Apache Spark pool to Synapse SQL connector is a data source implementation for Apache Spark. 
It uses the Azure Data Lake Storage Gen2 and PolyBase in SQL pools 
to efficiently transfer data between the Spark cluster and the Synapse SQL instance.

If we want to use the Apache Spark pool to Synapse SQL connector (sqlanalytics), 
one option is to create a temporary view of the data within the DataFrame

# Create a temporary view for top purchases 
topPurchases.createOrReplaceTempView("top_purchases")

*/
// Make sure the name of the SQL pool (SQLPool01 below) matches the name of your SQL pool.
val df = spark.sqlContext.sql("select * from top_purchases")
df.write.sqlanalytics("SQLPool01.wwi.TopPurchases", Constants.INTERNAL)

