--Authenticate in Azure Synapse Analytics
--To connect to the dedicated SQL pool database from which you want transfer data to or from

--SQL Authenticated User
CREATE USER Leo FROM LOGIN Leo;

--Azure Active Directory User
CREATE USER [chuck@contoso.com] FROM EXTERNAL PROVIDER;

--SQL Authenticated user
EXEC sp_addrolemember 'db_exporter', 'Leo';
--Azure Active Directory User
EXEC sp_addrolemember 'db_exporter',[chuck@contoso.com]


--Transfer data between SQL and spark pool in Azure Synapse Analytics
--Import statements are only required when you don't go through the integrated notebook experience.
--It is important that the Constants and the SqlAnalyticsConnector are set up as shown below:

#scala
import com.microsoft.spark.sqlanalytics.utils.Constants
import org.apache.spark.sql.SqlAnalyticsConnector._
 
--To read data from a dedicated SQL pool, you should use the Read API. 
--The Read API works for Internal tables (Managed Tables) and External Tables in the dedicated SQL pool.

val df = spark.read.sqlanalytics("<DBName>.<Schema>.<TableName>")

--To write data to a dedicated SQL Pool, you should use the Write API.
--The Write API creates a table in the dedicated SQL pool. 
--Then, it invokes Polybase to load the data into the table that was created. 
--One thing to keep in mind is that the table can't already exist in the dedicated SQL pool.
--The Write API using Azure Active Directory (Azure AD) looks as follows:

df.write.sqlanalytics("<DBName>.<Schema>.<TableName>", <TableType>)

--The parameters it takes in are:
--DBName: the name of the database.
--Schema: the schema definition such as dbo.
--TableName: the name of the table you want to read data from.
--TableType: specification of the type of table, which can have two values.
--Constants.INTERNAL - Managed table in dedicated SQL pool
--Constants.EXTERNAL - External table in dedicated SQL pool

--To use a SQL pool external table, you need to have an EXTERNAL DATA SOURCE 
--and an EXTERNAL FILE FORMAT that exists on the pool using the following examples:

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

--It is not necessary to create an EXTERNAL CREDENTIAL object if you are using Azure AD pass-through authentication from the storage account. 
--The only thing you need to keep in mind is that you need to be a member of the "Storage Blob Data Contributor" role on the storage account.
--The next step is to use the df.write command within Scala with DATA_SOURCE, FILE_FORMAT, 
--and the sqlanalytics command in a similar way to writing data to an internal table.

#scala
df.write.
    option(Constants.DATA_SOURCE, <DataSourceName>).
    option(Constants.FILE_FORMAT, <FileFormatName>).
    sqlanalytics("<DBName>.<Schema>.<TableName>", Constants.EXTERNAL)
	
	
--Authenticate between spark and SQL pool in Azure Synapse Analytics
--using SQL Authentication, instead of Azure Active Directory (Azure AD) 
--with the Azure Synapse Apache Spark Pool to Synapse SQL connector.

val df = spark.read.
option(Constants.SERVER, "samplews.database.windows.net").
option(Constants.USER, <SQLServer Login UserName>).
option(Constants.PASSWORD, <SQLServer Login Password>).
sqlanalytics("<DBName>.<Schema>.<TableName>")

--In order to write data to a dedicated SQL Pool, you use the Write API. 
--The Write API creates the table in the dedicated SQL pool, 
--and then uses Polybase to load the data into the table that was created.

df.write.
option(Constants.SERVER, "samplews.database.windows.net").
option(Constants.USER, <SQLServer Login UserName>).
option(Constants.PASSWORD, <SQLServer Login Password>).
sqlanalytics("<DBName>.<Schema>.<TableName>", <TableType>)

--To write to a dedicated SQL pool after performing data engineering tasks in Spark, 
--then reference the SQL pool data as a source for joining with Apache Spark DataFrames that contain data from other files.

--The Azure Synapse Apache Spark pool to Synapse SQL connector is a data source implementation for Apache Spark. 
--It uses the Azure Data Lake Storage Gen2 and PolyBase in SQL pools to efficiently transfer data 
--between the Spark cluster and the Synapse SQL instance.

#PySpark

# Create a temporary view for top purchases 
topPurchases.createOrReplaceTempView("top_purchases")

%%spark
// Make sure the name of the SQL pool (SQLPool01 below) matches the name of your SQL pool.
val df = spark.sqlContext.sql("select * from top_purchases")
df.write.sqlanalytics("SQLPool01.wwi.TopPurchases", Constants.INTERNAL)

--Return to the notebook and execute the code below in a new cell to read sales data 
--from all the Parquet files located in the sale-small/Year=2019/Quarter=Q4/Month=12/ folder:

#PySpark
dfsales = spark.read.load('abfss://wwi-02@' + datalake + '.dfs.core.windows.net/sale-small/Year=2019/Quarter=Q4/Month=12/*/*.parquet', format='parquet')
display(dfsales.limit(10))

--To read from the TopSales SQL pool table and save it to a temporary view:

%%spark
// Make sure the name of the SQL pool (SQLPool01 below) matches the name of your SQL pool.
val df2 = spark.read.sqlanalytics("SQLPool01.wwi.TopPurchases")
df2.createTempView("top_purchases_sql")

df2.head(10)

--To create a new DataFrame in Python from the top_purchases_sql temporary view, then display the first 10 results:

dfTopPurchasesFromSql = sqlContext.table("top_purchases_sql")

display(dfTopPurchasesFromSql.limit(10))

--To join the data from the sales Parquet files and the TopPurchases SQL pool:

#PySpark
inner_join = dfsales.join(dfTopPurchasesFromSql,
    (dfsales.CustomerId == dfTopPurchasesFromSql.visitorId) & (dfsales.ProductId == dfTopPurchasesFromSql.productId))

inner_join_agg = (inner_join.select("CustomerId","TotalAmount","Quantity","itemsPurchasedLast12Months","top_purchases_sql.productId")
    .groupBy(["CustomerId","top_purchases_sql.productId"])
    .agg(
        sum("TotalAmount").alias("TotalAmountDecember"),
        sum("Quantity").alias("TotalQuantityDecember"),
        sum("itemsPurchasedLast12Months").alias("TotalItemsPurchasedLast12Months"))
    .orderBy("CustomerId") )

display(inner_join_agg.limit(100))


--To allow other users to use the Azure Synapse Apache Spark to Synapse SQL connector in your Azure Synapse workspace.
--Transfer data outside the synapse workspace using the PySpark connector

--You can transfer data to and from a dedicated SQL pool using a Pyspark Connector, which currently works with Scala.

--Let's say that you have created or loaded a DataFrame called "pyspark_df", 
--and then assume that you want to write that DataFrame into the data warehouse. How would you go about that task?
--The first thing to do is to create a temporary table in a DataFrame in PySpark using the createOrReplaceTempView method

pyspark_df.createOrReplaceTempView("pysparkdftemptable")

%%spark
val scala_df = spark.sqlContext.sql ("select * from pysparkdftemptable")
scala_df.write.sqlanalytics("sqlpool.dbo.PySparkTable", Constants.INTERNAL)

--By using "val scala_df", we create a fixed value for the scala_dataframe, and then use the statement "select * from pysparkdftemptable", 
--that returns all the data that we created in the temporary table in the previous step, and storing it in a table named sqlpool.dbo.PySparkTable

--In the second line of the code, we specified following parameters:
--DBName: The database name that in the example above is named sqlpool
--Schema: The schema name that in the example above is named dbo
--TableName: The table name that in the example above is named PySparkTable
--TableType: Specifies the type of table, which has the value Constants.INTERNAL, which related to a managed table in the dedicated SQL pool.
-Should you wish to read data using the PySpark connector, keep in mind that you read the data using scala first, then write it into a temporary table. 
--Finally you use the Spark SQL in PySpark to query the temporary table into a DataFrame.
