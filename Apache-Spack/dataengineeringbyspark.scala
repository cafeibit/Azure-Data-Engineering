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

#1. PySpark contains a special explode function, which returns a new row for each element of the array. 
#The new row will help flatten the topProductPurchases column for better readability or for easier querying. 
#Execute the code below in a new cell:

from pyspark.sql.functions import udf, explode

flat=df.select('visitorId',explode('topProductPurchases').alias('topProductPurchases_flat'))
flat.show(100)

#In this cell, we created a new DataFrame named flat that includes the visitorId field and a new aliased field named topProductPurchases_flat. 
#As you can see, the output is a bit easier to read and, by extension, easier to query.

#2. Create a new cell and execute the following code to create a new flattened version of the DataFrame that extracts 
#the topProductPurchases_flat.productId and topProductPurchases_flat.itemsPurchasedLast12Months fields to create new rows for each data combination:

topPurchases = (flat.select('visitorId','topProductPurchases_flat.productId','topProductPurchases_flat.itemsPurchasedLast12Months')
    .orderBy('visitorId'))

topPurchases.show(100)

If we want to use the Apache Spark pool to Synapse SQL connector (sqlanalytics), 
one option is to create a temporary view of the data within the DataFrame

# Create a temporary view for top purchases 
topPurchases.createOrReplaceTempView("top_purchases")

*/

// Make sure the name of the SQL pool (SQLPool01 below) matches the name of your SQL pool.
val df = spark.sqlContext.sql("select * from top_purchases")
df.write.sqlanalytics("SQLPool01.wwi.TopPurchases", Constants.INTERNAL)

/**
As you can see, the wwi.TopPurchases table was automatically created for us, 
based on the derived schema of the Apache Spark DataFrame. 
The Apache Spark pool to Synapse SQL connector was responsible for creating the table and efficiently loading the data into it.
%%PySpark
dfsales = spark.read.load('abfss://wwi-02@' + datalake + '.dfs.core.windows.net/sale-small/Year=2019/Quarter=Q4/Month=12/*/*.parquet', format='parquet')
display(dfsales.limit(10))
*/

/**
Next, let's load the TopSales data from the SQL pool table we created earlier into a new Apache Spark DataFrame,
then join it with this new dfsales DataFrame. 
To do so, we must once again use the %%spark magic on a new cell 
since we'll use the Apache Spark pool to Synapse SQL connector to retrieve data from the SQL pool. 
Then we need to add the DataFrame contents to a new temporary view so we can access the data from Python.
*/

// Make sure the name of the SQL pool (SQLPool01 below) matches the name of your SQL pool.
val df2 = spark.read.sqlanalytics("SQLPool01.wwi.TopPurchases")
df2.createTempView("top_purchases_sql")

df2.head(10)

/**
The cell's language is set to Scala by using the %%spark magic (1) at the top of the cell.
We declared a new variable named df2 as a new DataFrame created by the spark.read.sqlanalytics method, 
which reads from the TopPurchases table (2) in the SQL pool. 
Then we populated a new temporary view named top_purchases_sql (3). 
Finally, we showed the first 10 records with the df2.head(10)) line (4). 
The cell output displays the DataFrame values (5).

Execute the code below in a new cell to create a new DataFrame in Python from the top_purchases_sql temporary view, 
then display the first 10 results:

dfTopPurchasesFromSql = sqlContext.table("top_purchases_sql")
display(dfTopPurchasesFromSql.limit(10))

#Execute the code below in a new cell to join the data from the sales Parquet files and the TopPurchases SQL pool:

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

In the query, we joined the dfsales and dfTopPurchasesFromSql DataFrames, matching on CustomerId and ProductId. 
This join combined the TopPurchases SQL pool table data with the December 2019 sales Parquet data (1).
We grouped by the CustomerId and ProductId fields. Since the ProductId field name is ambiguous (it exists in both DataFrames), 
we had to fully qualify the ProductId name to refer to the one in the TopPurchases DataFrame (2).
Then we created an aggregate that summed the total amount spent on each product in December, 
the total number of product items in December, and the total product items purchased in the last 12 months (3).
Finally, we displayed the joined and aggregated data in a table view.
*/

/**
Transfer data to and from a dedicated SQL pool which outside the synapse workspace using the PySpark connector
Let's say that you have created or loaded a DataFrame called "pyspark_df", 
and then assume that you want to write that DataFrame into the data warehouse. How would you go about that task?
The first thing to do is to create a temporary table in a DataFrame in PySpark using the createOrReplaceTempView method

pyspark_df.createOrReplaceTempView("pysparkdftemptable")

The parameter that is passed through is the temporary table name, which in this case is called: "pysparkdftemptable". 
We are still using the pyspark_df DataFrame as you can see in the beginning of the statement. 
Next, you would have to run a Scala cell in the PySpark notebook using magics (since we're using different languages, and it will only work in Scala):

By using "val scala_df", we create a fixed value for the scala_dataframe, and then use the statement "select * from pysparkdftemptable", 
that returns all the data that we created in the temporary table in the previous step, and storing it in a table named sqlpool.dbo.PySparkTable

Should you wish to read data using the PySpark connector, keep in mind that you read the data using scala first, 
then write it into a temporary table. Finally you use the Spark SQL in PySpark to query the temporary table into a DataFrame.
*/

val scala_df = spark.sqlContext.sql ("select * from pysparkdftemptable")
scala_df.write.sqlanalytics("sqlpool.dbo.PySparkTable", Constants.INTERNAL)

