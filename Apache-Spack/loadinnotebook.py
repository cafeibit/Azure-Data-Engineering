#To read a CSV file from an Azure Data Lake Store Gen2 store as an Apache Spark DataFrame.

from pyspark.sql import SparkSession
from pyspark.sql.types import *
account_name = "Your account name"
container_name = "Your container name"
relative_path = "Your path"
adls_path = 'abfss://%s@%s.dfs.core.windows.net/%s' % (container_name, account_name, relative_path)

spark.conf.set("fs.azure.account.auth.type.%s.dfs.core.windows.net" %account_name, "SharedKey")
spark.conf.set("fs.azure.account.key.%s.dfs.core.windows.net" %account_name ,"Your ADLS Gen2 Primary Key")

df1 = spark.read.option('header', 'true') \
                .option('delimiter', ',') \
                .csv(adls_path + '/Testfile.csv')


#To read a CSV file from Azure Storage Account as an Apache Spark DataFrame.

from pyspark.sql import SparkSession
from pyspark.sql.types import *

blob_account_name = "Your blob account name"
blob_container_name = "Your blob container name"
blob_relative_path = "Your blob relative path"
blob_sas_token = "Your blob sas token"

wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token)

df = spark.read.option("header", "true") \
            .option("delimiter","|") \
            .schema(schema) \
            .csv(wasbs_path)


#Ingest and explore Parquet files from a data lake with Apache Spark for Azure Synapse
#We import required Python libraries to use aggregation functions and types defined in the schema to successfully execute the query.
#The output shows the same data we saw in the chart above, but now with sum and avg aggregates (1).
#Notice that we use the alias method (2) to change the column names.

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

profitByDateProduct = (data_path.groupBy("TransactionDate","ProductId")
    .agg(
        sum("ProfitAmount").alias("(sum)ProfitAmount"),
        round(avg("Quantity"), 4).alias("(avg)Quantity"),
        sum("Quantity").alias("(sum)Quantity"))
    .orderBy("TransactionDate"))
display(profitByDateProduct.limit(100))


#Exercise: Load data into a spark dataframe
#You have customer profile data from an e-commerce system that provides top product purchases for each visitor of the site (customer) over the past 12 months. 
#This data is stored within JSON files in the data lake. They have struggled with ingesting, exploring, and transforming these JSON files and want your guidance. 
#The files have a hierarchical structure that they want to flatten before loading into relational data stores. 
#They also wish to apply grouping and aggregate operations as part of the data engineering process. 
#You recommend using Azure Synapse notebooks to explore and apply data transformations on the JSON files.

#This exercise will focus on how to load data into an Apache Spark DataFrame.

#1. Create a new cell in the Apache Spark notebook, add the following code beneath the code in the cell to define a variable named datalake
#whose value is the name of the primary storage account (replace the REPLACE_WITH_YOUR_DATALAKE_NAME value with the name of the storage account):

datalake = 'REPLACE_WITH_YOUR_DATALAKE_NAME'

#2. Create a new cell in the Apache Spark notebook, enter the code below and execute the cell:

df = (spark.read \
        .option('inferSchema', 'true') \
        .json('abfss://wwi-02@' + datalake + '.dfs.core.windows.net/online-user-profiles-02/*.json', multiLine=True)
    )

df.printSchema()

#Note: The datalake variable we created in the first cell is used here as part of the file path. 
#Notice that we are selecting all JSON files within the online-user-profiles-02 directory. 
#Each JSON file contains several rows, which is why we specified the multiLine=True option. 
#Also, we set the inferSchema option to true, which instructs the Apache Spark engine to review the files and create a schema based on the nature of the data.

#3. We have been using Python code in these cells up to this point. 
#If we want to query the files using SQL syntax, one option is to create a temporary view of the data within the DataFrame. 
#Execute the code below in a new cell to create a view named user_profiles:

# create a view called user_profiles
df.createOrReplaceTempView("user_profiles")

#Create a new cell. Since we want to use SQL instead of Python, 
#we use the %%sql magic to set the language of the cell to SQL. Execute the below code in the cell:

%%sql

SELECT * FROM user_profiles LIMIT 10

#Notice that the output shows nested data for topProductPurchases, which includes an array of productId and itemsPurchasedLast12Months values. 
#You can expand the fields by clicking the right triangle in each row.
#The JSON nested output makes analyzing the data a bit difficult. It is because the JSON file contents looks as follows:

#Exercise: Create a spark table
#1. The Apache Spark engine can analyze the Parquet files and infer the schema. 
#To do this analysis, enter the below code in the new cell and run it:

data_path.printSchema()

#2. Now let's use the DataFrame to use aggregates and grouping operations to better understand the data. 
#Create a new cell and enter the following, then run the cell:

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

profitByDateProduct = (data_path.groupBy("TransactionDate","ProductId")
    .agg(
        sum("ProfitAmount").alias("(sum)ProfitAmount"),
        round(avg("Quantity"), 4).alias("(avg)Quantity"),
        sum("Quantity").alias("(sum)Quantity"))
    .orderBy("TransactionDate"))
display(profitByDateProduct.limit(100))


#Exercise: Flatten nested structures and explode arrays with Apache Spark in synapse
#In this exercise, you will learn how to work with complex data structure and use functions to view data more easily

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

#Note: In the output, notice that we now have multiple rows for each visitorId.

#3. Let's order the rows by the number of items purchased in the last 12 months. Create a new cell and execute the following code:

# Let's order by the number of items purchased in the last 12 months
sortedTopPurchases = topPurchases.orderBy("itemsPurchasedLast12Months")

display(sortedTopPurchases.limit(100))

#4. How do we sort in reverse order? You might conclude that we could make a call like this:
#topPurchases.orderBy("itemsPurchasedLast12Months desc")
#Notice that there is an AnalysisException error, because itemsPurchasedLast12Months desc does not match up with a column name.

#5. The Column class is an object that encompasses more than just the name of the column, 
#but also column-level-transformations, such as sorting in a descending order. Execute the following code in a new cell:

sortedTopPurchases = (topPurchases
    .orderBy( col("itemsPurchasedLast12Months").desc() ))

display(sortedTopPurchases.limit(100))

#Notice that the results are now sorted by the itemsPurchasedLast12Months column in descending order, 
#thanks to the desc() method on the col object.

#6. How many types of products did each customer purchase? To find the answer, we need to group by visitorId and aggregate on the number of rows per customer. 
#Execute the following code in a new cell:

groupedTopPurchases = (sortedTopPurchases.select("visitorId")
    .groupBy("visitorId")
    .agg(count("*").alias("total"))
    .orderBy("visitorId") )

display(groupedTopPurchases.limit(100))

#Notice how we use the groupBy method on the visitorId column, and the agg method over a count of records to display the total for each customer.

#7. How many total items did each customer purchase? To find the answer, we need to group by visitorId 
#and aggregate on the sum of itemsPurchasedLast12Months values per customer. 
#Execute the following code in a new cell:

groupedTopPurchases = (sortedTopPurchases.select("visitorId","itemsPurchasedLast12Months")
    .groupBy("visitorId")
    .agg(sum("itemsPurchasedLast12Months").alias("totalItemsPurchased"))
    .orderBy("visitorId") )

groupedTopPurchases.show(100)

#Here we group by visitorId once again, but now we use a sum over the itemsPurchasedLast12Months column in the agg method. 
#Notice that we included the itemsPurchasedLast12Months column in the select statement so we could use it in the sum.


