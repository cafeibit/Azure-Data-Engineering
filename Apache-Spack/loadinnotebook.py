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

