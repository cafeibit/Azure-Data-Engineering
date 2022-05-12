# Data Engineering with Azure Cosmos DB

Azure Cosmos DB is a globally distributed database system that allows you to read and write data from the local replicas of your database and it transparently replicates the data to all the regions associated with your Cosmos account. Azure Cosmos DB analytical store is a fully isolated column store for enabling large-scale analytics against operational data in your Azure Cosmos DB, without any impact to your transactional workloads.

* To implement partitions are organized in Azure Cosmos DB and how to choose an appropriate partition key for your solution.
* To develop client and server-side programming solutions on Azure Cosmos DB.

## Work with Hybrid Transactional and Analytical Processing Solutions using Azure Synapse Analytics

  To perform operational analytics against Azure Cosmos DB using the Azure Synapse Link feature within Azure Synapse Analytic and solves the issue of making operational data available for analytical query in near real time. 

###  Design hybrid transactional and analytical processing using Azure Synapse Analytics

* Many business application architectures separate transactional and analytical processing into separate systems with data stored and processed on separate infrastructures. These infrastructures are commonly referred to as OLTP (online transaction processing) systems working with operational data, and OLAP (online analytical processing) systems working with historical data, with each system is optimized for their specific task.

    *   OLTP systems are optimized for dealing with discrete system or user requests immediately and responding as quickly as possible.

    *   OLAP systems are optimized for the analytical processing, ingesting, synthesizing, and managing large sets of historical data. The data processed by OLAP systems largely originates from OLTP systems and needs to be loaded into the OLAP systems by means of batch processes commonly referred to as ETL (Extract, Transform, and Load) jobs.

* Due to their complexity and the need to physically copy large amounts of data, this creates a delay in data being available to provide insights by way of the OLAP systems.

* As more and more businesses move to digital processes, they increasingly recognize the value of being able to respond to opportunities by making faster and well-informed decisions. HTAP (Hybrid Transactional/Analytical processing) enables business to run advanced analytics in near-real-time on data stored and processed by OLTP systems.

* **Azure Synapse Link for Azure Cosmos DB**

  Azure Synapse Link for Azure Cosmos DB is a cloud-native HTAP capability that enables you to run near-real-time analytics over operational data stored in Azure Cosmos DB. Azure Synapse Link creates a tight seamless integration between Azure Cosmos DB and Azure Synapse Analytics.

    *   Azure Cosmos DB provides both a transactional store optimized for transactional workloads and an analytical store optimized for analytical workloads and a fully managed autosync process to keep the data within these stores in sync.

    *   Azure Synapse Analytics provides both a SQL Serverless query engine for querying the analytical store using familiar T-SQL and an Apache Spark query engine for leveraging the analytical store using your choice of Scala, Java, Python or SQL and provides a user-friendly notebook experience.

    *   Together Azure Cosmos DB and Synapse Analytics enable organizations to generate and consume insights from their operational data in near-real time, using the query and analytics tools of their choice. All of this is achieved without the need for complex ETL pipelines and without affecting the performance of their OLTP systems using Azure Cosmos DB.

    <img src="./synapse-analytics-cosmos-db-architecture.png" />
    
* There are common use cases for using Azure Synapse Link for Azure Cosmos DB capability to address real-world business needs using operational analytics: 

    *   Supply chain analytics, forecasting and reporting.
    *   Retail real-time personalization.
    *   Predictive maintenance using anomaly detection with IOT
   
###  To configure and enable Azure Synapse Link to interact with Azure Cosmos DB. 

* Enable Cosmos DB account to use Azure Synapse Link

  Before we can create an Azure Cosmos DB container with an analytical store, we must first enable Azure Synapse Link on the Azure Cosmos DB account. Today you cannot disable the Synapse Link feature once it is enabled on the account, you cannot disable it. Enabling Synapse Link on the account has no billing implications until containers are created with the analytical store enabled.

* Enabling Synapse Link on Azure Cosmos DB SQL (Core) API account

  *   Navigate to the Azure portal and select the Azure Cosmos DB account.
  *   Navigate to your previously created Azure Cosmos DB SQL (Core) API account.
  *   Select Data Explorer in the left-hand menu (3)
  *   Then click Enable Azure Synapse Link button at the top of the screen (4)
  *   Then click Enable Azure Synapse Link on the pop-up dialog box.
  *   Select Features in the left-hand menu (1)
  *   Verify that the Azure Synapse Link feature shows with a Status of on (2), this will indicate that the Azure Cosmos DB account has been enabled for Azure Synapse Link.

* Enabling Synapse Link on Azure Cosmos DB API for MongoDB account

  *   Navigate to the Azure portal (https://portal.azure.com) and select the Azure Cosmos DB account.
  *   Navigate to your previously created Azure Cosmos DB API for MongoDB account.
  *   Select Features in the left-hand menu (3)
  *   Then click on the Azure Synapse Link entry in the features table (4).
  *   The click the Enable button on the dialog box on the right (5)

* Create an analytical store enabled container

  Before we can query our data using Azure Synapse Analytics using Azure Synapse Link, we must first create the container that is going to hold our data at the same time enabling it to have an analytical store.
  
  *   Create a new Azure Cosmos DB Core (SQL) API container
  *   Load sample data into the Azure Cosmos DB Core (SQL) API container
  *   Create a new Azure Cosmos DB API for MongoDB container
  *   Load sample data into the Azure Cosmos DB API for MongoDB container

* Implement Synapse Link for Cosmos DB

  In order to query the data within our Cosmos DB analytical store from Azure Synapse using Spark, we need to configure a linked service
  
  *   Configure Azure Synapse Linked Service for Azure Cosmos DB Core (SQL) API
  *   Configure Azure Synapse linked service for Azure Cosmos DB API for MongoDB

* Validate connectivity from Spark

  *   Spark Queries for Cosmos DB Core (SQL) API
  *   Spark queries for Azure Cosmos DB API for MongoDB

* Validate connectivity from SQL Serverless

  *   SQL Queries for Cosmos DB Core (SQL) API
  *   SQL Queries for Cosmos DB API for MongoDB


### Query Azure Cosmos DB with Apache Spark for Azure Synapse Analytics 

To perform analytics against Azure Cosmos DB using Azure Synapse Link and to use the Synapse Spark to query the Azure Cosmos DB data made available by Azure Synapse Link.

* Query the Azure Cosmos DB analytical store

  *   Connect to an Azure Synapse Workspace that has an Azure Synapse SQL Serverless instance, and an Azure Synapse Spark Pool.
  *   In the left-hand menu, select Data (A)
  *   Click on the Linked tab in the explorer view (B)
  *   Expand the AdventureWorksSQL linked service to expose the Customer container
  *   Expand the AdventureWorksMongoDB linked service to expose the SalesOrder container.
  *   Let’s open a new notebook to explore what is in these containers with Spark.
  *   In the left-hand menu, select develop (E)
  *   Then click the “+” (F) to add a resource.
  *   And then select Notebook (G) to create a new notebook. A new notebook will immediately be created within the Synapse Workspace.
  *   Within the notebook properties, provide an appropriate name, such as “Cosmos DB Notebook” (H).
  *   And then click the properties icon (I) to close the properties.
  *   Before we can run any spark jobs, we need to select the spark pool we wish to connect to execute our jobs.
  *   Click the attach to dropdown at the top of the notebook (J) and select adventurespark, our previously deployed spark pool.

* Synapse Apache Spark allows you to analyze data in your Azure Cosmos DB containers that are enabled with Azure Synapse Link. The following two options are available to query the Azure Cosmos DB analytical store from Spark:

  *   Load to Spark DataFrame
  *   Create Spark table
  *   Let’s start by creating a DataFrame for each of our two containers and perform a read of the content of the analytical store.
  *   Click into the first cell of the notebook (M)
  *   Paste the following python statements defining the two DataFrames we are going to use and the options for reading the data into them from the respective analytical stores:
  *   `dfCustomer = spark.read\
    .format("cosmos.olap")\
    .option("spark.synapse.linkedService", "AdventureWorksSQL")\
    .option("spark.cosmos.container", "Customer")\
    .load()

dfSalesOrder = spark.read\
    .format("cosmos.olap")\
    .option("spark.synapse.linkedService", "AdventureWorksMongoDB")\
    .option("spark.cosmos.container", "SalesOrder")\
    .load()`
    
    ...
    
* Perform simple aggregations with analytical store data

  Now that we have explored the basic structure of the analytical store, lets dig a little deeper into what this data can tell us about the Adventure Works business.
  
  *   Let’s start by exploring some basic statistics, the simplest being the number of sales orders we have.
  *   `print(dfCustomer.count())`
  *   `display(dfCustomer.groupBy("address.country","address.city").count().orderBy("count",  ascending=False).limit(10))`


* Perform cross container queries in Azure Cosmos DB

  Let’s explore the Adventure Works data in more detail, and see what additional insights we can get by combining the data that is stored in the Azure Cosmos DB Core (SQL) API, and the Azure Cosmos DB API for MongoDB.
  
  *   We are primarily going to use Spark SQL to explore this data, so most code with start with %%sql construct.
  *   To begin, the two options available for querying the Azure Cosmos DB analytical store from Spark include: Loading to Spark DataFrame and Creating Spark table
  *   `%%sql
CREATE TABLE Customers USING cosmos.olap OPTIONS (
    spark.synapse.linkedService 'AdventureWorksSQL',
    spark.cosmos.container 'Customer'
)`
  * `%%sql
CREATE TABLE SalesOrders USING cosmos.olap OPTIONS (
    spark.synapse.linkedService 'AdventureWorksMongoDB',
    spark.cosmos.container 'SalesOrder'
)`
  *   `%%sql
SELECT address.city AS City_Name, address.country AS Country_Name, count(*) as Address_Count 
                            FROM Customers 
                            GROUP BY address.city, address.country 
                            ORDER BY Address_Count DESC 
                            LIMIT 10`
  *   `%%sql
CREATE OR REPLACE TEMPORARY VIEW SalesOrderView
AS
SELECT s._id.string as SalesOrderId, 
        c.id AS CustomerId, c.address.country AS Country, c.address.city AS City, 
        to_date(s.orderdate.string) AS OrderDate, to_date(s.shipdate.string) AS ShipDate
    FROM Customers c 
    INNER JOIN SalesOrders s
        ON c.id = s.CustomerId.string`
        
                            
* Perform complex queries with JSON data

  We now need to unpack the sales order line-item data that is contained within an array embedded in the detail’s column of our SalesOrders table to access the unit and revenue information that it contains. Luckily, Spark SQL has two functions designed to assist us with this problem:

  *   explode(), which separates the elements of array into multiple rows and uses the default column name col for elements of the array.
  *   posexplode(), which separates the elements of array into multiple rows with positions and uses the column name pos for position, col for elements of the array.
  *   `%%sql
SELECT _id.string as SalesOrderId, explode(details.array)
    FROM SalesOrders
    LIMIT 10`
  *   `%%sql
SELECT _id.string as SalesOrderId, posexplode(details.array) 
    FROM SalesOrders
    LIMIT 10`
  *   `%%sql
CREATE OR REPLACE TEMPORARY VIEW SalesOrderDetailsView
AS
    SELECT Ax.SalesOrderId,
        pos+1 as SalesOrderLine,
        col.object.sku.string AS SKUCode,
        col.object.price.float64 AS Price, 
        col.object.quantity.int32 AS Quantity
    FROM 
        (
            SELECT _id.string as SalesOrderId, posexplode(details.array) FROM SalesOrders 
        ) Ax`
  *   `DESCRIBE SalesOrderDetailsView`

As you can see the data types for the SKUCode, Price and Quantity where automatically mapped back to the underlying data types contained within the array without the need to manually infer data types for these columns.

Now that we have our sales order details information in an easy-to-use format, we should easily be able to extract the insights Adventure Works setout to gather. This is the subject of the next unit.

* Work with the windowing function and other aggregations

  Adventure Works wants to be able to understand how the sales order volume and revenue is distributed by city for those customers where they have address details. In the previous units, we prepared a SalesOrderView that contains a row for every customer sales order with the country and city information for that customer where that information was available and a SalesOrderDetailsView that contains a row for every sale order line with information on the price and quantity associated with the product sold. Together these views contain all the raw data we need to answer these questions by:

  *   `%%sql
CREATE OR REPLACE TEMPORARY VIEW SalesOrderStatsView
AS
SELECT o.Country, o.City,
    COUNT(DISTINCT o.CustomerId) Total_Customers,
    COUNT(DISTINCT d.SalesOrderId) Total_Orders,
    COUNT(d.SalesOrderId) Total_OrderLines,
    SUM(d.Quantity*d.Price) AS Total_Revenue,
    dense_rank() OVER (ORDER BY SUM(d.Quantity*d.Price) DESC) as Rank_Revenue,
    dense_rank() OVER (ORDER BY COUNT(DISTINCT d.SalesOrderId) DESC) as Rank_Orders,
    dense_rank() OVER (ORDER BY COUNT(d.SalesOrderId) DESC) as Rank_OrderLines,
    dense_rank() OVER (PARTITION BY o.Country ORDER BY SUM(d.Quantity*d.Price) DESC) as Rank_Revenue_Country
FROM SalesOrderView o
INNER JOIN SalesOrderDetailsView d
    ON o.SalesOrderId = d.SalesOrderId
WHERE Country IS NOT NULL OR City IS NOT NULL
GROUP BY o.Country, o.City
ORDER BY Total_Revenue DESC`

  This query answers the questions being asked through traditional aggregation, as we are mostly interested in understanding the number (COUNT) or total (SUM) of values a GROUP BY clause that covers both Country and City (B) can answer most of the questions with absolute values for the total number of customers, orders and order lines and the sum of revenue by City (C).

  To answer the ranking part of the question, we use window functions. In essence a window function calculates a result for every row of a table based on a group of rows, called the frame. Every row can have a unique frame associated with it for that window function allowing you to concisely express and solve ranking, analytic, and aggregation problems in powerful yet simple a manner no other approach does.

  Here we use the dense_rank() function to calculate the rank of each city by revenue, number of orders and total order lines (D).

  As well as the rank of each city within each country, by partitioning the dense_rank() window function by the country.

* Write data back to the Azure Cosmos DB transactional store

  Adventure Works has decided that it would like to make information available to every user across the organization through their existing application. In order to achieve this, we need to write data back to the Azure Cosmos DB transactional stores. As with all applications the process starts by appropriately modeling the data. Some of the things to consider as we write this data back to Azure Cosmos DB:

  *   Does the document have an appropriate partition key, such that it will evenly distribute items and queries across all partitions as the data grows.
  *   Can we provide an appropriate and usable primary key (ID or _id, in the case of Core (SQL) API and API for MongoDB respectively) such that when used together with the partition key can uniquely identify an item for point operations (CRUD)
  *   Where we have modeled containers to support multiple entity types that we identify the new data with the appropriate entity type.
  *   For our example, where we are wanting to write the limited number of statistic rows back to the transactional store, we choose to create a new compound ID value that will allow client applications to look up the latest statistics for their city easily by concatenating the country code and city name together to uniquely identify each document. Given that we will be storing this in a container with other entity types we uniquely identify it type as “SalesOrderStatistic” data.
  *   `dfSalesOrderStatistic = spark.sql("SELECT concat(Country,'-',replace(City,' ','')) AS id, \
                                    'SalesOrderStatistic' AS type, \
                                    * FROM SalesOrderStatsView")

dfSalesOrderStatistic.write\
    .format("cosmos.oltp")\
    .option("spark.synapse.linkedService", "AdventureWorksSQL")\
    .option("spark.cosmos.container", "Sales")\
    .option("spark.cosmos.write.upsertEnabled", "true")\
    .mode('append')\
    .save()`

* Read data from the transactional store

  There may be scenarios where you may need to read data from the Azure Cosmos DB transactional store, for example, in where you cannot tolerate the near-real-time latency constraints of the analytical store to surface specific information or where schema inference rules have made data unavailable in the analytical store.

  When connecting to the transactional store any queries, you run will consume Azure Cosmos DB request units and could impact workloads running against the transactional store. For most analytical type queries from Spark the analytical store is going to be your best choice from both a performance and cost perspective.

  Now that you are familiar with reading from analytical store into a DataFrame reading from the transactional store will be simple as it requires only a single change to the query. For the transactional store, the format parameter is specified as cosmos.oltp instead of the cosmos.olap used to access the analytical store.

  *   `dfCustomer = spark.read\
    .format("cosmos.oltp")\
    .option("spark.synapse.linkedService", "AdventureWorksSQL")\
    .option("spark.cosmos.container", "Customer")\
    .load()

display(dfCustomer.limit(10))`

  *   And a similar approach applies to the creating a Spark SQL table to access the transactional store, the USING clause is changed to specify cosmos.oltp as the source.
  *   `%%sql
CREATE TABLE CustomersOLTP USING cosmos.oltp OPTIONS (
    spark.synapse.linkedService 'AdventureWorksSQL',
    spark.cosmos.container 'Customer'
)`

### Query Azure Cosmos DB with SQL Serverless for Azure Synapse Analytics

To use the Synapse serverless SQL pools to query the Azure Cosmos DB data made available by Azure Synapse Link. 

* Write basic queries against a single container

  We are going to be working the same two new containers (Customer and SalesOrder) we did in the previous module. The Customer and SalesOrder containers each contain example Adventure Works datasets of related customer profile records and sales order records respectively. These data sets reside in different Azure Cosmos DB accounts: the customer profile data resides in an Azure Cosmos DB Core (SQL) API account and the sales order data resides in Azure Cosmos DB API for MongoDB account, given that this data comes for distinct source systems. Adventure Works wants to use their available operational data to get insight into:

  What amount of revenue is coming from customers without completed profile data (no address details provided)
How sales order volume and revenue are distributed by city for those customers where they do have address details.

  *   Connect to an Azure Synapse Workspace that has an Azure Synapse SQL Serverless instance, and an Azure Synapse Spark Pool.
  *   In the left-hand menu, select Data (A)
  *   Click on the Linked tab in the explorer view (B)
  *   Expand the AdventureWorksSQL linked service to expose the Customer container
  *   Expand the AdventureWorksMongoDB linked service to expose the SalesOrder container.
  *   In the left-hand menu, select develop (E)
  *   Then click the “+” (F) to add a resource.
  *   And then select SQL Script (G) to create a new SQL script. A new SQL Script will immediately be created within the Synapse Workspace.
  *   Within the SQL script properties, provide an appropriate name Cosmos DB SQL Script (H)
  *   And then click the properties icon (I) to close the properties blade.
  *   Because we are going to utilize the built-in SQL Serverless pool, you can choose built-in from the connect to: drop down at the top of the query pane (J). You will see that the default database is now master.
  *   Lets create a database in which we will store the objects we are going to query. Paste the following Transact-SQL code into the query pane (L) and click the run button at the top of the query pane (M).
  *   Let’s also create a little more display real-estate by minimizing the explorer blade by: clicking << in the top right of the blade (P).
  *   We are now going to perform a SELECT operation against the Azure Cosmos DB analytical store using the OPENROWSET() function by: Paste the following SQL into the query pane, making sure to delete the previous SQL (Q).
  *   `SELECT TOP(10) * 
FROM OPENROWSET('CosmosDB',
                'Account=adventurework-sql;
Database=AdventureWorks;
Key=pRm30NZbE879vZa…Euw=='
                ,Customer) AS Customer`
                
  *   `CREATE VIEW Customers
AS
SELECT *
    FROM OPENROWSET('CosmosDB',
                    'Account=adventureworks-sql;
Database=AdventureWorks;
Key=NZbE879vZa…Euw==',
                    Customer) 
            WITH 
            (
                    CustomerId varchar(max) '$.id',
                    Title varchar(max) '$.title',
                    FirstName varchar(max) '$.firstName',
                    LastName varchar(max) '$.lastName',
                    EmailAddress varchar(max) '$.emailAddress',
                    PhoneNumber varchar(max) '$.phoneNumber',
                    AddressLine1 varchar(max) '$.address.addressLine1',
                    AddressLine2 varchar(max) '$.address.addressLine2',
                    City varchar(max) '$.address.city',
                    Country varchar(max) '$.address.country',
                    ZipCode varchar(max) '$.address.zipCode'
            )
                    AS Customers`
  *   Here you will note that we have expanded the functionality of OPENROWSET function of the CREATE VIEW statement to include a WITH clause (U) that allows us to:
      --Specify an alias for the column name (V), for example renaming the ID attribute to CustomerId in our example
      --Specify the datatype of the underlying column store (W)
      --Specify the JSON path of the property, the value of which to return in the specified column, this includes returning property values within embedded objects within the JSON. (X), for example “$.address.zipCode” to access the zipCode property contained within the embedded address object.
      
* Perform cross dataset queries in Cosmos DB

  Let's explore the second of our two containers; the SalesOrder container. This container hosts the AdventureWorks database within the Azure Cosmos DB API for MongoDB and contains sales order information.

  For reference here is a sample of a sales order JSON document from the SalesOrder container:

  *   `SELECT top 10 *
    FROM OPENROWSET('CosmosDB',
                    'Account=adventureworks-mongodb;Database=AdventureWorks;Key=v2mtZ85W0AMCv1ZrY7j…4g==',
                    SalesOrder) As SalesOrders`
  *   You are presented with a result set of the first 10 rows. It is a row-based representation of the documents contained within the SalesOrder container’s analytical store using Azure Cosmos Core API account for MongoDB. As a result, the data is represented using the full fidelity schema representation by default.

      All top-level properties of the document are represented as columns with the associated property values. All properties are represented as a structure of the type of values assigned to the properties; and the values themselves, (A) and (B). For complex types such as objects and arrays, they will remain embedded within the structure, and expanded to include type encapsulation of each of their property values.

      In this example, ignoring the system document properties for now, the _id, customerId, orderDate, and shipDate are all strings and have a type encapsulation of string (A). The details property is an embedded array (B), in turn with embedded properties sku, name, price, and quantity of each array element object.

      Let’s use what we observed here, and what we learned earlier about using OPENROWSET to combine this data with our Customers view we created previously to create a new SalesOrders view by creating the following script

  *   `CREATE VIEW SalesOrders
AS
SELECT  SalesOrderId, SalesOrders.customerId CustomerId, 
        CONVERT(date,orderDate) as OrderDate, CONVERT(date,shipdate) AS ShipDate,
        Customers.Country, Customers.City
    FROM OPENROWSET('CosmosDB',
                    'Account=adventureworks-mongodb;Database=AdventureWorks;Key=v2mtZ85W0AMCv1Zr…D3v4g==',
                    SalesOrder)
                        WITH 
                        (
                            SalesOrderId varchar(max) '$._id.string', 
                            customerId  varchar(max) '$.customerId.string',
                            orderDate varchar(max) '$.orderDate.string',
                            shipDate varchar(max) '$.shipDate.string'
                        )                  
                        As SalesOrders
            INNER JOIN Customers
                On SalesOrders.customerId = Customers.CustomerId`

  *   Here you can see we are using the WITH clause (C) again to specify the path to our property values, notably including the data type suffix (D) of the property as part of the path to the property in order to access the values when using the full fidelity schema. In this example, we need to specify “$.shipDate.string” rather than just “$.shipDate”.

      We are joining it back to our Customers view using the CustomerId key present in both (E), and doing some explicit type conversion for our date values using the CONVERT function to project them as SQL date data types.
                
* Work with windowing function


* Perform complex queries with JSON data


Visualize Azure Cosmos DB data in Power BI


