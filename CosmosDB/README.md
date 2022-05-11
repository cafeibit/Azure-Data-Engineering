# Data Engineering with Azure Cosmos DB

Azure Cosmos DB is a globally distributed database system that allows you to read and write data from the local replicas of your database and it transparently replicates the data to all the regions associated with your Cosmos account.

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

* Azure Synapse Link for Azure Cosmos DB

  Azure Synapse Link for Azure Cosmos DB is a cloud-native HTAP capability that enables you to run near-real-time analytics over operational data stored in Azure Cosmos DB. Azure Synapse Link creates a tight seamless integration between Azure Cosmos DB and Azure Synapse Analytics.

<img src="./synapse-analytics-cosmos-db-architecture.png" />


###  To configure and enable Azure Synapse Link to interact with Azure Cosmos DB. 


### To perform analytics against Azure Cosmos DB using Azure Synapse Link.


### To use the Synapse serverless SQL pools to query the Azure Cosmos DB data made available by Azure Synapse Link. 
