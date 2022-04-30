# Azure Data Engineering
##### To choose Azure Cloud Plaftorm as data engineering solution for multimodel data processing - batch/stream.
##### To make data-drven decisions and build data-powered products becoming morden business intelligence - AI/ML.
* To provision and set up data platform technologies that are on-premises and in the cloud manage and secure the flow of structured and unstructured data from multiple sources. The data platforms they use can include relational databases, nonrelational databases, data streams, and file stores. 
* To ensure that data services securely and seamlessly integrate with other data platform technologies or application services such as Azure Cognitive Services, Azure Search, or even bots. 

Primary responsibilities include using services and tools to ingest, egress, and transform data from multiple sources (data wrangling). Azure data engineers collaborate with business stakeholders to identify and meet data requirements. They design and implement solutions. They also manage, monitor, and ensure the security and privacy of data to satisfy business needs. 
* Design and develop data storage and data processing solutions for the enterprise. 
* Set up and deploy cloud-based data services such as blob services, databases, and analytics. 
* Secure the platform and the stored data. Make sure only the necessary users can access the data. 
* Ensure business continuity in uncommon conditions by using techniques for high availability and disaster recovery. 
* Monitor to ensure that the systems run properly and are cost-effective. 

### Data Engineering with Azure Data Factory
To orchestrate big data, integrate data sources, ingest data from on-premises, multiple-cloud, and software as a service (SaaS) data sources; create and schedule data-driven workflows to ingest data from different data stores, build complex ETL processes to transform this data visually with compute services or with data flows. To build complex and iterative processing logic within the pipelines you create with Azure Data Factory, which supports the creation of diverse data integration patterns such as building a modern data warehouse.

* Connect and Collect
  * To collect the required data from the appropriate data sources in different locations, including on-premises sources and in the cloud.  
  * To use the <code>Copy Activity</code> (Read data from source data store; Perform the following tasks on the data: Serialization/deserialization, Compression/decompression and Column mapping; Write data to the destination data store - sink) to move data from various sources to a single, centralized data store in the cloud. 
* Transform and Enrich
  * To use Data Factory mapping data flows to process and transform the data as needed.
* CI/CD & Publish
  * To develop and deliver ETL processes incrementally before publishing by using Azure DevOps and GitHub. 
  * Set triggers on-demand and schedule data processing based on your needs. Associate a pipeline with a trigger, or manually start it as and when needed. 
  * Connect to linked services, such as on-premises apps and data, or Azure services via integration runtimes.
  * After refined the raw data, to load the data into whichever analytics engine to access from business intelligence tools, including Azure Synapse Analytics, Azure SQL Database, and Azure Cosmos DB.
* Monitoring 
  * After successfully built and deployed data integration pipeline, to monitor your scheduled activities and pipelines. This enables you to track success and failure rates by using one of following: Azure Monitor, API, PowerShell, Azure Monitor logs, and Health panels in the Azure portal 

### Data Engineering with Azure Synapse Analytics

* To choose the tools and techniques used to work with Modern Data Warehouses productively and securely within Azure Synapse Analytics. To to build Data Warehouses using modern architecture patterns by using Azure Synapse Analytics. 
* To optimize data warehouse query performance in Azure Synapse Analytics 
* To integrate SQL and Apache Spark pools in Azure Synapse Analytics 
* To manage and monitor data warehouse activities in Azure Synapse Analytics 
* To analyze and optimize data warehouse storage in Azure Synapse Analytics 
* To approach and implement security to protect your data with Azure Synapse Analytics such as serverless SQL pools. 
* To use serverless SQL pools
  * To query and prepare data in an interactive way on files placed in Azure Storage such as querying the different file types stored in a data lake. 
  * To create objects to help you query data or optimize your existing data transformation pipeline through Azure Synapse serverless SQL pools
  * To use CREATE EXTERNAL TABLE AS SELECT statements to transform data and encapsulate the transformation logic in stored procedures.
* To perform data engineering with Azure Synapse Apache Spark Pools, to boost the performance of big-data analytic applications by in-memory cluster computing. 
  * To ingest data using Apache Spark Notebooks in Azure Synapse Analytics.
  * To transform data using DataFrames in Apache Spark Pools in Azure Synapse Analytics. 
  * To integrate SQL and Apache Spark pools in Azure Synapse Analytics.
  * To monitor and manage data engineering workloads with Apache Spark in Azure Synapse Analytics.
* Work with Hybrid Transactional and Analytical Processing Solutions using Azure Synapse Analytics, to perform operational analytics against Azure Cosmos DB using the Azure Synapse Link feature within Azure Synapse Analytics. 
  * To configure and enable Azure Synapse Link to interact with Azure Cosmos DB. 
  * To perform analytics against Azure Cosmos DB using Azure Synapse Link.
  * to use the Synapse serverless SQL pools to query the Azure Cosmos DB data made available by Azure Synapse Link. 
* Integrate Azure Synapse Analytics with Azure Data and AI services
  * To build Power BI reports from within Azure Synapse Analytics. 
  * To build machine learning modules from within Azure Synapse Analytics. 
 * 
### Data Engineering with Azure Databricks with best practices
To process and analyze data using multi-task jobs and Delta Live Tables, the Azure Databricks data processing pipeline framework. To run a Delta Live Tables pipeline as part of a data processing workflow with Databricks jobs, Apache Airflow, or Azure Data Factory. 
* Azure Databricks/Delta Lake architecture/performance/features programming by Scala/Python/Java
* Read and write data in Azure Databricks 
* Work and code with DataFrames in Azure Databricks 
* Build and query a Delta Lake & performance tuning
* Build production workloads on Azure Databricks with Azure Data Factory
* Integrate Azure Databricks with Azure Synapse 
  * Describe the connection architecture of Synapse and Spark
  * Configure a connection between Databricks and Synapse
  * Read data from Synapse and Write data to Synapse
* Implement CI/CD with Azure DevOps - an automated process version control, testing capabilities, and controls for deployment approvals

