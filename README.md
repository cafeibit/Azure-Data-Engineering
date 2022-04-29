# Azure Data Engineering
##### To choose Azure Cloud Plaftorm as data engineering solution for multimodel data processing - batch/stream.
##### To make data-drven decisions and build data-powered products becoming morden business intelligence - AI/ML.

### Data Engineering with Azure Data Factory
To orchestrate big data, integrate data sources, ingest data from on-premises, multiple-cloud, and software as a service (SaaS) data sources; create and schedule data-driven workflows to ingest data from different data stores, build complex ETL processes to transform this data visually with compute services or with data flows.
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

