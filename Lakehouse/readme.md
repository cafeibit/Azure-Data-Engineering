# Implement Azure Databricks Lakehouse and build incremental workflows

We choose Azure Databricks to build the data engineering and analytics platfrom in the Azure cloud. 
Before we implement the Lakehouse platform we need to explore the external data from the ingested raw data by ADF in the Azure Data Lake, 
then we can use Auto Loader function to build incremental ETL pipelines as the multi-hop architecture - "Boronze", "Silver" and "Gold" data layer.

 - Explore and analyss external data from "Source" in Azure Data Lake
 - Design and implement the incremental auto loader pipelines
 - Orchestrate and monitor the workflows by ADF and DLT
 - Test and build the report dashboard
 - Data govenance and more
