# Perform data engineering with Azure Synapse Apache Spark Pools

To perform data engineering with Azure Synapse Apache Spark Pools, which enable you to boost the performance of big-data analytic applications by in-memory cluster computing.

## Understand big data engineering with Apache Spark in Azure Synapse

To differentiate between Apache Spark, Azure Databricks, HDInsight, and SQL Pools, as well as understanding the use-cases of data-engineering with Apache Spark in Azure Synapse Analytics.

* When do you use Apache Spark pools in Azure Synapse Analytics

With the variety of Apache Spark data services that are available on Azure, the following table outlines where Azure Synapse Analytics Apache Spark pools fit in the ecosystem.

<img src="./whentouseapachespark.png" />

##  Ingest data with Apache Spark notebooks in Azure Synapse Analytics

Notebooks also enable you to write multiple languages in one notebook by using the magic commands expressed by using the %%<Name of Language> syntax. As a result you could create a temporary table to store ingested data within the notebook, and then use the magic command to enable multiple languages to work with this data.

*   With an Azure Synapse Studio notebook, you can:

    *   Get started with zero setup effort.
  
    *   Keep data secure with built-in enterprise security features.
  
    *   Analyze data across raw formats (CSV, txt, JSON, etc.), processed file formats (parquet, Delta Lake, ORC, etc.), and SQL tabular data files against Apache Spark and SQL.
  
    *   Be productive with enhanced authoring capabilities and built-in data visualization.
  
*   There are various use cases that make using notebooks compelling within Azure Synapse Analytics   
   
    *   To perform exploratory data analysis using a familiar paradigm
   
    *   To integrate notebooks as part of a broader data transformation process
   
    *   You wish to perform advanced analytics using notebooks with Azure Machine Learning Services

*   Exercise: Create a spark notebook in Azure Synapse Analytics
   
   *  In the Azure portal, navigate to the Azure Synapse workspace you want to use, and select Open Synapse Studio.
   
   *  Once the Azure Synapse Studio has launched, select Develop.
   
   *  From there, select the "+" icon, and then select Notebook. A new notebook is created and opened with an automatically generated name.
   
   *  In the Properties window, provide a name for the notebook. On the toolbar, select Publish.
   
   --Note: Add code by using the "+" icon or the "+ Cell" icon and select Code cell to input code or Markdown cell to input markdown content. When adding a code cell, the default language is PySpark. In the code cell below, you are going to use Pyspark. However, other supported languages are Scala, SQL, and .NET for Spark.
   
   `new_rows = [('CA',22, 45000),("WA",35,65000) ,("WA",50,85000)]`
   `demo_df = spark.createDataFrame(new_rows, ['state', 'age', 'salary'])`
   `demo_df.show()`
   
   --When you want to run a cell, you can use the following methods: 
   Press SHIFT + ENTER.
   Select the blue play icon to the left of the cell.
   Select the Run all button on the toolbar.
   
*  Azure Synapse Analytics Spark pools support various languages. The primary languages available within the notebook environment are: PySpark (Python), Spark (Scala), .NET Spark (C#), Spark SQL
   
   It is possible to use multiple languages in one notebook by specifying the language using a magic command at the beginning of a cell. The following table lists the magic commands to switch cell languages:
   
   <img src="./magicword.png" />
   
###  Create a Spark Notebook in Azure Synapse Analytics

### Develop Spark Notebooks

### Run Spark Notebooks, Load data in Spark Notebooks and Save Spark Notebooks

##  Transform data with DataFrames in Apache Spark Pools in Azure Synapse Analytics


##  Integrate SQL and Apache Spark pools in Azure Synapse Analytics


##  
