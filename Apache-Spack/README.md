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

###  Create a Spark Notebook in Azure Synapse Analytics
   
*   Exercise: Create a spark notebook in Azure Synapse Analytics
   
    *    In the Azure portal, navigate to the Azure Synapse workspace you want to use, and select Open Synapse Studio.
   
    *    Once the Azure Synapse Studio has launched, select Develop.
   
    *    From there, select the "+" icon, and then select Notebook. A new notebook is created and opened with an automatically generated name.
   
    *    In the Properties window, provide a name for the notebook. On the toolbar, select Publish.
   
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
   
   --Note: You cannot reference data or variables directly using different languages in an Azure Synapse Studio notebook. If you wish to do this using Spark, you first create a temporary table so that it can be referenced across different languages. Here is an example of how to read a `Scala` DataFrame in `PySpark` and `SparkSQL` using a Spark temp table as a workaround. The following code shows you how to read a DataFrame from a SQL pool connector using Scala. It also shows how you can create a temporary table.
   
   `%%spark`
   
   `val scalaDataFrame = spark.read.sqlanalytics("mySQLPoolDatabase.dbo.mySQLPoolTable")`
   
   `scalaDataFrame.createOrReplaceTempView( "mydataframetable" )`
   
   --If you want to query the DataFrame in the above example, using Spark SQL, you can add a code cell below the code snippet above, and use the %%sql command. Using the %%sql command enables you to use a SQL statement such as shown below where you would select everything from the 'mydataframetable'.
   
   `%%sql`
   
   `SELECT * FROM mydataframetable`
   
   --If you want to use the data in PySpark, below an example is given using the magic command %%pyspark, in which you create a new Python DataFrame based on the mydataframe table whilst using spark.sql to select everything from that table.
   
   `%%pyspark`
   
   `myNewPythonDataFrame = spark.sql("SELECT * FROM mydataframetable")`
   
   --You can use familiar Jupyter magic commands in Azure Synapse Studio notebooks. Review the following list as the current available magic commands: 
   
   Available line magics: %lsmagic, %time, %time it

   Available cell magics: %%time, %%timeit, %%capture, %%writefile, %%sql, %%pyspark, %%spark, %%csharp
   
### Develop/Run/Load data/Save Spark Notebooks

*  To develop solutions in a notebook, you work with cells. Cells are individual blocks of code or text that can be run independently or as a group. There are a range of actions that can be performed against a cell including:
   *  Move a cell.
   *  Delete a cell.
   *  Collapse Cell in and output.
   *  Undo Cell operations.

*  Run spark notebooks
   *  Run a Cell
   *  Run all cells
   *  Cancel a running cell
   *  Cell Status indicator
   *  Spark progress indicator
   *  Spark session config
   
*  Load data in spark notebooks
   In order to ingest data into a notebook, there are several options. Currently it is possible to load data from an Azure Storage Account, and an Azure Synapse Analytics dedicated SQL pool.

   *     Some examples for reading data in a notebook are:

         --Read a CSV from Azure Data Lake Store Gen2 as an Apache Spark DataFrame
   
         --Read a CSV from Azure Storage Account as an Apache Spark DataFrame
   
         --Read data from the primary storage account
   
   *     Example 1: Read a CSV file from an Azure Data Lake Store Gen2 store as an Apache Spark DataFrame.
   
   *     Example 2: Read a CSV file from Azure Storage Account as a Spark DataFrame.
   
   *     Example 3: Read data from the primary storage account
   
   *     Example 4: Ingest and explore Parquet files from a data lake with Apache Spark for Azure Synapse
   
         *  By using the Data hub to view the Parquet files in the connected storage account, then use the new notebook context menu to create a new Synapse notebook that loads a Spark DataFrame with the contents of a selected Parquet file.
   
         *  Add the following code beneath the code in the cell to define a variable named datalake whose value is the name of the primary storage account (replace the REPLACE_WITH_YOUR_DATALAKE_NAME value with the name of the storage account in line 2): `datalake = 'REPLACE_WITH_YOUR_DATALAKE_NAME'`
   
         *  By default, the cell outputs to a table view when we use the `display()` function. Let's select the Chart visualization to see a different view of the data.
   
         *  The Apache Spark engine can analyze the Parquet files and infer the schema. To do so, enter the below code in the new cell and run it: `data_path.printSchema()`
   
         --Note: Apache Spark evaluates the file contents to infer the schema. This automatic inference is sufficient for data exploration and most transformation tasks. However, when you load data to an external resource like a SQL pool table, sometimes you need to declare your own schema and apply that to the dataset. For now, the schema looks good.
   
   <a href="./loadinnotebook.py">Examples</a>

##  Transform data with DataFrames in Apache Spark Pools in Azure Synapse Analytics


##  Integrate SQL and Apache Spark pools in Azure Synapse Analytics


##  
