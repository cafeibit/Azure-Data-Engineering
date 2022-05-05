
# Analyze and optimize data warehouse storage in Azure Synapse Analytics 

`%%sql`
 -- Find data skew for a distributed table
 `DBCC PDW_SHOWSPACEUSED('dbo.FactInternetSales');`
  
### Analyze the space used by tables
1. **Open Synapse Studio.**
2. **Select the Develop hub.**
3. **From the Develop menu, select the + button (1) and choose SQL Script (2) from the context menu.**
4. **In the toolbar menu, connect to the SQLPool01 database to execute the query.**
5. **In the query window, replace the script with the following Database Console Command (DBCC):**
  <br><code>DBCC PDW_SHOWSPACEUSED('wwi_perf.Sale_Hash');</code>
6. **Analyze the number of rows in each distribution.**
  
 Let's find now the distribution of per-customer transaction item counts. Run the following query: 
  
### Use a more advanced approach to understand table space usage
1. **Run the following script to create the `vTableSizes` view:**        
2. **Run the following script to view the details about the structure of the tables in the wwi_perf schema:**

### View column store storage details
1. **Run the following query to create the `vColumnStoreRowGroupStats`:**      
2. **Explore the statistics of the columnstore for the Sale_Partition01 table using the following query:**
3. **Explore the results of the query:**
4. **Explore the statistics of the columnstore for the Sale_Hash_Ordered table using the same query:**
5. **Explore the results of the query:**
  
### Compare storage requirements between optimal and sub-optimal column data types
 
### Improve the execution plan of a query with a materialized view
  
### Understand rules for minimally logged operations & Optimize a delete operation


**Example:** Create hash distribution table with a clustered columnstore index

* Select the Develop hub 
* From the Develop menu, select the + button (1) and choose SQL Script (2) from the context menu.
* In the toolbar menu, connect to the SQL Pool database to execute the query.
* `CREATE TABLE [wwi_perf].[Sale_Hash]`<br>
`WITH`<br>
`(`<br>
   `DISTRIBUTION = HASH ( [CustomerId] ),`<br>
   `CLUSTERED COLUMNSTORE INDEX`<br>
`)`<br>
`AS`<br>
`SELECT`<br>
 `*`<br>
`FROM`<br>
   `[wwi_perf].[Sale_Heap]`<br>

* Select Run from the toolbar menu to execute the SQL command.

**Note** 
CTAS is a more customizable version of the SELECT...INTO statement. 
SELECT...INTO doesn't allow you to change either the distribution method or the index type as part of the operation. 
You create the new table by using the default distribution type of ROUND_ROBIN, and the default table structure of CLUSTERED COLUMNSTORE INDEX.
With CTAS, on the other hand, you can specify both the distribution of the table data as well as the table structure type.

*  To see performance improvements:
`SELECT TOP 1000 * FROM`<br>
`(`<br>
    `SELECT`<br>
        `S.CustomerId`<br>
        `,SUM(S.TotalAmount) as TotalAmount`<br>
    `FROM`<br>
        `[wwi_perf].[Sale_Hash] S`<br>
    `GROUP BY`<br>
        `S.CustomerId`<br>
`) T`<br>

#####  Statistics in dedicated SQL pools
*  To check if your data warehouse has AUTO_CREATE_STATISTICS configured by running the following command:
`SELECT name, is_auto_create_stats_on`<br>
`FROM sys.databases`<br>
*  If your data warehouse doesn't have AUTO_CREATE_STATISTICS enabled, it is recommended that you enable this property by running the following command:
`ALTER DATABASE <yourdatawarehousename>`<br>
`SET AUTO_CREATE_STATISTICS ON`<br>

##### Statistics in a serverless SQL pool has the same objective of using a cost-based optimizer to choose an execution plan that will execute the fastest. 
*  
`sys.sp_create_openrowset_statistics [ @stmt = ] N'statement_text'`
