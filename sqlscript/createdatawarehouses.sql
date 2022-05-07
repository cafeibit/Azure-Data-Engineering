--The Develop hub in Azure Synapse Studio is an interface you can use for developing a variety of solutions against an Azure Synapse Analytics instance. 
--In this area, you have the ability to create the following objects: SQL Scripts, Notebooks, Azure Synapse Pipelines and Power BI datasets and reports.
--also define Spark Job definitions, in either PySpark, Scala, or .NET Spark that can submit a batch job to the Azure Synapse Spark pool too

--OVER clause determines the partitioning and ordering of a rowset before the associated window function is applied.
--That is, the OVER clause defines a window or user-specified set of rows within a query result set. 
--A window function then computes a value for each row in the window. 
--You can use the OVER clause with functions to compute aggregated values such as moving averages, 
--cumulative aggregates, running totals, or a top N per group results.

SELECT
ROW_NUMBER() OVER(PARTITION BY Region ORDER BY Quantity DESC) AS "Row Number",
Product,
Quantity,
Region
FROM wwi_security.Sale
WHERE Quantity <> 0  
ORDER BY Region;

--When we use PARTITION BY with the OVER clause (1), we divide the query result set into partitions. 
--The window function is applied to each partition separately and computation restarts for each partition.

--To build window functions that use the PERCENTILE_CONT and PERCENTILE_DISC functions.

-- PERCENTILE_CONT, PERCENTILE_DISC
SELECT DISTINCT c.Category  
,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bc.Downloads)
                    OVER (PARTITION BY Category) AS MedianCont  
,PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY bc.Downloads)
                    OVER (PARTITION BY Category) AS MedianDisc  
FROM dbo.Category AS c  
INNER JOIN dbo.BookList AS bl
    ON bl.CategoryID = c.ID
INNER JOIN dbo.BookConsumption AS bc  
    ON bc.BookID = bl.ID
ORDER BY Category

--LAG Function
SELECT ProductId,
    [Hour] AS SalesHour,
    TotalAmount AS CurrentSalesTotal,
    LAG(TotalAmount, 1,0) OVER (ORDER BY [Hour]) AS PreviousSalesTotal,
    TotalAmount - LAG(TotalAmount,1,0) OVER (ORDER BY [Hour]) AS Diff
FROM [wwi_perf].[Sale_Index]
WHERE ProductId = 3848 AND [Hour] BETWEEN 8 AND 20;

-- ROWS UNBOUNDED PRECEDING
SELECT DISTINCT bc.Country, b.Title AS Book, bc.Downloads
    ,FIRST_VALUE(b.Title) OVER (PARTITION BY Country  
        ORDER BY Downloads ASC ROWS UNBOUNDED PRECEDING) AS FewestDownloads
FROM dbo.BookConsumption AS bc
INNER JOIN dbo.Books AS b
    ON b.ID = bc.BookID
ORDER BY Country, Downloads

--Approximate execution using HyperLogLog functions

```sql
SELECT COUNT(DISTINCT CustomerId) from wwi_perf.Sale_Heap
```
