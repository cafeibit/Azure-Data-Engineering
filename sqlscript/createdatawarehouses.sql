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

SELECT APPROX_COUNT_DISTINCT(CustomerId) from wwi_perf.Sale_Heap

--APPROX_COUNT_DISTINCT returns a result with a 2% accuracy of true cardinality on average.
--This means, if COUNT (DISTINCT) returns 1,000,000, HyperLogLog will return a value in the range of 999,736 to 1,016,234.

--Read JSON documents
--The easiest way to see the content of your JSON file is to provide the file URL to the OPENROWSET function, specify csv FORMAT, 
--and set the values of 0x0b for the fieldterminator and fieldquote variables. 
--If you need to read line-delimited JSON files, then this is enough. 
--If you have classic JSON file, you would need to set values 0x0b for rowterminator. 

select top 10 * 
from 
    openrowset( 
        bulk 'https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/ecdc_cases/latest/ecdc_cases.jsonl', 
        format = 'csv', 
        fieldterminator ='0x0b', 
        fieldquote = '0x0b' 
    ) with (doc nvarchar(max)) as rows

--

select top 10 * 
from 
    openrowset( 
        bulk 'https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/ecdc_cases/latest/ecdc_cases.json', 
        format = 'csv', 
        fieldterminator ='0x0b', 
        fieldquote = '0x0b', 
        rowterminator = '0x0b' --> You need to override rowterminator to read classic JSON 
    ) with (doc nvarchar(max)) as rows
    
 --The JSON document in the preceding sample query includes an array of objects. 
 --The query returns each object as a separate row in the result set. Make sure that you can access this file. 
 --If your file is protected with SAS key or custom identity, you would need to set up server level credential for sql login
 
 --The previous example uses a full path to the file. As an alternative, you can create an external data source 
 --with the location that points to the root folder of the storage, 
 --and use that data source and the relative path to the file in the OPENROWSET function:
 
create external data source covid 
with (location = 'https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/ecdc_cases');
go 
select top 10 * 
from 
    openrowset( 
        bulk 'latest/ecdc_cases.jsonl', 
        data_source = 'covid', 
        format = 'csv', 
        fieldterminator ='0x0b', 
        fieldquote = '0x0b' 
    ) with (doc nvarchar(max)) as rows 
go 
select top 10 * 
from 
    openrowset( 
        bulk 'latest/ecdc_cases.json', 
        data_source = 'covid', 
        format = 'csv', 
        fieldterminator ='0x0b', 
        fieldquote = '0x0b', 
        rowterminator = '0x0b' --> You need to override rowterminator to read classic JSON 
    ) with (doc nvarchar(max)) as rows
    
--The queries in the previous examples return every JSON document as a single string in a separate row of the result set. 
--You can use functions JSON_VALUE and OPENJSON to parse the values in JSON documents and return them as relational values    

--If these documents are stored as line-delimited JSON, you need to set FIELDTERMINATOR and FIELDQUOTE to 0x0b. 
--If you have standard JSON format you need to set ROWTERMINATOR to 0x0b.

select 
    JSON_VALUE(doc, '$.date_rep') AS date_reported, 
    JSON_VALUE(doc, '$.countries_and_territories') AS country, 
    JSON_VALUE(doc, '$.cases') as cases, 
    doc 
from 
    openrowset( 
        bulk 'latest/ecdc_cases.jsonl', 
        data_source = 'covid', 
        format = 'csv', 
        fieldterminator ='0x0b', 
        fieldquote = '0x0b' 
    ) with (doc nvarchar(max)) as rows 
order by JSON_VALUE(doc, '$.geo_id') desc

--The following query uses OPENJSON. It will retrieve COVID statistics reported in Serbia:

select * 
from 
    openrowset( 
        bulk 'latest/ecdc_cases.jsonl', 
        data_source = 'covid', 
        format = 'csv', 
        fieldterminator ='0x0b', 
        fieldquote = '0x0b' 
    ) with (doc nvarchar(max)) as rows 
    cross apply openjson (doc) 
        with ( date_rep datetime2, 
                   cases int, 
                   fatal int '$.deaths', 
                   country varchar(100) '$.countries_and_territories') 
where country = 'Serbia' 
order by country, date_rep desc;
    
