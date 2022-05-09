--Read a csv file
--to see the content of your CSV file is to provide the file URL to the OPENROWSET function. 
--You then include the csv FORMAT, and 2.0 PARSER_VERSION. 

select top 10 *
from openrowset(
    bulk 'https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/ecdc_cases/latest/ecdc_cases.csv',
    format = 'csv',
    parser_version = '2.0',
    firstrow = 2 ) as rows
    
--Note: The OPENROWSET function enables you to read the content of CSV file by providing the URL to your file.

--The previous example uses a full path to the file. As an alternative, 
--you can create an external data source with the location that points to the root folder of the storage:

create external data source covid
with ( location = 'https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/ecdc_cases' );

select top 10 *
from openrowset(
        bulk 'latest/ecdc_cases.csv',
        data_source = 'covid',
        format = 'csv',
        parser_version ='2.0',
        firstrow = 2
    ) as rows
    
 --The OPENROWSET function enables you to explicitly specify what columns you want to read from the file using WITH clause:
  
select top 10 *
from openrowset(
        bulk 'latest/ecdc_cases.csv',
        data_source = 'covid',
        format = 'csv',
        parser_version = '2.0',
        firstrow = 2
    ) with (
        date_rep date 1,
        cases int 5,
        geo_id varchar(6) 8
    ) as rows
    
 --Note: The numbers after a data type in the WITH clause represent column location, known as a column index in the CSV file.
 
 --The following query shows how to read a CSV file without a header row, with a Windows-style new line, 
 --and comma-delimited columns, as shown in the following file preview example:
 
 SELECT *
FROM OPENROWSET(
        BULK 'csv/population/population.csv',
        DATA_SOURCE = 'SqlOnDemandDemo',
        FORMAT = 'CSV', PARSER_VERSION = '2.0',
        FIELDTERMINATOR =',',
        ROWTERMINATOR = '\n'
    )
WITH (
    [country_code] VARCHAR (5) COLLATE Latin1_General_BIN2,
    [country_name] VARCHAR (100) COLLATE Latin1_General_BIN2,
    [year] smallint,
    [population] bigint
) AS [r]
WHERE
    country_name = 'Luxembourg'
    AND year = 2017;
 
--The following query shows how to read a file without a header row, with a Unix-style new line, 
--and comma-delimited columns as shown in the following file preview.

SELECT * 
FROM OPENROWSET( 
    BULK 'csv/population-unix/population.csv', 
    DATA_SOURCE = 'SqlOnDemandDemo', 
    FORMAT = 'CSV', PARSER_VERSION = '2.0', 
    FIELDTERMINATOR =',', 
    ROWTERMINATOR = '0x0a' ) 
WITH ( 
    [country_code] VARCHAR (5) COLLATE Latin1_General_BIN2, 
    [country_name] VARCHAR (100) COLLATE Latin1_General_BIN2, 
    [year] smallint, [population] bigint 
) AS [r] 
WHERE 
    country_name = 'Luxembourg' 
    AND year = 2017;
    
--The following query shows how to read a file with a header row, with a Unix-style new line, and comma-delimited columns, as shown in the following file preview.

SELECT *
FROM OPENROWSET(
        BULK 'csv/population-unix-hdr/population.csv',
        DATA_SOURCE = 'SqlOnDemandDemo',
        FORMAT = 'CSV', PARSER_VERSION = '2.0',
        FIELDTERMINATOR =',',
        HEADER_ROW = TRUE
    ) AS [r]
    
  --The following query shows how to read a file with a header row, with a Unix-style new line, 
  --comma-delimited columns, and quoted value as shown in the following file preview.
  
SELECT * 
FROM OPENROWSET( 
    BULK 'csv/population-unix-hdr-quoted/population.csv', 
    DATA_SOURCE = 'SqlOnDemandDemo', 
    FORMAT = 'CSV', PARSER_VERSION = '2.0', 
    FIELDTERMINATOR =',', 
    ROWTERMINATOR = '0x0a',
    FIRSTROW = 2,
    FIELDQUOTE = ' ” '
) 
WITH ( 
    [country_code] VARCHAR (5) COLLATE Latin1_General_BIN2, 
    [country_name] VARCHAR (100) COLLATE Latin1_General_BIN2, 
    [year] smallint, [population] bigint 
) AS [r] 
WHERE 
    country_name = 'Luxembourg' 
    AND year = 2017;
    
 --The following query shows how to read a file with a header row, with a Unix-style new line, comma-delimited columns, 
 --and an escape char used for the field delimiter (comma) within values, as shown in the following file preview.
 
SELECT * 
FROM OPENROWSET( 
    BULK 'csv/population-unix-hdr-escape/population.csv', 
    DATA_SOURCE = 'SqlOnDemandDemo', 
    FORMAT = 'CSV', PARSER_VERSION = '2.0', 
    FIELDTERMINATOR =',', 
    ROWTERMINATOR = '0x0a',
    FIRSTROW = 2,
    ESCAPECHAR = ' \\ '
) 
WITH ( 
    [country_code] VARCHAR (5) COLLATE Latin1_General_BIN2, 
    [country_name] VARCHAR (100) COLLATE Latin1_General_BIN2, 
    [year] smallint, 
    [population] bigint 
) AS [r] 
WHERE 
    country_name = 'Slovenia';
    
--The following query shows how to read a file with a header row, with a Unix-style new line, 
--comma-delimited columns, and an escaped double quote char within values as shown in the following file preview.

SELECT * 
FROM OPENROWSET( 
    BULK 'csv/population-unix-hdr-escape-quoted/population.csv', 
    DATA_SOURCE = 'SqlOnDemandDemo', 
    FORMAT = 'CSV', PARSER_VERSION = '2.0', 
    FIELDTERMINATOR =',', 
    ROWTERMINATOR = '0x0a',
    FIRSTROW = 2
) 
WITH ( 
    [country_code] VARCHAR (5) COLLATE Latin1_General_BIN2, 
    [country_name] VARCHAR (100) COLLATE Latin1_General_BIN2, 
    [year] smallint, 
    [population] bigint 
) AS [r] 
WHERE 
    country_name = 'Slovenia';
    
--The following query shows how to read a file with a header row, 
--with a Unix-style new line, and tab-delimited columns, as shown in the following file preview.
 
SELECT * 
FROM OPENROWSET( 
    BULK 'csv/population-unix-hdr-tsv/population.csv', 
    DATA_SOURCE = 'SqlOnDemandDemo', 
    FORMAT = 'CSV', PARSER_VERSION = '2.0', 
    FIELDTERMINATOR ='\t', 
    ROWTERMINATOR = '0x0a',
    FIRSTROW = 2
) 
WITH ( 
    [country_code] VARCHAR (5) COLLATE Latin1_General_BIN2, 
    [country_name] VARCHAR (100) COLLATE Latin1_General_BIN2, 
    [year] smallint, 
    [population] bigint 
) AS [r] 
WHERE 
    country_name = 'Slovenia'
    AND year = 2017
    
-- The following query returns the number of distinct country/region names in a file, 
--specifying only the columns that are needed.

SELECT 
    COUNT(DISTINCT country_name) AS countries 
FROM OPENROWSET(
    BULK 'csv/population/population.csv', 
    DATA_SOURCE = 'SqlOnDemandDemo', 
    FORMAT = 'CSV', PARSER_VERSION = '2.0', 
    FIELDTERMINATOR =',', 
    ROWTERMINATOR = '\n' ) 
WITH ( 
    --[country_code] VARCHAR (5), 
    [country_name] VARCHAR (100) 2 
    --[year] smallint, 
    --[population] bigint 
) AS [r]

--
--Query specific column of parquet files
--

SELECT 
    YEAR(tpepPickupDateTime), 
    passengerCount, 
    COUNT(*) AS cnt 
FROM 
    OPENROWSET( 
        BULK 'puYear=2018/puMonth=*/*.snappy.parquet', 
        DATA_SOURCE = 'YellowTaxi', 
        FORMAT='PARQUET' 
    ) WITH ( 
        tpepPickupDateTime DATETIME2, 
        passengerCount INT 
    ) AS nyc 
GROUP BY 
    passengerCount, 
    YEAR(tpepPickupDateTime) 
ORDER BY 
    YEAR(tpepPickupDateTime), 
    passengerCount;
    
--Note： Although you don't need to use the OPENROWSET WITH clause when reading Parquet files. Column names and data types are automatically read from Parquet files.

-- You can target specific partitions using the filepath function. 
--This example shows fare amounts by year, month, and payment_type for the first three months of 2017.
--The serverless SQL pool query is compatible with Hive/Hadoop partitioning scheme.

SELECT 
    YEAR(tpepPickupDateTime), 
    passengerCount, 
    COUNT(*) AS cnt 
FROM 
    OPENROWSET(
        BULK 'puYear=*/puMonth=*/*.snappy.parquet', 
        DATA_SOURCE = 'YellowTaxi', 
        FORMAT='PARQUET' ) nyc 
WHERE 
    nyc.filepath(1) = 2017 
    AND nyc.filepath(2) IN (1, 2, 3) 
    AND tpepPickupDateTime BETWEEN CAST('1/1/2017' AS datetime) AND CAST('3/31/2017' AS datetime)
GROUP BY 
    passengerCount, 
    YEAR(tpepPickupDateTime) 
ORDER BY 
    YEAR(tpepPickupDateTime), 
    passengerCount;
    
 --
 --Query a JSON file using Azure Synapse serverless SQL pools
 --
 
 --Query JSON files using JSON_VALUE
 
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

--Query JSON files using OPENJSON

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

--
--Query multiple files and folders using Azure Synapse serverless SQL pools
--

--Read all files in folder

SELECT 
    YEAR(pickup_datetime) AS [year], 
    SUM(passenger_count) AS passengers_total, 
    COUNT(*) AS [rides_total] 
FROM 
    OPENROWSET( 
        BULK 'csv/taxi/*.csv', 
        DATA_SOURCE = 'sqlondemanddemo', 
        FORMAT = 'CSV', 
        PARSER_VERSION = '2.0', 
        FIRSTROW = 2 
    ) WITH ( 
        pickup_datetime DATETIME2 2, 
        passenger_count INT 4 
    ) AS nyc 
GROUP BY 
    YEAR(pickup_datetime) 
ORDER BY 
    YEAR(pickup_datetime);
    
--Read subset of files in folder

SELECT 
    payment_type,
    SUM(fare_amount) AS fare_total
FROM 
    OPENROWSET( 
        BULK 'csv/taxi/yellow_tripdata_2017-*.csv', 
        DATA_SOURCE = 'sqlondemanddemo', 
        FORMAT = 'CSV', 
        PARSER_VERSION = '2.0', 
        FIRSTROW = 2 
    ) WITH ( 
        payment_type INT 10, 
        fare_amount FLOAT 11 
    ) AS nyc 
GROUP BY 
    payment_type 
ORDER BY 
    payment_type;
    
--Read all files from multiple folders

SELECT
    YEAR(pickup_datetime) as [year],
    SUM(passenger_count) AS passengers_total,
    COUNT(*) AS [rides_total]
FROM OPENROWSET(
        BULK 'csv/t*i/', 
        DATA_SOURCE = 'sqlondemanddemo',
        FORMAT = 'CSV', PARSER_VERSION = '2.0',
        FIRSTROW = 2
    )
    WITH (
        vendor_id VARCHAR(100) COLLATE Latin1_General_BIN2, 
        pickup_datetime DATETIME2, 
        dropoff_datetime DATETIME2,
        passenger_count INT,
        trip_distance FLOAT,
        rate_code INT,
        store_and_fwd_flag VARCHAR(100) COLLATE Latin1_General_BIN2,
        pickup_location_id INT,
        dropoff_location_id INT,
        payment_type INT,
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        improvement_surcharge FLOAT,
        total_amount FLOAT
    ) AS nyc
GROUP BY
    YEAR(pickup_datetime)
ORDER BY
    YEAR(pickup_datetime);
    
--Use multiple wildcards

SELECT
    YEAR(pickup_datetime) as [year],
    SUM(passenger_count) AS passengers_total,
    COUNT(*) AS [rides_total]
FROM OPENROWSET(
        BULK 'csv/t*i/yellow_tripdata_2017-*.csv',
        DATA_SOURCE = 'sqlondemanddemo',
        FORMAT = 'CSV', PARSER_VERSION = '2.0',
        FIRSTROW = 2
    )
    WITH (
        vendor_id VARCHAR(100) COLLATE Latin1_General_BIN2, 
        pickup_datetime DATETIME2, 
        dropoff_datetime DATETIME2,
        passenger_count INT,
        trip_distance FLOAT,
        rate_code INT,
        store_and_fwd_flag VARCHAR(100) COLLATE Latin1_General_BIN2,
        pickup_location_id INT,
        dropoff_location_id INT,
        payment_type INT,
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        improvement_surcharge FLOAT,
        total_amount FLOAT
    ) AS nyc
GROUP BY
    YEAR(pickup_datetime)
ORDER BY
    YEAR(pickup_datetime);

