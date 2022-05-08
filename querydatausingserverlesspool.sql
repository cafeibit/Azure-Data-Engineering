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

