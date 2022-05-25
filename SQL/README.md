# T-SQL for Data Engineering

Microsoft database systems such as SQL Server, Azure SQL Database, Azure Synapse Analytics, and others use a dialect of SQL called Transact-SQL, or T-SQL. T-SQL includes language extensions for writing stored procedures and functions, which are application code that is stored in the database, and managing user accounts.

* <a href="#section1">T-SQL</a>
  * <a href="#section1-1">Querying with Transact-SQL</a>
  * <a href="#section1-2">Querying with Transact-SQL</a>
  * 

* <a href="#section2">Optimize query performance in Azure SQL</a>
  * <a href="#section2-1">Describe SQL Server query plans</a>
  * <a href="#section2-2">Explore performance-based design</a>
  * <a href="#section2-3">Evaluate performance improvements</a>
  
## <h2 id="section1">T-SQL</h2>

### <h3 id="section1-1">Querying with Transact-SQL</h3>

SQL supports some procedural syntax, but querying data with SQL usually follows declarative semantics. You use SQL to describe the results you want, and the database engine's query processor develops a query plan to retrieve it. The query processor uses statistics about the data in the database and indexes that are defined on the tables to come up with a good query plan.

The tables in the database are related to one another using key columns that uniquely identify the particular entity represented. A primary key is defined for each table, and a reference to this key is defined as a foreign key in any related table. 

Set theory is one of the mathematical foundations of the relational model of data management and is fundamental to working with relational databases. While you might be able to write queries in T-SQL without a thorough understanding of sets, you may eventually have difficulty writing some of the more complex types of statements that may be needed for optimum performance. You will see that the results of a SELECT statement also form a set. As you learn more about T-SQL query statements, it is important to always think of the entire set, instead of individual members. This mindset will better equip you to write set-based code, instead of thinking one row at a time. Working with sets requires thinking in terms of operations that occur "all at once" instead of one at a time.

One important feature to note about set theory is that there is no specification regarding any ordering of the members of a set. This lack of order applies to relational database tables. There is no concept of a first row, a second row, or a last row. Elements may be accessed (and retrieved) in any order. If you need to return results in a certain order, you must specify it explicitly by using an ORDER BY clause in your SELECT query.

* Work with schemas

  * In SQL Server database systems, tables are defined within schemas to create logical namespaces in the database. 
  * Database systems such as SQL Server use a hierarchical naming system. This multi-level naming helps to disambiguate tables with the same name in different schemas. The fully qualified name of an object includes the name of a database server instance in which the database is stored, the name of the database, the schema name, and the table name. For example: Server1.StoreDB.Sales.Order.
  * When working with tables within the context of a single database, it's common to refer to tables (and other objects) by including the schema name. For example, Sales.Order.
  * Work with data types (Data type conversion): CAST and TRY_CAST; CONVERT and TRY_CONVERT; PARSE and TRY_PARSE; STR

* Explore the structure of SQL statements

  * **Data Manipulation Language** (DML) is the set of SQL statements that focuses on querying and modifying data. DML statements include SELECT, the primary focus of this training, and modification statements such as INSERT, UPDATE, and DELETE.
  * **Data Definition Language** (DDL) is the set of SQL statements that handles the definition and life cycle of database objects, such as tables, views, and procedures. DDL includes statements such as CREATE, ALTER, and DROP.
  * **Data Control Language** (DCL) is the set of SQL statements used to manage security permissions for users and objects. DCL includes statements such as GRANT, REVOKE, and DENY.

 Sometimes you may also see TCL listed as a type of statement, to refer to **Transaction Control Language**. In addition, some lists may redefine DML as **Data Modification Language**, which wouldn't include SELECT statements, but then they add DQL as **Data Query Language** for SELECT statements. 

```
SELECT OrderDate, COUNT(OrderID) AS Orders
FROM Sales.SalesOrder
WHERE Status = 'Shipped'
GROUP BY OrderDate
HAVING COUNT(OrderID) > 1
ORDER BY OrderDate DESC;
```

> The query consists of a SELECT statement, which is composed of multiple clauses, each of which defines a specific operation that must be applied to the data being retrieved. 
> 
> The SELECT clause returns the columns/values; the FROM clause identifies which table is the source of the rows for the query; the WHERE clause filters rows out of the results, keeping only those rows that satisfy the specified condition; 
> 
> the GROUP BY clause takes the rows that met the filter condition and groups them by OrderDate, so that all the rows with the same OrderDate are considered as a single group and one row will be returned for each group; 
> 
> After the groups are formed, the HAVING clause filters the groups based on its own predicate. Only dates with more than one order will be included in the results;
> 
> For the purposes of previewing this query, the final clause is the ORDER BY, which sorts the output into descending order of OrderDate

Now that you've seen what each clause does, let's look at the order in which SQL Server actually evaluates them:

1. The FROM clause is evaluated first, to provide the source rows for the rest of the statement. A virtual table is created and passed to the next step.
2. The WHERE clause is next to be evaluated, filtering those rows from the source table that match a predicate. The filtered virtual table is passed to the next step.
3. GROUP BY is next, organizing the rows in the virtual table according to unique values found in the GROUP BY list. A new virtual table is created, containing the list of groups, and is passed to the next step. From this point in the flow of operations, only columns in the GROUP BY list or aggregate functions may be referenced by other elements.
4. The HAVING clause is evaluated next, filtering out entire groups based on its predicate. The virtual table created in step 3 is filtered and passed to the next step.
5. The SELECT clause finally executes, determining which columns will appear in the query results. Because the SELECT clause is evaluated after the other steps, any column aliases (in our example, Orders) created there cannot be used in the GROUP BY or HAVING clause.
6. The ORDER BY clause is the last to execute, sorting the rows as determined by its column list.


* Formatting queries

  You may note from the examples in this section that you can be flexible about how you format your query code. For example, you can write each clause (or the entire query) on a single line, or break it over multiple lines. In most database systems, the code is case-insensitive, and some elements of the T-SQL language are optional (including the AS keyword as mentioned previously, and even the semi-colon at the end of a statement). Consider the following guidelines to make your T-SQL code easily readable (and therefore easier to understand and debug!):

   * Capitalize T-SQL keywords, like SELECT, FROM, AS, and so on. Capitalizing keywords is a commonly used convention that makes it easier to find each clause of a complex statement.
   * Start a new line for each major clause of a statement.
   * If the SELECT list contains more than a few columns, expressions, or aliases, consider listing each column on its own line.
   * Indent lines containing subclauses or columns to make it clear which code belongs to each major clause.

#### Handle NULLs

  A NULL value means no value or unknown. It does not mean zero or blank, or even an empty string. Those values are not unknown. A NULL value can be used for values that haven’t been supplied yet, for example, when a customer has not yet supplied an email address. As you've seen previously, a NULL value can also be returned by some conversion functions if a value is not compatible with the target data type.

  You'll often need to take special steps to deal with NULL. NULL is really a non-value. It is unknown. It isn't equal to anything, and it’s not unequal to anything. NULL isn't greater or less than anything. We can’t say anything about what it is, but sometimes we need to work with NULL values. Thankfully, T-SQL provides functions for conversion or replacement of NULL values.
  
  * ISNULL

  The ISNULL function takes two arguments. The first is an expression we are testing. If the value of that first argument is NULL, the function returns the second argument. If the first expression is not null, it is returned unchanged.
  
  ```
  SELECT FirstName,
      ISNULL(MiddleName, 'None') AS MiddleIfAny,
      LastName
  FROM Sales.Customer;
  ```
  
  **Note**
  
  The value substituted for NULL must be the same datatype as the expression being evaluated. In the above example, MiddleName is a varchar, so the replacement value could not be numeric. In addition, you'll need to choose a value that will not appear in the data as a regular value. It can sometimes be difficult to find a value that will never appear in your data.
  
  * COALESCE

  The ISNULL function is not ANSI standard, so you may wish to use the COALESCE function instead. COALESCE is a little more flexible is that it can take a variable number of arguments, each of which is an expression. It will return the first expression in the list that is not NULL. If there are only two arguments, COALESCE behaves like ISNULL. However, with more than two arguments, COALESCE can be used as an alternative to a multipart CASE expression using ISNULL. If all arguments are NULL, COALESCE returns NULL. All the expressions must return the same or compatible data types. The syntax is as follows: `SELECT COALESCE(&lt;expression_1&gt;[, ...&lt;expression_n&gt;];`
  
  ```
  SELECT EmployeeID,
      COALESCE(HourlyRate * 40,
                WeeklySalary,
                Commission * SalesQty) AS WeeklyEarnings
   FROM HR.Wages;
   ```
   
  * NULLIF

  The NULLIF function allows you to return NULL under certain conditions. This function has useful applications in areas such as **data cleansing**, when you wish to replace blank or placeholder characters with NULL. NULLIF takes two arguments and returns NULL if they're equivalent. If they aren't equal, NULLIF returns the first argument. In this example, NULLIF replaces a discount of 0 with a NULL. It returns the discount value if it is not 0:

   ```
   SELECT SalesOrderID,
      ProductID,
      UnitPrice,
      NULLIF(UnitPriceDiscount, 0) AS Discount
   FROM Sales.SalesOrderDetail;
   ```

#### Sort and filter results in T-SQL
  
  In the logical order of query processing, ORDER BY is the last phase of a SELECT statement to be executed. ORDER BY enables you to control the sorting of rows as they are returned from SQL Server to the client application. SQL Server doesn't guarantee the physical order of rows in a table, and the only way to control the order the rows will be returned to the client is with an ORDER BY clause. This behavior is consistent with relational theory.
  
  ORDER BY can take several types of elements in its list:

  * Columns by name. You can specify the names of the column(s) by which the results should be sorted. The results are returned in order of the first column, and then subsorted by each additional column in order.
  * Column aliases. Because the ORDER BY is processed after the SELECT clause, it has access to aliases defined in the SELECT list.
  * Columns by ordinal position in the SELECT list. Using the position isn't recommended in your applications, because of diminished readability and the extra care required to keep the ORDER BY list up to date. However, for complex expressions in the SELECT list, using the position number can be useful during troubleshooting.
  * Columns not included in the SELECT list, but available from tables listed in the FROM clause. If the query uses a DISTINCT option, any columns in the ORDER BY list must be included in the SELECT list.
  * In addition to specifying which columns should be used to determine the sort order, you may also control the direction of the sort. You can use ASC for ascending (A-Z, 0-9) or DESC for descending (Z-A, 9-0). Ascending sorts are the default. Each column can have its own direction specified.

 **Limit the sorted results**:
 
 The TOP clause is a Microsoft-proprietary extension of the SELECT clause. TOP will let you specify how many rows to return, either as a positive integer or as a percentage of all qualifying rows. The number of rows can be specified as a constant or as an expression. TOP is most frequently used with an ORDER BY, but can be used with unordered data.
 
* Using the TOP clause
 
  The TOP operator depends on an ORDER BY clause to provide meaningful precedence to the rows selected. TOP can be used without ORDER BY, but in that case, there is no way to predict which rows will be returned. In this example, any 10 orders might be returned if there wasn’t an ORDER BY clause.
   
     ```
     SELECT TOP (N) <column_list>
     FROM <table_source>
     WHERE <search_condition>
     ORDER BY <order list> [ASC|DESC];
     ```
     
  * Using WITH TIES

  In addition to specifying a fixed number of rows to be returned, the TOP keyword also accepts the WITH TIES option, which will retrieve any rows with values that might be found in the selected top N rows.
   
  In the previous example, the query returned the first 10 products in descending order of price. However, by adding the WITH TIES option to the TOP clause, you will see that more rows qualify for inclusion in the top 10 most expensive products:
  
  ```
   SELECT TOP 10 WITH TIES Name, ListPrice
   FROM Production.Product
   ORDER BY ListPrice DESC;
   ```
   
  The decision to include WITH TIES will depend on your knowledge of the source data, its potential for unique values, and the requirements of the query you are writing.
   
  * Using PERCENT

  To return a percentage of the eligible rows, use the PERCENT option with TOP instead of a fixed number. The PERCENT may also be used with the WITH TIES option. For the purposes of row count, TOP (N) PERCENT will round up to the nearest integer. 
  
  The TOP option is used by many SQL Server professionals as a method for retrieving only a certain range of rows. However, consider the following facts when using TOP:

    * TOP is proprietary to T-SQL.
    * TOP on its own doesn't support skipping rows.
    * Because TOP depends on an ORDER BY clause, you cannot use one sort order to establish the rows filtered by TOP and another to determine the output order.
   
 * Page results

 An extension to the ORDER BY clause called OFFSET-FETCH enables you to return only a range of the rows selected by your query. It adds the ability to supply a starting point (an offset) and a value to specify how many rows you would like to return (a fetch value). This extension provides a convenient technique for paging through results.
 
 If you want to return rows a "page" at a time (using whatever number you choose for a page), you'll need to consider that each query with an OFFSET-FETCH clause runs independently of any other queries. There's no server-side state maintained, and you'll need to track your position through a result set via client-side code.

 **OFFSET-FETCH syntax**
 
 The syntax for the OFFSET-FETCH clause, which is technically part of the ORDER BY clause, is as follows:
 ```
 OFFSET { integer_constant | offset_row_count_expression } { ROW | ROWS }
 [FETCH { FIRST | NEXT } {integer_constant | fetch_row_count_expression } { ROW | ROWS } ONLY]
 ```
 
 **Using OFFSET-FETCH**
 
 To use OFFSET-FETCH, you'll supply a starting OFFSET value, which may be zero, and an optional number of rows to return, as in the following example:

 This example will return the first 10 rows, and then return the next 10 rows of product data based on the ListPrice:
 ```
 SELECT ProductID, ProductName, ListPrice
 FROM Production.Product
 ORDER BY ListPrice DESC 
 OFFSET 0 ROWS --Skip zero rows
 FETCH NEXT 10 ROWS ONLY; --Get the next 10
 ```
 To retrieve the next page of product data, use the OFFSET clause to specify the number of rows to skip:
 ```
 SELECT ProductID, ProductName, ListPrice
 FROM Production.Product
 ORDER BY ListPrice DESC 
 OFFSET 10 ROWS --Skip 10 rows
 FETCH NEXT 10 ROWS ONLY; --Get the next 10
 ```
 In the syntax definition you can see the OFFSET clause is required, but the FETCH clause is not. If the FETCH clause is omitted, all rows following OFFSET will be returned. You'll also find that the keywords ROW and ROWS are interchangeable, as are FIRST and NEXT, which enables a more natural syntax.

 To ensure the accuracy of the results, especially as you move from page to page of data, it's important to construct an ORDER BY clause that will provide unique ordering and yield a deterministic result. Because of the way SQL Server’s query optimizer works, it's technically possible for a row to appear on more than one page, unless the range of rows is deterministic.



### <h3 id="section1-2">Querying with Transact-SQL</h3>
  
## <h2 id="section2">Optimize query performance in Azure SQL</h2>

Analyze individual query performance and determine where improvements can be made. Explore performance-related Dynamic Management Objects. Investigate how indexes and database design affect queries.


### <h3 id="section2-1">Describe SQL Server query plans</h3>

Read and understand various forms of execution plans. Compare estimated vs actual plans. Learn how and why plans are generated.

### <h3 id="section2-2">Explore performance-based design</h3>

Explore normalization for relational databases. Investigate the impact of proper datatype usage. Compare types of indexes.

### <h3 id="section2-3">Evaluate performance improvements</h3>

Evaluate possible changes to indexes. Determine the impact of changes to queries and indexes. Explore Query Store hints.
