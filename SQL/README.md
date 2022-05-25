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

* **Handle NULLs**

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

  The NULLIF function allows you to return NULL under certain conditions. This function has useful applications in areas such as data cleansing, when you wish to replace blank or placeholder characters with NULL. NULLIF takes two arguments and returns NULL if they're equivalent. If they aren't equal, NULLIF returns the first argument. In this example, NULLIF replaces a discount of 0 with a NULL. It returns the discount value if it is not 0:

   ```
   SELECT SalesOrderID,
      ProductID,
      UnitPrice,
      NULLIF(UnitPriceDiscount, 0) AS Discount
   FROM Sales.SalesOrderDetail;
   ```



### <h3 id="section1-2">Querying with Transact-SQL</h3>
  
## <h2 id="section2">Optimize query performance in Azure SQL</h2>

Analyze individual query performance and determine where improvements can be made. Explore performance-related Dynamic Management Objects. Investigate how indexes and database design affect queries.


### <h3 id="section2-1">Describe SQL Server query plans</h3>

Read and understand various forms of execution plans. Compare estimated vs actual plans. Learn how and why plans are generated.

### <h3 id="section2-2">Explore performance-based design</h3>

Explore normalization for relational databases. Investigate the impact of proper datatype usage. Compare types of indexes.

### <h3 id="section2-3">Evaluate performance improvements</h3>

Evaluate possible changes to indexes. Determine the impact of changes to queries and indexes. Explore Query Store hints.
