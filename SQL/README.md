# T-SQL for Data Engineering

Microsoft database systems such as SQL Server, Azure SQL Database, Azure Synapse Analytics, and others use a dialect of SQL called Transact-SQL, or T-SQL. T-SQL includes language extensions for writing stored procedures and functions, which are application code that is stored in the database, and managing user accounts.

* <a href="#section1">T-SQL</a>
  * <a href="#section1-1">Querying with Transact-SQL</a>
  * <a href="#section1-2">Programming with Transact-SQL</a>
  * <a href="#section1-3">Write advanced Transact-SQL queries</a>

* <a href="https://docs.microsoft.com/en-ca/learn/paths/optimize-query-performance-sql-server/">Optimize query performance in Azure SQL</a>
  * <a href="#section2-1">Describe SQL Server query plans</a>
  * <a href="#section2-2">Explore query performance optimization</a>
  * <a href="#section2-3">Explore performance-based design</a>
  * <a href="#section2-4">Evaluate performance improvements</a>
  
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

 * Remove duplicates

 Although the rows in a table should always be unique, when you select only a subset of the columns, the result rows may not be unique even if the original rows are. By default, the SELECT clause includes an implicit ALL keyword that results in this behavior. T-SQL also supports an alternative the `DISTINCT` keyword, which removes any duplicate result rows.
 ```
 SELECT DISTINCT City, CountryRegion
 FROM Production.Supplier
 ORDER BY CountryRegion, City;
 ```
 
 * Filter data with predicates

 The simplest SELECT statements with only SELECT and FROM clauses will evaluate every row in a table. By using a WHERE clause, you define conditions that determine which rows will be processed and potentially reduce result set. 
 
 **The structure of the WHERE clause**
 
 The WHERE clause is made up of one or more search conditions, each of which must evaluate to TRUE, FALSE, or 'unknown' for each row of the table. Rows will only be returned when the WHERE clause evaluates as TRUE. *The individual conditions act as filters on the data, and are referred to as 'predicates'*. Each predicate includes a condition that is being tested, usually using the basic comparison operators:

  * `=` (equals)
  * `<>` (not equals)
  * `>` (greater than)
  * `>=` (greater than or equal to)
  * `<` (less than)
  * `<=` (less than or equal to)

  **IS NULL / IS NOT NULL**
  
  You can also easily filter to allow or exclude the 'unknown' or NULL values using IS NULL or IS NOT NULL.
  
  ```
  SELECT ProductCategoryID AS Category, ProductName
  FROM Production.Product
  WHERE ProductName IS NOT NULL;
  ```
  
  **Multiple conditions**
  
  Multiple predicates can be combined with the AND and OR operators and with parentheses. However SQL Server will only process two conditions at a time. All conditions must be TRUE when connecting multiple conditions with AND operator. When using OR operator to connect two conditions, one or both may be TRUE for the result set. AND operators are processed before OR operators, unless parentheses are used. For best practice, use parentheses when using more than two predicates. The following query returns products in category 2 OR 3 AND cost less than 10.00:
  ```
  SELECT ProductCategoryID AS Category, ProductName
  FROM Production.Product
  WHERE (ProductCategoryID = 2 OR ProductCategoryID = 3)
    AND (ListPrice < 10.00);
 ```
 
 **Comparison operators**
 
 Transact-SQL includes comparison operators that can help simplify the WHERE clause.

   * IN
   The IN operator is a shortcut for multiple equality conditions for the same column connected with OR. There's nothing wrong with using multiple OR conditions in a query, as in the following example:
   ```
   SELECT ProductCategoryID AS Category, ProductName
   FROM Production.Product
   WHERE ProductCategoryID IN (2, 3, 4);
   ```
   
   * BETWEEN
   BETWEEN is another shortcut that can be used when filtering for an upper and lower bound for the value instead of using two conditions with the AND operator. The following two queries are equivalent:
   ```
   SELECT ProductCategoryID AS Category, ProductName
   FROM Production.Product
   WHERE ListPrice BETWEEN 1.00 AND 10.00;
   ```
   
   The BETWEEN operator uses inclusive boundary values. Products with a price of either 1.00 or 10.00 would be included in the results. BETWEEN is also helpful when querying date fields. For example, the following query will include all product names modified between January 1, 2012 and December 31, 2012:
   ```
   SELECT ProductName, ListPrice, ModifiedDate
   FROM Production.Product
   WHERE ModifiedDate BETWEEN '2012-01-01 00:00:00.000' AND '2012-12-31 23:59:59.999';
   ```
   Basic comparison operators such as Greater Than (>) and Equals (=) are also accurate when only filtering by date:
   ```
   SELECT ProductName, ListPrice, ModifiedDate
   FROM Production.Product
   WHERE ModifiedDate >= '2012-01-01' 
    AND ModifiedDate < '2013-01-01';
   ```
   
   * LIKE
   The final comparison operator can only be used for character data and allows us to use wildcard characters and regular expression patterns. Wildcards allow us to specify partial strings. For example, you could use the following query to return all products with names that contain the word "mountain":
   ```
   SELECT Name, ListPrice
   FROM SalesLT.Product
   WHERE Name LIKE '%mountain%';
   ```
   The `%` wildcard represents any string of 0 or more characters, so the results include products with the word "mountain" anywhere in their name. You can use the `_ (underscore)` wildcard to represent a single character. You can also define complex patterns for strings that you want to find. For example, the following query searched for products with a name that starts with "Mountain-", then followed by:

     * three characters between 0 and 9
     * a space
     * any string
     * a comma
     * a space
     * two characters between 0 and 9
   
   ```
   SELECT ProductName, ListPrice
   FROM SalesLT.Product
   WHERE ProductName LIKE 'Mountain-[0-9][0-9][0-9] %, [0-9][0-9]';
   ```
   
#### Combine multiple tables with JOINs in T-SQL

Relational databases usually contain multiple tables that are linked by common key fields. This normalized design minimizes duplication of data, but means that you'll often need to write queries to retrieve related data from two or more tables.

The most fundamental and common method of combining data from multiple tables is to use a JOIN operation. Some people think of JOIN as a separate clause in a SELECT statement, but others think of it as part of the FROM clause. This module will mainly consider it to be part of the FROM clause. In this module, we'll discover how the FROM clause in a T-SQL SELECT statement creates intermediate virtual tables that will be consumed by later phases of the query.

**The FROM Clause and Virtual Tables**

If you’ve learned about the logical order of operations that are performed when SQL Server processes a query, you’ve seen that the FROM clause of a SELECT statement is the first clause to be processed. This clause determines which table or tables will be the source of rows for the query. The FROM can reference a single table or bring together multiple tables as the source of data for your query. You can think of the FROM clause as creating and populating a virtual table. This virtual table will hold the output of the FROM clause and be used by clauses of the SELECT statement that are applied later, such as the WHERE clause. As you add extra functionality, such as join operators, to a FROM clause, it will be helpful to think of the purpose of the FROM clause elements as either to add rows to, or remove rows from, the virtual table.

The virtual table created by a FROM clause is a logical entity only. In SQL Server, no physical table is created, whether persistent or temporary, to hold the results of the FROM clause, as it is passed to the WHERE clause or other parts of the query. The virtual table created by the FROM clause contains data from all of the joined tables. It can be useful to think of the results as sets, and conceptualize the join results as a Venn diagram.

In the ANSI SQL-89 standard, joins were specified by including multiple tables in the FROM clause in a comma-separated list. Any filtering to determine which rows to include were performed in the WHERE clause, like this:

```
SELECT p.ProductID, m.Name AS Model, p.Name AS Product
FROM SalesLT.Product AS p, SalesLT.ProductModel AS m
WHERE p.ProductModelID = m.ProductModelID;
```

This syntax is still supported by SQL Server, but because of the complexity of representing the filters for complex joins, it is not recommended. Additionally, if a WHERE clause is accidentally omitted, ANSI SQL-89-style joins can easily become Cartesian products and return an excessive number of result rows, causing performance problems, and possibly incorrect results.

When learning about writing multi-table queries in T-SQL, it's important to understand the concept of Cartesian products. In mathematics, a Cartesian product is the product of two sets. In databases, a Cartesian product is the result of combining every row in one table to every row of another table. 

The underlying result of a JOIN operation is a Cartesian product but for most T-SQL queries, a Cartesian product isn't the desired result. In T-SQL, a Cartesian product occurs when two input tables are joined without considering any relationships between them. With no information about relationships, the SQL Server query processor will return all possible combinations of rows. While this result can have some practical applications, such as generating test data, it's not typically useful and can have severe performance implications.

With the advent of the ANSI SQL-92 standard, support for the keywords JOIN and ON clauses was added. T-SQL also supports this syntax. Joins are represented in the FROM clause by using the appropriate JOIN operator. The logical relationship between the tables, which becomes a filter predicate, is specified in the ON clause.

The following example restates the previous query with the newer syntax:
```
SELECT p.ProductID, m.Name AS Model, p.Name AS Product
FROM SalesLT.Product AS p
JOIN SalesLT.ProductModel AS m
    ON p.ProductModelID = m.ProductModelID;
```    
**Note**
The ANSI SQL-92 syntax makes it more difficult to create accidental Cartesian products. Once the keyword JOIN has been added, a syntax error will be raised if an ON clause is missing, unless the JOIN is specified as a CROSS JOIN.

* Use inner joins

 The most frequent type of JOIN in T-SQL queries is INNER JOIN. Inner joins are used to solve many common business problems, especially in highly normalized database environments. To retrieve data that has been stored across multiple tables, you will often need to combine it via INNER JOIN queries. An INNER JOIN begins its logical processing phase as a Cartesian product, which is then filtered to remove any rows that don't match the predicate.

**Processing an INNER JOIN**

Let’s examine the steps by which SQL Server will logically process a JOIN query. Line numbers in the following hypothetical example are added for clarity:
```
1) SELECT emp.FirstName, ord.Amount
2) FROM HR.Employee AS emp 
3) JOIN Sales.SalesOrder AS ord
4) ON emp.EmployeeID = ord.EmployeeID;
```
As you should be aware, the FROM clause will be processed before the SELECT clause. Let’s track the processing, beginning with line 2:

  * The FROM clause specifies the **HR.Employee** table as one of the input tables, giving it the alias **emp**.
  * The JOIN operator in line 3 reflects the use of an INNER JOIN (the default type in T-SQL) and specifies **Sales.SalesOrder** as the other input table, which has an alias of **ord**.
  * SQL Server will perform a logical Cartesian join on these tables and pass the results as a virtual table to the next step. (The physical processing of the query may not actually perform the Cartesian product operation, depending on the optimizer's decisions. But it can be helpful to imagine the Cartesian product being created.)
  * Using the ON clause, SQL Server will filter the virtual table, keeping only those rows where an **EmployeeID** value from the **emp** table matches a **EmployeeID** in the **ord** table.
  * The remaining rows are left in the virtual table and handed off to the next step in the SELECT statement. In this example, the virtual table is next processed by the SELECT clause, and the two specified columns are returned to the client application.

 The result of the completed query is a list of employees and their order amounts. Employees that do not have any associated orders have been filtered out by the ON clause, as have any orders that happen to have a EmployeeID that doesn't correspond to an entry in the **HR.Employee table**.

 An INNER JOIN is the default type of JOIN, and the optional INNER keyword is implicit in the JOIN clause. When mixing and matching join types, it can be useful to specify the join type explicitly. When writing queries using inner joins, consider the following guidelines:
 * Table aliases are preferred, not only for the SELECT list, but also for writing the ON clause.
 * Inner joins may be performed on a single matching column, such as an OrderID, or on multiple matching attributes, such as the combination of OrderID and ProductID. Joins that specify multiple matching columns are called composite joins.
 * The order in which tables are listed in the FROM clause for an INNER JOIN doesn't matter to the SQL Server optimizer. Conceptually, joins will be evaluated from left to right. 
 * Use the JOIN keyword once for each pair of joined tables in the FROM list. For a two-table query, specify one join. For a three-table query, you'll use JOIN twice; once between the first two tables, and once again between the output of the JOIN between the first two tables and the third table.
 
 This next example shows how an inner join may be extended to include more than two tables. The Sales.SalesOrderDetail table is joined to the output of the JOIN between Production.Product and Production.ProductModel. Each instance of JOIN/ON does its own population and filtering of the virtual output table. The SQL Server query optimizer determines the order in which the joins and filtering will be performed.
 
 ```
 SELECT od.SalesOrderID, m.Name AS Model, p.Name AS ProductName, od.OrderQty
FROM Production.Product AS p
INNER JOIN Production.ProductModel AS m
    ON p.ProductModelID = m.ProductModelID
INNER JOIN Sales.SalesOrderDetail AS od
    ON p.ProductID = od.ProductID
ORDER BY od.SalesOrderID;
```

**Use outer joins**

However, like the INNER keyword, it is often helpful to write code that is explicit about the kind of join being used.  When writing queries using OUTER JOIN, consider the following guidelines:

  * As you have seen, table aliases are preferred not only for the SELECT list, but also for the ON clause.
  * As with an INNER JOIN, an OUTER JOIN may be performed on a single matching column or on multiple matching attributes.
  * Unlike an INNER JOIN, the order in which tables are listed and joined in the FROM clause does matter with OUTER JOIN, as it will determine whether you choose LEFT or RIGHT for your join.
  * Multi-table joins are more complex when an OUTER JOIN is present. The presence of NULLs in the results of an OUTER JOIN may cause issues if the intermediate results are then joined to a third table. Rows with NULLs may be filtered out by the second join's predicate.
  * To display only rows where no match exists, add a test for NULL in a WHERE clause following an OUTER JOIN predicate.
  * A FULL OUTER JOIN is used rarely. It returns all the matching rows between the two tables, plus all the rows from the first table with no match in the second, plus all the rows in the second without a match in the first.
  * There is no way to predict the order the rows will come back without an ORDER BY clause. There’s no way to know if the matched or unmatched rows will be returned first.
 ```
 SELECT emp.FirstName, ord.Amount
FROM HR.Employee AS emp
LEFT JOIN Sales.SalesOrder AS ord
    ON emp.EmployeeID = ord.EmployeeID;
 ```
 
 **Use cross joins**

 A cross join is simply a Cartesian product of the two tables. Using ANSI SQL-89 syntax, you can create a cross join by just leaving off the filter that connects the two tables. Using the ANSI-92 syntax, it’s a little harder; which is good, because in general, a cross join isn't something that you usually want. With the ANSI-92 syntax, it's highly unlikely you'll end up with a cross join accidentally.

 To explicitly create a Cartesian product, you use the CROSS JOIN operator. This operation creates a result set with all possible combinations of input rows:
 ```
 SELECT <select_list>
 FROM table1 AS t1
 CROSS JOIN table2 AS t2;
 ```
 While this result isn't typically a desired output, there are a few practical applications for writing an explicit CROSS JOIN:

  * Creating a table of numbers, with a row for each possible value in a range.
  * Generating large volumes of data for testing. When cross joined to itself, a table with as few as 100 rows can readily generate 10,000 output rows with little work from you.
 
 CROSS JOIN syntax
 When writing queries with CROSS JOIN, consider the following guidelines:

  * There is no matching of rows performed, and so no ON clause is used. (It is an error to use an ON clause with CROSS JOIN.)
  * To use ANSI SQL-92 syntax, separate the input table names with the CROSS JOIN operator.
 
 **Use self joins**

 So far, the joins we've used have involved different tables. There may be scenarios in which you need to retrieve and compare rows from a table with other rows from the same table. For example, in a human resources application, an Employee table might include information about the manager of each employee, and store the manager's ID in the employee's own row. Each manager is also listed as an employee. To retrieve the employee information and match it to the related manager, you can use the table twice in your query, joining it to itself for the purposes of the query.
 ```
 SELECT emp.FirstName AS Employee, 
       mgr.FirstName AS Manager
FROM HR.Employee AS emp
LEFT OUTER JOIN HR.Employee AS mgr
  ON emp.ManagerID = mgr.EmployeeID;
 ```
 The results of this query include a row for each employee with the name of their manager. The CEO of the company has no manager. To include the CEO in the results, an outer join is used, and the manager name is returned as NULL for rows where the ManagerID field has no matching EmployeeID field.
 
 There are other scenarios in which you'll want to compare rows in a table with different rows in the same table. As you've seen, it's fairly easy to compare columns in the same row using T-SQL, but the method to compare values from different rows (such as a row that stores a starting time, and another row in the same table that stores a corresponding stop time) is less obvious. Self-joins are a useful technique for these types of queries. To accomplish tasks like this, you should consider the following guidelines:

    * Define two instances of the same table in the FROM clause, and join them as needed, using inner or outer joins.
    * Use table aliases to differentiate the two instances of the same table.
    * Use the ON clause to provide a filter comparing columns of one instance of the table with columns from the other instance of the table.
 
 #### Write Subqueries in T-SQL
 
 Sometimes, when using Transact-SQL to retrieve data from a database, it can be easier to simplify complex queries by breaking them down into multiple simpler queries that can be combined to achieve the desired results. Transact-SQL supports the creation of subqueries, in which an inner query returns its result to an outer query. Being able to nest one query within another will enhance your ability to create effective queries in T-SQL. In general, subqueries are evaluated once, and provide their results to the outer query.
 
* Working with subqueries

  A subquery is a SELECT statement nested, or embedded, within another query. The nested query, which is the subquery, is referred to as the inner query. The query containing the nested query is the outer query. The purpose of a subquery is to return results to the outer query. The form of the results will determine whether the subquery is a scalar or multi-valued subquery:
  * Scalar subqueries return a single value. Outer queries must process a single result.
  * Multi-valued subqueries return a result much like a single-column table. Outer queries must be able to process multiple values.
  In addition to the choice between scalar and multi-valued subqueries, subqueries can either be self-contained subqueries or they can be correlated with the outer query:
  * Self-contained subqueries can be written as stand-alone queries, with no dependencies on the outer query. A self-contained subquery is processed once, when the outer query runs and passes its results to that outer query.
  * Correlated subqueries reference one or more columns from the outer query and therefore depend on it. Correlated subqueries cannot be run separately from the outer query.
 
* Use scalar or multi-valued subqueries

  A scalar subquery is an inner SELECT statement within an outer query, written to return a single value. Scalar subqueries may be used anywhere in an outer T-SQL statement where a single-valued expression is permitted—such as in a SELECT clause, a WHERE clause, a HAVING clause, or even a FROM clause. They can also be used in data modification statements, such as UPDATE or DELETE. Multi-valued subqueries, as the name suggests, can return more than one row. However they still return a single column.
  
  Suppose you want to retrieve the details of the last order that was placed, on the assumption that it is the one with the highest SalesOrderID value. To find the highest SalesOrderID value, you might use the following query:
  ```
  SELECT MAX(SalesOrderID)
  FROM Sales.SalesOrderHeader
  ```
  This query returns a single value that indicates the highest value for an OrderID in the SalesOrderHeader table. To get the details for this order, you might need to filter the SalesOrderDetails table based on whatever value is returned by the query above. You can accomplish this task by nesting the query to retrieve the maximum SalesOrderID within the WHERE clause of a query that retrieves the order details.
  ```
  SELECT SalesOrderID, ProductID, OrderQty
  FROM Sales.SalesOrderDetail
  WHERE SalesOrderID = 
   (SELECT MAX(SalesOrderID)
    FROM Sales.SalesOrderHeader);
  ```
  To write a scalar subquery, consider the following guidelines:
    * To denote a query as a subquery, enclose it in parentheses.
    * Multiple levels of subqueries are supported in Transact-SQL. In this module, we'll only consider two-level queries (one inner query within one outer query), but up to 32 levels are supported.
    * If the subquery returns no rows (an empty set), the result of the subquery is a NULL. If it is possible in your scenario for no rows to be returned, you should ensure your outer query can gracefully handle a NULL, in addition to other expected results.
    * The inner query should generally return a single column. Selecting multiple columns in a subquery is almost always an error. The only exception is if the subquery is introduced with the EXISTS keyword.

 A scalar subquery can be used anywhere in a query where a value is expected, including the SELECT list. For example, we could extend the query that retrieved details for the most recent order to include the average quantity of items that is ordered, so we can compare the quantity ordered in the most recent order with the average for all orders.

  ```
  SELECT SalesOrderID, ProductID, OrderQty,
    (SELECT AVG(OrderQty)
     FROM SalesLT.SalesOrderDetail) AS AvgQty
  FROM SalesLT.SalesOrderDetail
  WHERE SalesOrderID = 
    (SELECT MAX(SalesOrderID)
     FROM SalesLT.SalesOrderHeader);
  ```   
  
* Multi-valued subqueries
  
 A multi-valued subquery is well suited to return results using the IN operator. The following hypothetical example returns the CustomerID, SalesOrderID values for all orders placed by customers in Canada.
  ```
  SELECT CustomerID, SalesOrderID
  FROM Sales.SalesOrderHeader
  WHERE CustomerID IN (
    SELECT CustomerID
    FROM Sales.Customer
    WHERE CountryRegion = 'Canada');
  ```
In this example, if you were to execute only the inner query, a column of CustomerID values would be returned, with a row for each customer in Canada. In many cases, multi-valued subqueries can easily be written using joins. For example, here's a query that uses a join to return the same results as the previous example:
  ```
  SELECT c.CustomerID, o.SalesOrderID
  FROM Sales.Customer AS c
  JOIN Sales.SalesOrderHeader AS o
    ON c.CustomerID = o.CustomerID
  WHERE c. CountryRegion = 'Canada';
  ```
 So how do you decide whether to write a query involving multiple tables as a JOIN or with a subquery? Sometimes, it just depends on what you’re more comfortable with. Most nested queries that are easily converted to JOINs will actually BE converted to a JOIN internally. For such queries, there is then no real difference in writing the query one way vs another.

 One restriction you should keep in mind is that when using a nested query, the results returned to the client can only include columns from the outer query. So if you need to return columns from both tables, you should write the query using a JOIN.

 Finally, there are situations where the inner query needs to perform much more complicated operations that the simple retrievals in our examples. Rewriting complex subqueries using a JOIN can be difficult. Many SQL developers find subqueries work best for complicated processing because it allows you to break down the processing into smaller steps.

 * Use self-contained or correlated subqueries

 Previously, we looked at self-contained subqueries; in which the inner query is independent of the outer query, executes once, and returns its results to the outer query. T-SQL also supports correlated subqueries, in which the inner query references column in the outer query and conceptually executes once per row.
 
 **Working with correlated subqueries**
 
 Like self-contained subqueries, correlated subqueries are SELECT statements nested within an outer query. Correlated subqueries may also be either scalar or multi-valued subqueries. They're typically used when the inner query needs to reference a value in the outer query. However, unlike self-contained subqueries, there are some special considerations when using correlated subqueries:

    * Correlated subqueries cannot be executed separately from the outer query. This restriction complicates testing and debugging.
    * Unlike self-contained subqueries, which are processed once, correlated subqueries will run multiple times. Logically, the outer query runs first, and for each row returned, the inner query is processed.

 The following example uses a correlated subquery to return the most recent order for each customer. The subquery refers to the outer query and references its CustomerID value in its WHERE clause. For each row in the outer query, the subquery finds the maximum order ID for the customer referenced in that row, and the outer query checks to see if the row it’s looking at is the row with that order ID.
 ```
 SELECT SalesOrderID, CustomerID, OrderDate
 FROM SalesLT.SalesOrderHeader AS o1
 WHERE SalesOrderID =
    (SELECT MAX(SalesOrderID)
     FROM SalesLT.SalesOrderHeader AS o2
     WHERE o2.CustomerID = o1.CustomerID)
 ORDER BY CustomerID, OrderDate;
 ```
 
 *Writing correlated subqueries*
 
 To write correlated subqueries, consider the following guidelines:

    * Write the outer query to accept the appropriate return result from the inner query. If the inner query is scalar, you can use equality and comparison operators, such as =, <, >, and <>, in the WHERE clause. If the inner query might return multiple values, use an IN predicate. Plan to handle NULL results.
    * Identify the column from the outer query that will be referenced by the correlated subquery. Declare an alias for the table that is the source of the column in the outer query.
    * Identify the column from the inner table that will be compared to the column from the outer table. Create an alias for the source table, as you did for the outer query.
    * Write the inner query to retrieve values from its source, based on the input value from the outer query. For example, use the outer column in the WHERE clause of the inner query.

 The correlation between the inner and outer queries occurs when the outer value is referenced by the inner query for comparison. It’s this correlation that gives the subquery its name.

 *Working with EXISTS*
 
 In addition to retrieving values from a subquery, T-SQL provides a mechanism for checking whether any results would be returned from a query. The EXISTS predicate determines whether any rows meeting a specified condition exist, but rather than return them, it returns TRUE or FALSE. This technique is useful for validating data without incurring the overhead of retrieving and processing the results.

 When a subquery is related to the outer query using the EXISTS predicate, SQL Server handles the results of the subquery in a special way. Rather than retrieve a scalar value or a multi-valued list from the subquery, EXISTS simply checks to see if there are any rows in the result.

 Conceptually, an EXISTS predicate is equivalent to retrieving the results, counting the rows returned, and comparing the count to zero. Compare the following queries, which will return details about customers who have placed orders:

 The first example query uses COUNT in a subquery:
 ```
 SELECT CustomerID, CompanyName, EmailAddress 
 FROM Sales.Customer AS c 
 WHERE
  (SELECT COUNT(*) 
  FROM Sales.SalesOrderHeader AS o
  WHERE o.CustomerID = c.CustomerID) > 0;
 ```
 The second query, which returns the same results, uses EXISTS:
 ```
 SELECT CustomerID, CompanyName, EmailAddress 
 FROM Sales.Customer AS c 
 WHERE EXISTS
  (SELECT * 
   FROM Sales.SalesOrderHeader AS o
   WHERE o.CustomerID = c.CustomerID);
 ```
 
 In the first example, the subquery must count every occurrence of each custid found in the Sales.Orders table, and compare the count results to zero, simply to indicate that the customer has placed orders.

 In the second query, EXISTS returns TRUE for a custid as soon as a relevant order has been found in the Sales.Orders table. A complete accounting of each occurrence is unnecessary. Also note that with the EXISTS form, the subquery is not restricted to returning a single column. Here, we have SELECT *. The returned columns are irrelevant because we’re only checking if any rows are returned at all, not what values are in those rows.

 From the perspective of logical processing, the two query forms are equivalent. From a performance perspective, the database engine may treat the queries differently as it optimizes them for execution. Consider testing each one for your own usage.

 **Note**

  If you're converting a subquery using COUNT(*) to one using EXISTS, make sure the subquery uses a SELECT * and not SELECT COUNT(*). SELECT COUNT(*) always returns a row, so the EXISTS will always return TRUE.

  Another useful application of EXISTS is negating the subquery with NOT, as in the following example, which will return any customer who has never placed an order:
  ```
  SELECT CustomerID, CompanyName, EmailAddress 
  FROM SalesLT.Customer AS c 
  WHERE NOT EXISTS
    (SELECT * 
     FROM SalesLT.SalesOrderHeader AS o
     WHERE o.CustomerID = c.CustomerID);
  ```
  
 SQL Server won't have to return data about the related orders for customers who have placed orders. If a custid is found in the Sales.Orders table, NOT EXISTS evaluates to FALSE and the evaluation quickly completes. To write queries that use EXISTS with subqueries, consider the following guidelines:

    * The keyword EXISTS directly follows WHERE. No column name (or other expression) precedes it, unless NOT is also used.
    * Within the subquery, use SELECT *. No rows are returned by the subquery, so no columns need to be specified.

#### Use built-in functions and GROUP BY in Transact-SQL

When retrieving data from tables in a database, it's often useful to be able to manipulate data values by using functions; to format, convert, aggregate, or otherwise affect the output from the query. Additionally, when aggregating data, you'll often want to group the results and show aggregations for each group - for example, to see total values by category.

Transact-SQL includes many built-in functions, ranging from functions that perform data type conversion, to functions that aggregate and analyze groups of rows.

Functions in T-SQL can be categorized as follows:
```
|Function Category  | Description
|Scalar             | Operate on a single row, return a single value.
|Logical            | Compare multiple values to determine a single output.
|Ranking            | Operate on a partition (set) of rows.
|Rowset             | Return a virtual table that can be used in a FROM clause in a T-SQL statement.
|Aggregate          | Take one or more input values, return a single summarizing value.
```
**Use scalar functions**

Scalar functions return a single value and usually work on a single row of data. The number of input values they take can be zero (for example, GETDATE), one (for example, UPPER), or multiple (for example, ROUND). Because scalar functions always return a single value, they can be used anywhere a single value (the result) is needed. They are most commonly used in SELECT clauses and WHERE clause predicates. They can also be used in the SET clause of an UPDATE statement.

Built-in scalar functions can be organized into many categories, such as string, conversion, logical, mathematical, and others. This module will look at a few common scalar functions.

Some considerations when using scalar functions include:

 * Determinism: If the function returns the same value for the same input and database state each time it is called, we say it is deterministic. For example, ROUND(1.1, 0) always returns the value 1.0. Many built-in functions are nondeterministic. For example, GETDATE() returns the current date and time. Results from nondeterministic functions cannot be indexed, which affects the query processor's ability to come up with a good plan for executing the query.
 * Collation: When using functions that manipulate character data, which collation will be used? Some functions use the collation (sort order) of the input value; others use the collation of the database if no input collation is supplied.

Scalar function examples

At the time of writing, the SQL Server Technical Documentation listed more than 200 scalar functions that span multiple categories, including:

 * Configuration functions
 * Conversion functions
 * Cursor functions
 * Date and Time functions
 * Mathematical functions
 * Metadata functions
 * Security functions
 * String functions
 * System functions
 * System Statistical functions
 * Text and Image functions

There isn't enough time in this course to describe each function, but the examples below show some commonly used functions. The following hypothetical example uses several date and time functions:
```
SELECT  SalesOrderID,
    OrderDate,
        YEAR(OrderDate) AS OrderYear,
        DATENAME(mm, OrderDate) AS OrderMonth,
        DAY(OrderDate) AS OrderDay,
        DATENAME(dw, OrderDate) AS OrderWeekDay,
        DATEDIFF(yy,OrderDate, GETDATE()) AS YearsSinceOrder
FROM Sales.SalesOrderHeader;
```

The next example includes some mathematical functions:
```
SELECT TaxAmt,
       ROUND(TaxAmt, 0) AS Rounded,
       FLOOR(TaxAmt) AS Floor,
       CEILING(TaxAmt) AS Ceiling,
       SQUARE(TaxAmt) AS Squared,
       SQRT(TaxAmt) AS Root,
       LOG(TaxAmt) AS Log,
       TaxAmt * RAND() AS Randomized
FROM Sales.SalesOrderHeader;
```

The following example uses some string functions:
```
SELECT  CompanyName,
        UPPER(CompanyName) AS UpperCase,
        LOWER(CompanyName) AS LowerCase,
        LEN(CompanyName) AS Length,
        REVERSE(CompanyName) AS Reversed,
        CHARINDEX(' ', CompanyName) AS FirstSpace,
        LEFT(CompanyName, CHARINDEX(' ', CompanyName)) AS FirstWord,
        SUBSTRING(CompanyName, CHARINDEX(' ', CompanyName) + 1, LEN(CompanyName)) AS RestOfName
FROM Sales.Customer;
```

Logical functions

Another category of functions allows to to determine which of several values is to be returned. Logical functions evaluate an input expression, and return an appropriate value based on the result.

**IIF**

The IIF function evaluates a Boolean input expression, and returns a specified value if the expression evaluates to True, and an alternative value if the expression evaluates to False.

For example, consider the following query, which evaluates the address type of a customer. If the value is "Main Office", the expression returns "Billing". For all other address type values, the expression returns "Mailing".
```
SELECT AddressType,
      IIF(AddressType = 'Main Office', 'Billing', 'Mailing') AS UseAddressFor
FROM Sales.CustomerAddress;
```

**CHOOSE**

The CHOOSE function evaluates an integer expression, and returns the corresponding value from a list based on its (1-based) ordinal position.
```
SELECT SalesOrderID, Status,
CHOOSE(Status, 'Ordered', 'Shipped', 'Delivered') AS OrderStatus
FROM Sales.SalesOrderHeader;
```

#### Use ranking and rowset functions

Ranking and rowset functions aren't scalar functions because they don't return a single value. These functions work accept a set of rows as input and return a set of rows as output.

**Ranking functions**
Ranking functions allow you to perform calculations against a user-defined set of rows. These functions include ranking, offset, aggregate, and distribution functions.

This example uses the RANK function to calculate a ranking based on the ListPrice, with the highest price ranked at 1:
```
SELECT TOP 100 ProductID, Name, ListPrice,
RANK() OVER(ORDER BY ListPrice DESC) AS RankByPrice
FROM Production.Product AS p
ORDER BY RankByPrice;
```

**OVER**
You can use the OVER clause to define partitions, or groupings within the data. For example, the following query extends the previous example to calculate price-based rankings for products within each category.
```
SELECT c.Name AS Category, p.Name AS Product, ListPrice,
  RANK() OVER(PARTITION BY c.Name ORDER BY ListPrice DESC) AS RankByPrice
FROM Production.Product AS p
JOIN Production.ProductCategory AS c
ON p.ProductCategoryID = c.ProductcategoryID
ORDER BY Category, RankByPrice;
```

**Note**
Notice that several rows have the same rank value and some values are skipped. This is because we are using RANK only. Depending on the requirement, you may want to avoid ties at the same rank value. You can control the rank value with other functions, DENSE_RANK, NTILE, and ROW_NUMBER, as needed. For details on these functions, see the <a href="https://docs.microsoft.com/en-us/sql/t-sql/functions/ranking-functions-transact-sql?view=sql-server-ver16">Transact-SQL reference documentation</a>.

**Rowset functions**
Rowset functions return a virtual table that can be used in the FROM clause as a data source. These functions take parameters specific to the rowset function itself. They include OPENDATASOURCE, OPENQUERY, OPENROWSET, OPENXML, and OPENJSON.

The OPENDATASOURCE, OPENQUERY, and OPENROWSET functions enable you to pass a query to a remote database server. The remote server will then return a set of result rows. For example, the following query uses OPENROWSET to get the results of a query from a SQL Server instance named SalesDB.
```
SELECT a.*
FROM OPENROWSET('SQLNCLI', 'Server=SalesDB;Trusted_Connection=yes;',
    'SELECT Name, ListPrice
    FROM AdventureWorks.Production.Product') AS a;
```
To use remote servers, you must enable some advanced options in the SQL Server instance where you're running the query. The OPENXML and OPENJSON functions enable you to query structured data in XML or JSON format and extract values into a tabular rowset. A detailed exploration of rowset functions is beyond the scope of this module. For more information, see the <a href="https://docs.microsoft.com/en-us/sql/t-sql/functions/functions?view=sql-server-ver16">Transact-SQL reference documentation</a>.
```
USE AdventureWorks2012;  
GO  
SELECT p.FirstName, p.LastName  
    ,ROW_NUMBER() OVER (ORDER BY a.PostalCode) AS "Row Number"  
    ,RANK() OVER (ORDER BY a.PostalCode) AS Rank  
    ,DENSE_RANK() OVER (ORDER BY a.PostalCode) AS "Dense Rank"  
    ,NTILE(4) OVER (ORDER BY a.PostalCode) AS Quartile  
    ,s.SalesYTD  
    ,a.PostalCode  
FROM Sales.SalesPerson AS s   
    INNER JOIN Person.Person AS p   
        ON s.BusinessEntityID = p.BusinessEntityID  
    INNER JOIN Person.Address AS a   
        ON a.AddressID = p.BusinessEntityID  
WHERE TerritoryID IS NOT NULL AND SalesYTD <> 0;
```

**Use aggregate functions**

T-SQL provides aggregate functions such as SUM, MAX, and AVG to perform calculations that take multiple values and return a single result.

*Working with aggregate functions*

Most of the queries we have looked at operate on a row at a time, using a WHERE clause to filter rows. Each row returned corresponds to one row in the original data set. Many aggregate functions are provided in SQL Server. In this section, we’ll look at the most common functions such as SUM, MIN, MAX, AVG, and COUNT.

When working with aggregate functions, you need to consider the following points:

 * Aggregate functions return a single (scalar) value and can be used in SELECT statements almost anywhere a single value can be used. For example, these functions can be used in the SELECT, HAVING, and ORDER BY clauses. However, they cannot be used in the WHERE clause.
 * Aggregate functions ignore NULLs, except when using COUNT(*).
 * Aggregate functions in a SELECT list don't have a column header unless you provide an alias using AS.
 * Aggregate functions in a SELECT list operate on all rows passed to the SELECT operation. If there is no GROUP BY clause, all rows satisfying any filter in the   WHERE clause will be summarized. You will learn more about GROUP BY in the next topic.
 * Unless you're using GROUP BY, you shouldn't combine aggregate functions with columns not included in functions in the same SELECT list.

To extend beyond the built-in functions, SQL Server provides a mechanism for user-defined aggregate functions via the .NET Common Language Runtime (CLR). That topic is beyond the scope of this module.

*Built-in aggregate functions*
As mentioned, Transact-SQL provides many built-in aggregate functions. Commonly used functions include:
```
Function Name      Syntax             Description
SUM                SUM(expression)    Totals all the non-NULL numeric values in a column.
AVG                AVG(expression)    Averages all the non-NULL numeric values in a column (sum/count).
MIN                MIN(expression)    Returns the smallest number, earliest date/time, or first-occurring string (according to collation sort rules).
MAX                MAX(expression)    Returns the largest number, latest date/time, or last-occurring string (according to collation sort rules).
COUNT or COUNT_BIG COUNT(*) or COUNT(expression) With (*), counts all rows, including rows with NULL values. When a column is specified as expression, returns the count of non-NULL rows for that column. COUNT returns an int; COUNT_BIG returns a big_int.
```

To use a built-in aggregate in a SELECT clause, consider the following example in the MyStore sample database:
```
SELECT AVG(ListPrice) AS AveragePrice,
       MIN(ListPrice) AS MinimumPrice,
       MAX(ListPrice) AS MaximumPrice
FROM Production.Product
WHERE ProductCategoryID = 15;
```

When using aggregates in a SELECT clause, all columns referenced in the SELECT list must be used as inputs for an aggregate function, or be referenced in a GROUP BY clause.

Consider the following query, which attempts to include the ProductCategoryID field in the aggregated results:
```
SELECT ProductCategoryID, AVG(ListPrice) AS AveragePrice,
MIN(ListPrice) AS MinimumPrice,
MAX(ListPrice) AS MaximumPrice
FROM Production.Product;

Running this query results in the following error
Msg 8120, Level 16, State 1, Line 1
Column 'Production.ProductCategoryID' is invalid in the select list because it isn't contained in either an aggregate function or the GROUP BY clause.
```

The query treats all rows as a single aggregated group. Therefore, all columns must be used as inputs to aggregate functions.

In the previous examples, we aggregated numeric data such as the price and quantities in the previous example,. Some of the aggregate functions can also be used to summarize date, time, and character data. The MIN and MAX functions can also be used with date data, to return the earliest and latest chronological values. However, AVG and SUM can only be used for numeric data, which includes integers, money, float and decimal datatypes.

*Using DISTINCT with aggregate functions*
You should be aware of the use of DISTINCT in a SELECT clause to remove duplicate rows. When used with an aggregate function, DISTINCT removes duplicate values from the input column before computing the summary value. DISTINCT is useful when summarizing unique occurrences of values, such as customers in the orders table.

The following example returns the number of customers who have placed orders, no matter how many orders they placed:
```
SELECT COUNT(DISTINCT CustomerID) AS UniqueCustomers
FROM Sales.SalesOrderHeader;
COUNT(<some_column>) merely counts how many rows have some value in the column. If there are no NULL values, COUNT(<some_column>) will be the same as COUNT(*). COUNT (DISTINCT <some_column>) counts how many different values there are in the column.
```

*Using aggregate functions with NULL*
It is important to be aware of the possible presence of NULLs in your data, and of how NULL interacts with T-SQL query components, including aggregate function. There are a few considerations to be aware of:

 * With the exception of COUNT used with the (*) option, T-SQL aggregate functions ignore NULLs. For example, a SUM function will add only non-NULL values. NULLs don't evaluate to zero. COUNT(*) counts all rows, regardless of value or non-value in any column.
 * The presence of NULLs in a column may lead to inaccurate computations for AVG, which will sum only populated rows and divide that sum by the number of non-NULL rows. There may be a difference in results between AVG(<column>) and (SUM(<column>)/COUNT(*)).

If you need to summarize all rows, whether NULL or not, consider replacing the NULLs with another value that will not be ignored by your aggregate function. You can use the COALESCE function for this purpose.

**Summarize data with GROUP BY**
 
While aggregate functions are useful for analysis, you may wish to arrange your data into subsets before summarizing it. In this section, you will learn how to accomplish this using the GROUP BY clause.

*Using the GROUP BY clause*
As you've learned, when your SELECT statement is processed, after the FROM clause and WHERE clause have been evaluated, a virtual table is created. The contents of the virtual table are now available for further processing. You can use the GROUP BY clause to subdivide the contents of this virtual table into groups of rows.

To group rows, specify one or more elements in the GROUP BY clause:
```
GROUP BY <value1< [, <value2>, …]
```

 GROUP BY creates groups and places rows into each group as determined by the elements specified in the clause. 

 After the GROUP BY clause has been processed and each row has been associated with a group, later phases of the query must aggregate any elements of the source rows that are in the SELECT list but that don't appear in the GROUP BY list. This requirement will have an impact on how you write your SELECT and HAVING clauses.

So, what’s the difference between writing the query with a GROUP BY or a DISTINCT? If all you want to know is the distinct values for CustomerID, there is no difference. But with GROUP BY, we can add other elements to the SELECT list that are then aggregated for each group.

The simplest aggregate function is COUNT(*). The following query takes the original 830 source rows from CustomerID and groups them into 89 groups, based on the CustomerID values. Each distinct CustomerID value generates one row of output in the GROUP BY query

```
SELECT CustomerID, COUNT(*) AS OrderCount
FROM Sales.SalesOrderHeader
GROUP BY CustomerID;
```
 
For each CustomerID value, the query aggregates and counts the rows, so we result shows us how many rows in the SalesOrderHeader table belong to each customer. Note that GROUP BY does not guarantee the order of the results. Often, as a result of the way the grouping operation is performed by the query processor, the results are returned in the order of the group values. However, you should not rely on this behavior. If you need the results to be sorted, you must explicitly include an ORDER clause.
 
**The clauses in a SELECT statement are applied in the following order:**

1. FROM
2. WHERE
3. GROUP BY
4. HAVING
5. SELECT
6. ORDER BY
 
Column aliases are assigned in the SELECT clause, which occurs after the GROUP BY clause but before the ORDER BY clause. You can reference a column alias in the ORDER BY clause, but not in the GROUP BY clause. The following query will result in an invalid column name error:
 
```
SELECT CustomerID AS Customer,
       COUNT(*) AS OrderCount
FROM Sales.SalesOrderHeader
GROUP BY Customer
ORDER BY Customer;
```
 
However, the following query will succeed, grouping and sorting the results by the customer ID.

```
SELECT CustomerID AS Customer,
       COUNT(*) AS OrderCount
FROM Sales.SalesOrderHeader
GROUP BY CustomerID
ORDER BY Customer; 
```
 
**Troubleshooting GROUP BY errors**
 
A common obstacle to becoming comfortable with using GROUP BY in SELECT statements is understanding why the following type of error message occurs:

```
Msg 8120, Level 16, State 1, Line 2 Column <column_name> is invalid in the select list because it is not contained in either an aggregate function or the GROUP BY clause.
```
 
For example, the following query is permitted because each column in the SELECT list is either a column in the GROUP BY clause or an aggregate function operating on each group:

```
SELECT CustomerID, COUNT(*) AS OrderCount
FROM Sales.SalesOrderHeader
GROUP BY CustomerID;
```
 
The following query will return an error because PurchaseOrderNumber isn't part of the GROUP BY, and it isn't used with an aggregate function.

```
SELECT CustomerID, PurchaseOrderNumber, COUNT(*) AS OrderCount
FROM Sales.SalesOrderHeader
GROUP BY CustomerID;
```
 
This query returns the error:

```
Msg 8120, Level 16, State 1, Line 1
Column 'Sales.SalesOrderHeader.PurchaseOrderNumber' is invalid in the select list because it is not contained in either an aggregate function or the GROUP BY clause.
```

Here’s another way to think about it. This query returns one row for each CustomerID value. But rows for the same CustomerID can have different PurchaseOrderNumber values, so which of the values is the one that should be returned?

If you want to see orders per customer ID and per purchase order, you can add the PurchaseOrderNumber column to the GROUP BY clause, as follows:

```
SELECT CustomerID, PurchaseOrderNumber, COUNT(*) AS OrderCount
FROM Sales.SalesOrderHeader
GROUP BY CustomerID, PurchaseOrderNumber;
```

This query will return one row for each customer and each purchase order combination, along with the count of orders for that combination.

#### Filter groups with HAVING

When you have created groups with a GROUP BY clause, you can further filter the results. The HAVING clause acts as a filter on groups. This is similar to the way that the WHERE clause acts as a filter on rows returned by the FROM clause.

A HAVING clause enables you to create a search condition, conceptually similar to the predicate of a WHERE clause, which then tests each group returned by the GROUP BY clause.

The following example counts the orders for each customer, and filters the results to include only customers that have placed more than 10 orders:

```
SELECT CustomerID,
      COUNT(*) AS OrderCount
FROM Sales.SalesOrderHeader
GROUP BY CustomerID
HAVING COUNT(*) > 10;
```
 
*Compare HAVING to WHERE*
 
While both HAVING and WHERE clauses filter data, remember that WHERE operates on rows returned by the FROM clause. If a GROUP BY ... HAVING section exists in your query following a WHERE clause, the WHERE clause will filter rows before GROUP BY is processed—potentially limiting the groups that can be created.

A HAVING clause is processed after GROUP BY and only operates on groups, not detail rows. To summarize:

 * A WHERE clause filters rows before any groups are formed
 * A HAVING clause filters entire groups, and usually looks at the results of an aggregation. 
 
####  Modify data with T-SQL
 
In many cases, data analysts and business users simply need to retrieve data from a database for reporting or analysis. However, when developing an application, or even during some complex analysis, you may need to insert, modify, or delete data.
 
* Insert data

Transact-SQL provides multiple ways to insert rows into a table.

*The INSERT statement*
 
The INSERT statement is used to add one or more rows to a table. There are several forms of the statement.

The basic syntax of a simple INSERT statement is shown below:
```
INSERT [INTO] <Table> [(column_list)]
VALUES ([ColumnName or an expression or DEFAULT or NULL],…n)
``` 
With this form of the INSERT statement, called INSERT VALUES, you can specify the columns that will have values placed in them and the order in which the data will be presented for each row inserted into the table. The column_list is optional but recommended. Without the column_list, the INSERT statement will expect a value for every column in the table in the order in which the columns were defined. You can also provide the values for those columns as a comma-separated list.

When listing values, the keyword DEFAULT means a predefined value, that was specified when the table was created, will be used. There are three ways a default can be determined:

    * If a column has been defined to have an automatically generated value, that value will be used. Autogenerated values will be discussed later in this module.
    * When a table is created, a default value can be supplied for a column, and that value will be used if DEFAULT is specified.
    * If a column has been defined to allow NULL values, and the column isn't an autogenerated column and doesn't have a default defined, NULL will be inserted as a DEFAULT.
    * The details of table creation are beyond the scope of this module. However, it is often useful to see what columns are in a table. The easiest way is to just execute a SELECT statement on the table without returning any rows. By using a WHERE condition that can never be TRUE, no rows can be returned.
 
Suppose that the table is defined such that a default value of the current date is applied to the StartDate column, and the Notes column allows NULL values. You can indicate that you want to use these values explicitly. Alternatively, you can omit values in the INSERT statement, in which case the default value will be used if defined, and if there is no default value but the column allows NULLs, then a NULL will be inserted. If you’re not supplying values for all columns, you must have a column list indicated which column values you're supplying. In addition to inserting a single row at a time, the INSERT VALUES statement can be used to insert multiple rows by providing multiple comma-separated sets of values. The sets of values are also separated by commas. 
 
**INSERT ... SELECT**
 
In addition to specifying a literal set of values in an INSERT statement, T-SQL also supports using the results of other operations to provide values for INSERT. You can use the results of a SELECT statement or the output of a stored procedure to supply the values for the INSERT statement.

To use the INSERT with a nested SELECT, build a SELECT statement to replace the VALUES clause. With this form, called INSERT SELECT, you can insert the set of rows returned by a SELECT query into a destination table. The use of INSERT SELECT presents the same considerations as INSERT VALUES:

    * You may optionally specify a column list following the table name.
    * You must provide column values or DEFAULT, or NULL, for each column.

 The following syntax illustrates the use of INSERT SELECT:
```
INSERT [INTO] <table or view> [(column_list)]
SELECT <column_list> FROM <table_list>...;
``` 
 
 **Note**

Result sets from stored procedures (or even dynamic batches) may also be used as input to an INSERT statement. This form of INSERT, called INSERT EXEC, is conceptually similar to INSERT SELECT and will present the same considerations. However, stored procedures can return multiple result sets, so extra care is needed.

The following example inserts multiple rows for a new promotion named Get Framed by retrieving the model ID and model name from the Production.ProductModel, table for every model that contains "frame" in its name.

```
INSERT INTO Sales.Promotion (PromotionName, ProductModelID, Discount, Notes)
SELECT DISTINCT 'Get Framed', m.ProductModelID, 0.1, '10% off ' + m.Name
FROM Production.ProductModel AS m
WHERE m.Name LIKE '%frame%';
```
 
Unlike a subquery, the nested SELECT used with an INSERT isn't enclosed in parentheses.

**SELECT ... INTO**
 
Another option for inserting rows, which is similar to INSERT SELECT, is the SELECT INTO statement. **The biggest difference between INSERT SELECT and SELECT INTO is that SELECT INTO cannot be used to insert rows into an existing table**, because it always creates a new table that is based on the result of the SELECT. Each column in the new table will have the same name, data type, and nullability as the corresponding column (or expression) in the SELECT list.

To use SELECT INTO, add INTO <new_table_name> in the SELECT clause of the query, just before the FROM clause. Here’s an example that extracts data from the Sales.SalesOrderHeader table into a new table named Sales.Invoice..

```
SELECT SalesOrderID, CustomerID, OrderDate, PurchaseOrderNumber, TotalDue
INTO Sales.Invoice
FROM Sales.SalesOrderHeader;
```
 
A SELECT INTO will fail if there already is a table with the name specified after INTO. After the table is created, it can be treated like any other table. You can select from it, join it to other tables, or insert more rows into it.
 
* Generate automatic values

You may need to automatically generate sequential values for one column in a specific table. Transact-SQL provides two ways to do this: use the IDENTITY property with a specific column in a table, or define a SEQUENCE object and use values generated by that object.

The IDENTITY property
To use the IDENTITY property, define a column using a numeric data type with a scale of 0 (meaning whole numbers only) and include the IDENTITY keyword. The allowable types include all integer types and decimal types where you explicitly give a scale of 0.

An optional seed (starting value), and an increment (step value) can also be specified. Leaving out the seed and increment will set them both to 1.

 **Note**

The IDENTITY property is specified in place of specifying NULL or NOT NULL in the column definition. Any column with the IDENTITY property is automatically not nullable. You can specify NOT NULL just for self-documentation, but if you specify the column as NULL (meaning nullable), the table creation statement will generate an error.

Only one column in a table may have the IDENTITY property set; it's frequently used as either the PRIMARY KEY or an alternate key.

The following code shows the creation of the Sales.Promotion table used in the previous section examples, but this time with an identity column named PromotionID as the primary key:

```
CREATE TABLE Sales.Promotion
(
PromotionID int IDENTITY PRIMARY KEY,
PromotionName varchar(20),
StartDate datetime NOT NULL DEFAULT GETDATE(),
ProductModelID int NOT NULL REFERENCES Production.ProductModel(ProductModelID),
Discount decimal(4,2) NOT NULL,
Notes nvarchar(max) NULL
);
```
 
 **Note**

The full details of the CREATE TABLE statement are beyond the scope of this module.

**Inserting data into an identity column**
 
When the IDENTITY property is defined for a column, INSERT statements into the table generally don't specify a value for the IDENTITY column. The database engine generates a value using the next available value for the column.

For example, you could insert a row into the Sales.Promotion table without specifying a value for the PromotionID column:

```
INSERT INTO Sales.Promotion
VALUES ('Clearance Sale', '01/01/2021', 23, 0.10, '10% discount')
```
 
Notice that even though the VALUES clause doesn't include a value for the PromotionID column, you don't need to specify a column list in the INSERT clause - Identity columns are exempt from this requirement.
 
**Retrieving an identity value**
 
To return the most recently assigned IDENTITY value within the same session and scope, use the SCOPE_IDENTITY function; like this:

```
SELECT SCOPE_IDENTITY();
```
 
The SCOPE_IDENTITY function returns the most recent identity value generated in the current scope for any table. If you need the latest identity value in a specific table, you can use the IDENT_CURRENT function, like this:

```
SELECT IDENT_CURRENT('Sales.Promotion');
``` 
 
**Overriding identity values**
 
If you want to override the automatically generated value and assign a specific value to the IDENTITY column, you first need to enable identity inserts by using the SET IDENTITY INSERT table_name ON statement. With this option enabled, you can insert an explicit value for the identity column, just like any other column. When you're finished, you can use the SET IDENTITY INSERT table_name OFF statement to resume using automatic identity values, using the last value you explicitly entered as a seed.

```
SET IDENTITY_INSERT SalesLT.Promotion ON;

INSERT INTO SalesLT.Promotion (PromotionID, PromotionName, ProductModelID, Discount)
VALUES
(20, 'Another short sale',37, 0.3);

SET IDENTITY_INSERT SalesLT.Promotion OFF;
```

 As you've learned, the IDENTITY property is used to generate a sequence of values for a column within a table. However, the IDENTITY property isn't suitable for coordinating values across multiple tables within a database. For example, suppose your organization differentiates between direct sales and sales to resellers, and wants to store data for these sales in separate tables. Both kinds of sale may need a unique invoice number, and you may want to avoid duplicating the same value for two different kinds of sale. One solution for this requirement is to maintain a pool of unique sequential values across both tables.

**Reseeding an identity column**
 
Occasionally, you'll need to reset or skip identity values for the column. To do this, you'll be "reseeding" the column using the DBCC CHECKIDENT function. You can use this to skip many values, or to reset the next identity value to 1 after you've deleted all of the rows in the table. For full details using DBCC CHECKIDENT, see the Transact-SQL reference documentation.

**SEQUENCE**
 
In Transact-SQL, you can use a sequence object to define new sequential values independently of a specific table. A sequence object is created using the CREATE SEQUENCE statement, optionally supplying the data type (must be an integer type or decimal or numeric with a scale of 0), the starting value, an increment value, a maximum value, and other options related to performance.

```
CREATE SEQUENCE Sales.InvoiceNumber AS INT
START WITH 1000 INCREMENT BY 1;
```
 
To retrieve the next available value from a sequence, use the NEXT VALUE FOR construct, like this:

```
INSERT INTO Sales.ResellerInvoice
VALUES
(NEXT VALUE FOR Sales.InvoiceNumber, 2, GETDATE(), 'PO12345', 107.99);
``` 

 **IDENTITY or SEQUENCE**
 
When deciding whether to use IDENTITY columns or a SEQUENCE object for auto-populating values, keep the following points in mind:

Use SEQUENCE if your application requires sharing a single series of numbers between multiple tables or multiple columns within a table.

SEQUENCE allows you to sort the values by another column. The NEXT VALUE FOR construct can use the OVER clause to specify the sort column. The OVER clause guarantees that the values returned are generated in the order of the OVER clause's ORDER BY clause. This functionality also allows you to generate row numbers for rows as they’re being returned in a SELECT. In the following example, the Production.Product table is sorted by the Name column, and the first returned column is a sequential number.

```
SELECT NEXT VALUE FOR dbo.Sequence OVER (ORDER BY Name) AS NextID,
    ProductID,
    Name
FROM Production.Product;
```
 
Even though the previous statement was just selecting SEQUENCE values to display, the values are still being 'used up' and the displayed SEQUENCE values will no longer be available. If you run the above SELECT multiple times, you'll get different SEQUENCE values each time.

Use SEQUENCE if your application requires multiple numbers to be assigned at the same time. For example, an application needs to reserve five sequential numbers. Requesting identity values could result in gaps in the series if other processes were simultaneously issued numbers. You can use the sp_sequence_get_range system procedure to retrieve several numbers in the sequence at once.

SEQUENCE allows you to change the specification of the sequence, such as the increment value.

IDENTITY values are protected from updates. If you try to update a column with the IDENTITY property, you'll get an error.

* Update data

The UPDATE statement in T-SQL is used to change existing data in a table. UPDATE operates on a set of rows, either defined by a condition in a WHERE clause or defined in a join. The UPDATE statement has a SET clause that specifies which columns are to be modified. The SET clause one or more columns, separated by commas, and supplies new values to those columns. The WHERE clause in an UPDATE statement has the same structure as a WHERE clause in a SELECT statement.

 **Note**

It’s important to note that an UPDATE without a corresponding WHERE clause or a join, will update all the rows in a table. Use the UPDATE statement with caution.

The basic syntax of an UPDATE statement is shown below.

```
UPDATE <TableName>
SET 
<ColumnName> = { expression | DEFAULT | NULL }
{,…n}
WHERE <search_conditions>;
```
 
The following example shows the UPDATE statement used to modify the notes for a promotion:

```
UPDATE Sales.Promotion
SET Notes = '25% off socks'
WHERE PromotionID = 2;
```
 
You can modify multiple columns in the SET clause. For example, the following UPDATE statement modified both the Discount and Notes fields for all rows where the promotion name is "Get Framed":

```
UPDATE Sales.Promotion
SET Discount = 0.2, Notes = REPLACE(Notes, '10%', '20%')
WHERE PromotionName = 'Get Framed';
```
 
The UPDATE statement also supports a FROM clause, enabling you to modify data based on the results of a query. For example, the following code updates the Sales.Promotion table using values retrieved from the Product.ProductModel table.

```
UPDATE Sales.Promotion
SET Notes = FORMAT(Discount, 'P') + ' off ' + m.Name
FROM Product.ProductModel AS m
WHERE Notes IS NULL
    AND Sales.Promotion.ProductModelID = m.ProductModelID;
```
 
* Delete data

Just as the INSERT statement always adds whole rows to a table, the DELETE statement always removes entire rows.

**Use DELETE to remove specific rows**
 
DELETE operates on a set of rows, either defined by a condition in a WHERE clause or defined in a join. The WHERE clause in a DELETE statement has the same structure as a WHERE clause in a SELECT statement.

 **Note**

It’s important to keep in mind that a DELETE without a corresponding WHERE clause will remove all the rows from a table. Use the DELETE statement with caution.

The following code shows the basic syntax of the DELETE statement:

```
DELETE [FROM] <TableName>
WHERE <search_conditions>;
```
 
The following example uses the DELETE statement to remove all products from the specified table that have been discontinued. There's a column in the table called discontinued and for products that are no longer available, the column has a value of 1.

```
DELETE FROM Production.Product
WHERE discontinued = 1;
```
 
Use TRUNCATE TABLE to remove all rows
DELETE without a WHERE clause removes all the rows from a table. For this reason, DELETE is usually used conditionally, with a filter in the WHERE clause. If you really do want to remove all the rows and leave an empty table, you can use the TRUNCATE TABLE statement. This statement does not allow a WHERE clause and always removes all the rows in one operation. Here’s an example:

```
TRUNCATE TABLE Sales.Sample;
```
 
TRUNCATE TABLE is more efficient than DELETE when you do want to remove all rows.
 
* Merge data based on multiple tables

In database operations, there is sometimes a need to perform a SQL MERGE operation. This DML option allows you to synchronize two tables by inserting, updating, or deleting rows in one table based on differences found in the other table. The table that is being modified is referred to as the target table. The table that is used to determine which rows to change are called the source table.

MERGE modifies data, based on one or more conditions:
  * When the source data has a matching row in the target table, it can update data in the target table.
  * When the source data has no match in the target, it can insert data into the target table.
  * When the target data has no match in the source, it can delete the target data.

  The general syntax of a MERGE statement is shown below. We're matching the target and the source on a specified column, and if there's a match between target and source, we specify an action to take on the target table. If there's not a match, we specify an action. The action can be an INSERT, UPDATE, or DELETE operation. This code indicates that an UPDATE is performed when there's a match between the source and the target. An INSERT is performed when there's data in the source with no matching data in the target. Finally, a DELETE is performed when there is data in the target with no match in the source. There are many other possible forms of a MERGE statement.

```
MERGE INTO schema_name.table_name AS TargetTbl
USING (SELECT <select_list>) AS SourceTbl
ON (TargetTbl.col1 = SourceTbl.col1)
WHEN MATCHED THEN 
   UPDATE SET TargetTbl.col2 = SourceTbl.col2
WHEN NOT MATCHED [BY TARGET] THEN
   INSERT (<column_list>)
   VALUES (<value_list>)
WHEN NOT MATCHED BY SOURCE THEN
   DELETE;
```
 
You can use only the elements of the MERGE statement that you need. For example, suppose the database includes a table of staged invoice updates, that includes a mix of revisions to existing invoices and new invoices. You can use the WHEN MATCHED and WHEN NOT MATCHED clauses to update or insert invoice data as required.

```
MERGE INTO Sales.Invoice as i
USING Sales.InvoiceStaging as s
ON i.SalesOrderID = s.SalesOrderID
WHEN MATCHED THEN
    UPDATE SET i.CustomerID = s.CustomerID,
                i.OrderDate = GETDATE(),
                i.PurchaseOrderNumber = s.PurchaseOrderNumber,
                i.TotalDue = s.TotalDue
WHEN NOT MATCHED THEN
    INSERT (SalesOrderID, CustomerID, OrderDate, PurchaseOrderNumber, TotalDue)
    VALUES (s.SalesOrderID, s.CustomerID, s.OrderDate, s.PurchaseOrderNumber, s.TotalDue);
```
 
### <h3 id="section1-2">Programming with Transact-SQL</h3>
 
#### Get started with Transact-SQL programming

#### Create stored procedures and user-defined functions
 
#### Implement error handling with Transact-SQL
 
#### Implement transactions with Transact-SQL
 
### <h3 id="section1-3">Write advanced Transact-SQL queries</h3> 

Learn how to use advanced Transact-SQL features to fetch and transform data in your databases.
 
#### Create tables, views, and temporary objects
 
Learn how to use Transact-SQL to create tables, views, and temporary objects for your databases.
 
Use Transact-SQL to enhance your querying experience. In this module, you'll learn how to use Transact-SQL capabilities including views, derived tables, and Common Table Expressions, to help you to meet your daily querying needs. For example, you can use Transact-SQL to break down complex queries into smaller and more manageable parts, and even get the data need without having to directly access the underlying source data.

By the end of this module, you should be able to:

* Create and query tables using Transact-SQL.
* Create and query views using Transact-SQL.
* Create queries using Common Table Expressions (CTE).
* Write queries that use derived tables.

**Create and query tables**

You can use Transact-SQL to make tables for your databases, to populate them, and fetch data from them.

*Create tables*
Use Transact-SQL statements to create tables for your databases so that you can store and query your data. To create a table, you perform the following steps:

1. Point to your database. For example, to point to a database named OnlineShop, you'd run the following statement in your chosen query editor window:

```
USE OnlineShop;
```
 
2. You can then use CREATE TABLE to create your table in your chosen database. For example, to create a Products table, you can run the following statement:

```
CREATE TABLE Products  
(ProductID int PRIMARY KEY NOT NULL,  
ProductName varchar(50) NOT NULL,  
ProductDescription varchar(max) NOT NULL);
```
 
This creates a table with the following columns:
```
Column	Description
ProductID	A product ID column with int type. It is also the primary key for the table.
ProductName	A column for the name of each product of type varchar with limit of up to 50 characters. NOT NULL means the column can't be empty.
ProductDescription	A column for the description of each product. Also of type varchar.
``` 
To successfully create a table, you must provide a name for your table, the names of the columns for your table, and the data type for each column.

 **Note**

You must have the CREATE TABLE and ALTER SCHEMA permissions to create tables.

Insert and read data from a table
Once you've created your table, you'll want to populate it with data. You can do this with Transact-SQL using the INSERT statement. For example, to add a product to a Products table, you could run the following statement:

```
INSERT Products (ProductID, ProductName, ProductDescription)  
    VALUES (1, 'The brown fox and the yellow bear', 'A popular book for children.');
```
 
To read data from your table, you use the SELECT statement. For instance, to fetch the names and descriptions for all the products in your Products table, you'd run the following statement:

```
SELECT ProductName, ProductDescription
    FROM Products; 
```

**Create and query views**

Views are saved queries that you can create in your databases. A single view can reference one or more tables. And, just like a table, a view consists of rows and columns of data. You can use views as a source for your queries in much the same way as tables. However, views don't persistently store data; the definition of your view is unpacked at runtime and the source objects are queried.

The apparent similarity between a table and a view provides an important benefit. Your applications can be written to use views instead of the underlying tables, shielding the application from making changes to the tables. This provides you with an additional layer of security for your data.

As long as the view continues to present the same structure to the calling application, the application will also receive consistent results. This way, views can be considered an application programming interface (API) to a database for purposes of retrieving data.

 **Note**

Views must be created by a database developer or administrator with appropriate permission in the database.

Create a view
To create a view, you use the CREATE VIEW statement to name and store a single SELECT statement. You'd create a view using the following syntax:

```
CREATE VIEW <schema_name.view_name> [<column_alias_list>] 
[WITH <view_options>]
AS select_statement;
```
 
 **Note**

The ORDER BY clause is not permitted in a view definition unless the view uses a TOP, OFFSET/FETCH, or FOR XML element.

For example, to create a view named Sales.CustOrders based on a custom SELECT statement that encompasses multiple tables, you could write the following query:

```
CREATE VIEW Sales.CustOrders
AS
SELECT
  O.custid, 
  DATEADD(month, DATEDIFF(month, 0, O.orderdate), 0) AS ordermonth,
  SUM(OD.qty) AS qty
FROM Sales.Orders AS O
  JOIN Sales.OrderDetails AS OD
    ON OD.orderid = O.orderid
GROUP BY custid, DATEADD(month, DATEDIFF(month, 0, O.orderdate), 0);
```
 
Notice that most of the code within the example consists of your SELECT statement. The SELECT statements inside view definitions can be as complex or simple as you want them to be.

Query a view
To query a view and retrieve results from it, refer to it in the FROM clause of a SELECT statement, as you would refer to a table. For example, to return the customer ID, the order month, and the quantity of items from each order in your Sales.CustOrders view, you could run the following query:

```
SELECT custid, ordermonth, qty
FROM Sales.CustOrders; 
```

**Use temporary tables

You can use Transact-SQL to create temporary tables. Temporary tables come in two types:

* Local temporary tables
* Global temporary tables
Create local temporary tables
Use local temporary tables to create tables scoped to your current session. This means that your temporary table is only visible to you, and when the session is over, the table no longer exists. Multiple users can create tables using the same name, and they would have no effect on each other.

To create a local temporary table, you use the same approach as you would when creating a regular table. However, you'd add # before the table name to signify that it's a local temporary table:

```
CREATE TABLE #Products (
    ProductID INT PRIMARY KEY,
    ProductName varchar,
    ...
);
```
 
Create global temporary tables
 
You can also create global temporary tables. A global temporary table is accessible across all sessions. But this means that a global temporary table must have a unique name, unlike a local temporary table. Global temporary tables are dropped automatically when the session that created it ends, and all tasks referencing it across all sessions have also ended. You create a global temporary table in the same way you would create a local temporary table, except you'd use ## instead of the single # specify it as a global temporary table:

```
CREATE TABLE ##Products (
    ProductID INT PRIMARY KEY,
    ProductName varchar,
    ...
);
```
 
Insert and read data from a temporary table
 
You can insert data into your temporary tables (both local and global) using the same approach as regular tables, using INSERT. You just need to make sure to append the # or ## to the table name. For example:

```
INSERT #Products (ProductID, ProductName, ProductDescription)  
    VALUES (1, 'The temporary time leap', 'A novel about temporary time leaping.');  
```
 
You can also retrieve results from a temporary table using SELECT. For example, to retrieve all rows and columns for your #Products temporary table, and order the results by product name, you'd run:

```
SELECT *  
FROM #Products  
ORDER BY ProductName;
```  
 
**Use Common Table Expressions**

Common Table Expressions (CTEs) provide a mechanism for you to define a subquery that may then be used elsewhere in a query. Unlike a derived table, a CTE is defined at the beginning of a query and may be referenced multiple times in the outer query.

CTEs are named expressions defined in a query. Like subqueries and derived tables, CTEs provide a means to break down query problems into smaller, more modular units. CTEs are limited in scope to the execution of the outer query. When the outer query ends, so does the CTE's lifetime.

Write queries with CTEs to retrieve results
You can use CTEs to retrieve results. To create a CTE, you define it in a WITH clause, based on the following syntax:

```
WITH <CTE_name>
AS (<CTE_definition>)
```
 
For example, to use a CTE to retrieve information about orders placed per year by distinct customers, you could run the following query:

```
WITH CTE_year 
AS
(
    SELECT YEAR(orderdate) AS orderyear, custid
    FROM Sales.Orders
)
SELECT orderyear, COUNT(DISTINCT custid) AS cust_count
FROM CTE_year
GROUP BY orderyear;
```
 
You name the CTE (named CTE_year) using the WITH clause, then you use AS () to define your subquery. You can then reference your resulting CTE in the outer query, which in this case is done in the final SELECT statement (FROM CTE_year). The result would look like this:

orderyear	cust_count
2019	67
2020	86
2021	81
 
When writing queries with CTEs, consider the following guidelines:

* CTEs require a name for the table expression, in addition to unique names for each of the columns referenced in the CTE's SELECT clause.
* CTEs may use inline or external aliases for columns.
* Unlike a derived table, a CTE may be referenced multiple times in the same query with one definition. Multiple CTEs may also be defined in the same WITH clause.
* CTEs support recursion, in which the expression is defined with a reference to itself. Recursive CTEs are beyond the scope of this module. 
 
**Write queries that use derived tables**

Derived tables allow you to write Transact-SQL statements that are more modular, helping you to break down complex queries into more manageable parts. Using derived tables in your queries can also provide workarounds for some of the restrictions imposed by the logical order of query processing, such as the use of column aliases.

Like subqueries, you create derived tables in the FROM clause of an outer SELECT statement. Unlike subqueries, you write derived tables using a named expression that is logically equivalent to a table and may be referenced as a table elsewhere in the outer query.

Derived tables are not stored in the database. Therefore, no special security privileges are required to write queries using derived tables, other than the rights to select from the source objects. A derived table is created at the time of execution of the outer query and goes out of scope when the outer query ends. Derived tables do not necessarily have an impact on performance, compared to the same query expressed differently. When the query is processed, the statement is unpacked and evaluated against the underlying database objects.

Return results using derived tables
 
To create a derived table, you write an inner query between parentheses, followed by an AS clause and a name for your derived table, using the following syntax:

```
SELECT <outer query column list>
FROM (SELECT <inner query column list>
    FROM <table source>) AS <derived table alias>
```
 
For example, you can use a derived table to retrieve information about orders placed per year by distinct customers:

```
SELECT orderyear, COUNT(DISTINCT custid) AS cust_count
FROM (SELECT YEAR(orderdate) AS orderyear, custid
    FROM Sales.Orders) AS derived_year
GROUP BY orderyear;
```
 
The inner query builds a set of orders and places it into the derived table's derived year. The outer query operates on the derived table and summarizes the results. The results would look like this:

orderyear	cust_count
2019	67
2020	86
2021	81
Pass arguments to derived tables
Derived tables can accept arguments passed in from a calling routine, such as a Transact-SQL batch, function, or a stored procedure. You can write derived tables with local variables serving as placeholders. At runtime, the placeholders can be replaced with values supplied in the batch or with values passed as parameters to the stored procedure that invoked the query. This will allow your code to be reused more flexibly than rewriting the same query with different values each time.

For example, the following batch declares a local variable (marked with the @ symbol) for the employee ID, and then uses the ability of SQL Server to assign a value to the variable in the same statement. The query accepts the @emp_id variable and uses it in the derived table expression:

```
DECLARE @emp_id INT = 9; --declare and assign the variable
SELECT orderyear, COUNT(DISTINCT custid) AS cust_count
FROM (    
    SELECT YEAR(orderdate) AS orderyear, custid
    FROM Sales.Orders
    WHERE empid=@emp_id --use the variable to pass a value to the derived table query
) AS derived_year
GROUP BY orderyear;
GO
```
 
When writing queries that use derived tables, keep the following guidelines in mind:

* The nested SELECT statement that defines the derived table must have an alias assigned to it. The outer query will use the alias in its SELECT statement in much the same way you refer to aliased tables joined in a FROM clause.
* All columns referenced in the derived table's SELECT clause should be assigned aliases, a best practice that is not always required in Transact-SQL. Each alias must be unique within the expression. The column aliases may be declared inline with the columns or externally to the clause.
* The SELECT statement that defines the derived table expression may not use an ORDER BY clause, unless it also includes a TOP operator, an OFFSET/FETCH clause, or a FOR XML clause. As a result, there is no sort order provided by the derived table. You sort the results in the outer query.
* The SELECT statement that defines the derived table may be written to accept arguments in the form of local variables. If the SELECT statement is embedded in a stored procedure, the arguments may be written as parameters for the procedure.
* Derived table expressions that are nested within an outer query can contain other derived table expressions. Nesting is permitted, but it is not recommended due to increased complexity and reduced readability.
* A derived table may not be referred to multiple times within an outer query. If you need to manipulate the same results, you will need to define the derived table expression every time, such as on each side of a JOIN operator. 
 
 
 
 
#### Combine query results with set operators
 
#### Write queries that use window functions
 
#### Transform data by implementing pivot, unpivot, rollup, and cube
 
 
 
## <h2 id="section2">Optimize query performance in Azure SQL</h2>

Analyze individual query performance and determine where improvements can be made. Explore performance-related Dynamic Management Objects. Investigate how indexes and database design affect queries. 

### <h3 id="section2-1">Describe SQL Server query plans</h3>
 
Read and understand various forms of execution plans. Compare estimated vs actual plans. Learn how and why plans are generated. The most important skill you should acquire in database performance tuning is being able to read and understand query execution plans. The plans explain the behavior of the database engine as it executes queries and retrieves the results.
 
#### Describe types of query plans
 
It is helpful to have a basic understanding of how database optimizers work before taking a deeper dive into execution plan details. SQL Server uses what is known as cost-based query optimizer. The query optimizer calculates a cost for multiple possible plans based on the statistics it has on the columns being utilized, and the possible indexes that can be used for each operation in each query plan. Based on this information, it comes up with a total cost for each plan. Some complex queries can have thousands of possible execution plans. The optimizer does not evaluate every possible plan, but uses heuristics to determine plans that are likely to have good performance. The optimizer will then choose the lowest cost plan of all the plans evaluated for a given query.

Because the query optimizer is cost-based, it is important that it has good inputs for decision making. The statistics SQL Server uses to track the distribution of data in columns and indexes need be kept up to date, or it can cause suboptimal execution plans to be generated. SQL Server automatically updates its statistics as data changes in a table; however, more frequent updates may be needed for rapidly changing data. The engine uses a number of factors when building a plan including **compatibility level of the database, row estimates based on statistics and available indexes**.

When a user submits a query to the database engine, the following process happens:

1. The query is parsed for proper syntax and a parse tree of database objects is generated if the syntax is correct.
2. The parse tree from Step 1 is taken as input to a database engine component called the Algebrizer for binding. This step validates that columns and objects in the query exist and identifies the data types that are being processed for a given query. This step outputs a query processor tree which is in the input for step 3. Because query optimization is a relatively expensive process in terms of CPU consumption, the database engine caches execution plans in a special area of memory called the plan cache. If a plan for a given query already exists, that plan is retrieved from the cache. The queries whose plans are stored in cache will each have a hash value generated based on the T-SQL in the query. This value is referred to as the query_hash. When looking for a plan in cache, the engine will generate a query_hash for the current query and then look to see if it matches any existing queries in the plan cache.
4. If the plan does not exist, the Query Optimizer then uses its cost-based optimizer to generate several execution plan options based on the statistics about the columns, tables, and indexes that are used in the query, as described above. The output of this step is a query execution plan.
5. The query is then executed using an execution plan that is pulled from the plan cache, or a new plan generated in step 4. The output of this step is the results of your query.
 
Let’s look at an example. Consider the following query:

```
SELECT orderdate

 ,avg(salesAmount)

FROM FactResellerSales

WHERE ShipDate = '2013-07-07'

GROUP BY orderdate;
```

 In this example SQL Server will check for the existence of the OrderDate, ShipDate, and SalesAmount columns in the table FactResellerSales. If those columns exist, it will then generate a hash value for the query, and examine the plan cache for a matching hash value. If there is plan for a query with a matching hash the engine will try to reuse that plan. If there is no plan with a matching hash, it will examine the statistics it has available on the OrderDate and ShipDate columns. The WHERE clause referencing the ShipDate column is what is known as the predicate in this query. If there is a nonclustered index that includes the ShipDate column SQL Server will most likely include that in the plan, if the costs are lower than retrieving data from the clustered index. The optimizer will then choose the lowest cost plan of the available plans and execute the query.

Query plans combine a series of relational operators to retrieve the data, and also capture information about the data such as estimated row counts. Another element of the execution plan is the memory required to perform operations such as joining or sorting data. The memory needed by the query is called the memory grant. The memory grant is a good example of the importance of statistics. If SQL Server thinks an operator is going to return 10,000,000 rows, when it is only returning 100, a much larger amount of memory is granted to the query. A memory grant that is larger than necessary can cause a twofold problem. First, the query may encounter a RESOURCE_SEMAPHORE wait, which indicates that query is waiting for SQL Server to allocate it a large amount of memory. SQL Server defaults to waiting for 25 times the cost of the query (in seconds) before executing, up to 24 hours. Second, when the query is executed, if there is not enough memory available, the query will spill to tempdb, which is much slower than operating in memory.

The execution plan also stores other metadata about the query, including, but not limited to, the database compatibility level, the degree of parallelism of the query, and the parameters that are supplied if the query is parameterized.

Query plans can be viewed either in a graphical representation or in a text-based format. The text-based options are invoked with SET commands and apply only to the current connection. Text-based plans can be viewed anywhere you can run TSQL queries.

Most DBAs prefer to look at plans graphically, because a graphical plan allows you to see the plan as a whole, including what’s called the shape of the plan, easily. There are several ways you can view and save graphical query plans. The most common tool used for this purpose is SQL Server Management Studio, but estimated plans can also be viewed in Azure Data Studio. There are also third-party tools that support viewing graphical execution plans.

There are three different types of execution plans that can be viewed.

**Estimated Execution Plan**: This type is the execution plan as generated by the query optimizer. The metadata and size of query memory grant are based on estimates from the statistics as they exist in the database at the time of query compilation. To see a text-based estimated plan run the command `SET SHOWPLAN_ALL ON` before running the query. When you run the query, you will see the steps of the execution plan, but the query will NOT be executed, and you will not see any results. The SET option will stay in effect until you set it OFF.

**Actual Execution Plan**: This type is same plan as the estimated plan; however this plan also contains the execution context for the query, which includes the estimated and actual row counts, any execution warnings, the actual degree of parallelism (number of processors used) and elapsed and CPU times used during the execution. To see a text-based actual plan run the command `SET STATISTICS PROFILE ON` before running the query. The query will execute, and you get the plan and the results.

**Live Query Statistics**: This plan viewing option combines the estimated and actual plans into an animated plan that displays execution progress through the operators in the plan. It refreshes every second and shows the actual number of rows flowing through the operators. The other benefit to Live Query Statistics is that it shows the handoff from operator to operator, which may be helpful in troubleshooting some performance issues. Because the type of plan is animated, it is only available as a graphical plan.
 
#### Explain estimated and actual query plans
 
The topic of actual versus estimated execution plans can be confusing. The difference is that the actual plan includes runtime statistics that are not captured in the estimated plan. The operators used, and order of execution will be the same as the estimated plan in nearly all cases. The other consideration is that in order to capture an actual execution plan the query has to be executed, which can be time consuming, or not possible. For example, the query may be an UPDATE statement that can only be run once. However, if you need to see query results as well as the plan, you’ll need to use one of the actual plan options.
 
There is overhead to both executing a query and generating an estimated execution plan, so viewing execution plans should be done carefully in a production environment.

For the most part you can use the estimated execution plan while writing your query, to understand its performance characteristics, identify missing indexes, or detect query anomalies. The actual execution plan is best used to understand the runtime performance of the query, and most importantly gaps in statistical data that cause the query optimizer to make suboptimal choices based on the data it has available.
 
**Read a query plan**
 
Execution plans show you what tasks the database engine is performing while retrieving the data needed to satisfy a query. Let’s dive into the plan. 
 
Each icon in the plan shows a specific operation, which represents the various actions and decisions that make up an execution plan. The SQL Server database engine has over 100 query operators that can make up on an execution plan. You will notice that under each operator icon, there is a cost percentage relative to the total cost of the query. Even an operation that shows a cost of 0% still represents some cost. In fact, 0% is usually due to rounding, because the graphical plan costs are always shown as whole numbers, and the real percentage is something less than 0.5%. 
 
The tooltip highlights the cost and estimates for the estimated plan, and for an actual plan will include the comparisons to the actual rows and costs. Each operator also has properties that will show you more than the tooltip does. If you right-click on a specific operator, you can select the Properties option from the context menu to see the full property list. This option will open up a separate Properties pane in SQL Server Management Studio, which by default is on the right side. Once the Properties pane is open, clicking on any operator will populate the Properties list with properties for that operator. Alternatively, you can open the Properties pane by clicking on View in the main SQL Server Management Studio menu and choosing Properties. 
 
The Properties pane includes some additional information and shows the output list which provides details of the columns being passed to the next operator. Examining these columns, in conjunction with a clustered index scan operator can indicate that an additional nonclustered index might be needed to improve the performance of the query. **Since a clustered index scan operation is reading the entire table, in this scenario a non-clustered index on the StockItemID column in each table could be more efficient**. 
 
**Lightweight query profiling**
 
As mentioned above, capturing actual execution plans, whether using SSMS or the Extended Events monitoring infrastructure can have a large amount of overhead, and is typically only done in live site troubleshooting efforts. Observer overhead, as it is known, is the cost of monitoring a running application. In some scenarios this cost can be just a few percentage points of CPU utilization, but in other cases like capturing actual execution plans, it can slow down individual query performance significantly. The legacy profiling infrastructure in SQL Server’s engine could produce up to 75% overhead for capturing query information, whereas the lightweight profiling infrastructure has a maximum overhead of around 2%.

Starting with SQL Server 2014 SP2 and SQL Server 2016, Microsoft introduced lightweight profiling and enhanced it with SQL Server 2016 SP1 and all later versions. In the first version of this feature, lightweight profiling collected row count and I/O utilization information (the number of logical and physical reads and writes performed by the database engine to satisfy a given query). In addition, a new extended event called query_thread_profile was introduced to allow data from each operator in a query plan to be inspected. In the initial version of lightweight profiling, using the feature requires trace flag 7412 to be enabled globally.

In newer releases (SQL Server 2016 SP2 CU3, SQL Server 2017 CU11, and SQL Server 2019), if lightweight profiling is not enabled globally, you can use the `USE HINT` query hint with `QUERY_PLAN_PROFILE` to enable lightweight profiling at the query level. When a query that has this hint completes execution, a *query_plan_profile* extended event is generated, which provides an actual execution plan. You can see an example of a query with this hint:

```
SELECT [stockItemName]

 ,[UnitPrice] * [QuantityPerOuter] AS CostPerOuterBox

 ,[ QuantityonHand]

FROM [Warehouse].[StockItems] s

JOIN [Warehouse].[StockItems] sh ON s.StockItemID = sh.StockItemID

ORDER BY CostPerOuterBox 

OPTION(USE HINT ('QUERY_PLAN_PROFILE')); 
```
 
**Last query plans stats**
 
SQL Server 2019 and Azure SQL Database support two further enhancements to the query profiling infrastructure. First, lightweight profiling is enabled by default in both SQL Server 2019 and Azure SQL Database and managed instance. Lightweight profiling is also available as a database scoped configuration option, called `LIGHTWEIGHT_QUERY_PROFILING`. With the database scoped option, you can disable the feature for any of your user databases independent of each other.

Second, there is a new dynamic management function called `sys.dm_exec_query_plan_stats`, which can show you the last known actual query execution plan for a given plan handle. In order to see the last known actual query plan through the function, you can enable trace flag 2451 server-wide. Alternatively, you can enable this functionality using a database scoped configuration option called `LAST_QUERY_PLAN_STATS`.

You can combine this function with other objects to get the last execution plan for all cached queries as shown below:

```
SELECT *

FROM sys.dm_exec_cached_plans AS cp

CROSS APPLY sys.dm_exec_sql_text(plan_handle) AS st

CROSS APPLY sys.dm_exec_query_plan_stats(plan_handle) AS qps; 

GO
```
 
This functionality lets you quickly identify the runtime stats for the last execution of any query in your system, with minimal overhead. The image below shows how to retrieve the plan. If you click on the execution plan XML, which will be the first column of results, will display the execution plan shown in the second image below. 
 
#### Identify problematic query plans

The path most DBAs take to troubleshoot query performance is to first identify the problematic query (typically the query consuming the highest amount of system resources), and then retrieve that query’s execution plan. There are two scenarios. One is that the query consistently performs poorly. Consistent poor performance can be caused by a few different problems, including hardware resource constraints (though this situation typically will not affect a single query running in isolation), a suboptimal query structure, database compatibility settings, missing indexes, or poor choice of plan by the query optimizer. The second scenario is that the query performs well for some executions, but not others. This problem can be caused by a few other factors, the most common being data skew in a parameterized query that has an efficient plan for some executions, and a poor one for other executions. The other common factors in inconsistent query performance are blocking, where a query is waiting on another query to complete in order to gain access to a table, or hardware contention.
 
**Hardware constraints**
 
For the most part, hardware constraints will not manifest themselves with single query executions but will be evident when production load is applied and there is a limited number of CPU threads and a limited amount of memory to be shared among the queries. When you have CPU contention, it will usually be detectable by observing the performance monitor counter ‘% Processor Time’, which measures the CPU usage of the server. Looking deeper into SQL Server, you may see SOS_SCHEDULER_YIELD and CXPACKET wait types when the server is under CPU pressure. However, in some cases with poor storage system performance, even single executions of a query that is otherwise optimized can be slow. Storage system performance is best tracked at the operating system level using the performance monitor counters ‘Disk Seconds/Read’ and ‘Disk Seconds/Write’ which measure how long an I/O operation takes to complete. SQL Server will write to its error log if it detects poor storage performance (if an I/O takes longer than 15 seconds to complete). If you look at wait statistics and see a high percentage of PAGEIOLATCH_SH waits in your SQL Server, you might have a storage system performance issue. Typically, hardware performance is examined at a high level, early in the performance troubleshooting process, because it is relatively easy to evaluate.

Most database performance issues can be attributed to suboptimal query patterns, but in many cases running inefficient queries will put undue pressure on your hardware. For example, missing indexes could lead to CPU, storage, and memory pressure by retrieving more data than is required to process the query. It is recommended that you address suboptimal queries and tune them, before addressing hardware issue. We’ll start looking at query tuning next. 

**Suboptimal query constructs**
 
Relational databases perform best when executing set-based operations. Set-based operations perform data manipulation (INSERT, UPDATE, DELETE, and SELECT) in sets, where work is done on a set of values and produces either a single value or a result set. The alternative to set-based operations is to perform row-based work, using a cursor or a while loop. This type of processing is known as row-based processing, and its cost increases linearly with the number of rows impacted. That linear scale is problematic as data volumes grow for an application.

While detecting suboptimal use of row-based operations with cursors or WHILE loops is important, there are other SQL Server anti-patterns that you should be able to recognize. Table-valued functions (TVF), particularly multi-statement table-valued functions, caused problematic execution plan patterns prior to SQL Server 2017. Many developers like to use multi-statement table valued functions because they can execute multiple queries within a single function and aggregate the results into a single table. However, anyone writing T-SQL code needs to be aware of the possible performance penalties for using TVFs.

SQL Server has two types of table-valued functions, inline and multi-statement. If you use an inline TVF, the database engine treats it just like a view. Multi-statement TVFs are treated just like another table when processing a query. Because TVFs are dynamic and as such, SQL Server does not have statistics on them, it used a fixed row count when estimating the query plan cost. A fixed count can be fine, if the number of rows is small, however if the TVF returns thousands or millions of rows, the execution plan could be very inefficient.

Another anti-pattern has been the use of scalar functions, which have similar estimation and execution problems. Microsoft has made a lot of headway on improving the performance of the aforementioned patterns with the introduction of Intelligent Query Processing in SQL Server 2017 and Azure SQL Database, under compatibility levels 140 and 150.

**SARGability**
 
The term SARGable in relational databases refers to a predicate (WHERE clause) in a specific format that can leverage an index to speed up execution of a query. Predicates in the correct format are called ‘Search Arguments’ or SARGs. In SQL Server, using a SARG means that the optimizer will evaluate using a nonclustered index on the column referenced in the SARG for a SEEK operation, instead of scanning the entire index (or the entire table) to retrieve a value.

The presence of a SARG does not guarantee the use of an index for a SEEK. The optimizer’s costing algorithms could still determine that the index was too expensive. This could be the case if a SARG refers to a large percentage of rows in a table. The absence of a SARG does mean that the optimizer will not even evaluate a SEEK on a nonclustered index.

Some examples of expressions that are not SARGs (sometimes said to be non-sargable) are those that include a LIKE clause with a wildcard at the beginning of the string to be matched, for example, WHERE lastName LIKE ‘%SMITH%’. Other predicates that are not SARGs occur when using functions on a column, for example, WHERE CONVERT(CHAR(10), CreateDate,121) = ‘2020-03-22’. These queries with non-sargable expressions are typically identified by examining execution plans for index or table scans, where seeks should otherwise be taking place. 
 
By changing the LEFT function into a LIKE, an index SEEK is used.

 **Note**

The LIKE keyword, in this instance, does not have a wildcard on the left, so it is looking for cities that begin with M. , if it was “two-sided” or started with a wildcard (‘%M% or ‘%M’) it would be non-SARGable. The seek operation is estimated to return 1267 rows, or approximately 15% of the estimate for the query with the non-SARGable predicate.

Some other database development anti-patterns are treating the database as a service rather than a data store. Using a database to convert data to JSON, manipulate strings, or perform complex calculations can lead to excessive CPU use and increased latency. Queries that try to retrieve all records and then perform computations in the database can lead to excessive IO and CPU usage. Ideally, you should use the database for data access operations and optimized database constructs like aggregation.

**Missing indexes**
 
The most common performance problems we see as database administrators are due to a lack of useful indexes causing the engine to read far more pages than necessary to return the results of a query. While indexes are not free in terms of resources (adding additional indexes to a table can impact write performance and consume space), the performance gains they offer can offset the additional resource costs many times over. Frequently execution plans with these performance issues can be identified by the query operator Clustered Index Scan or the combination of the Nonclustered Index Seek and Key Lookup (which is more indicative of missing columns in an existing index). The database engine attempts to help with this problem by reporting on missing indexes in execution plans. The names and details of the indexes that have been deemed potentially useful are available through a dynamic management view called sys.dm_db_missing_index_details. There are also other DMVs in SQL Server like sys.dm_db_index_usage_stats and sys.dm_db_index_operational_stats, which highlight the utilization of existing indexes. It may make sense to drop an index that is not used by any queries in the database. The missing index DMVs and plan warnings should only be used as a starting point for tuning your queries. It’s important to understand what your key queries are and build indexes to support those queries. Creating all missing indexes without evaluating indexes in the context of each other is not recommended.

**Missing and out-of-date statistics**
 
You have learned about the importance of column and index statistics to the query optimizer. It is also important to understand conditions that can lead to out-of-date statistics, and how this problem can manifest itself in SQL Server. SQL Server and Azure SQL Database and Managed Instance default to having auto-update statistics set to ON. Prior to SQL Server 2016, the default behavior of auto-update statistics was to not update statistics until the number of modifications to columns in the index was equal to about 20% of the number of rows in a table. Because of this behavior, you could have data modifications that were significant enough to change query performance, but not update the statistics. Any plan that used the table with the changed data would be based on out-of-date statistics and would frequently be suboptimal. Prior to SQL Server 2016, you had the option of using trace flag 2371, which changed the required number of modifications to be a dynamic value, so as your table grew in size, the percentage of row modifications that was required to trigger a statistics update got smaller. Newer versions of SQL Server and Azure SQL Database and Managed Instance support this behavior by default. There is also a dynamic management function called sys.dm_db_stats_properties, which shows you the last time statistics were updated and the number of modifications that have been made since the last update, which allows you to quickly identity statistics that might need to be manually updated.

**Poor optimizer choices**
 
While the query optimizer does a good job of optimizing most queries, there are some edge cases where the cost-based optimizer may make impactful decisions that are not fully understood. There are a number of ways to address this including using query hints, trace flags, execution plan forcing, and other adjustments in order to reach a stable and optimal query plan. Microsoft has a support team that can help troubleshoot these scenarios.

In the below example from the AdventureWorks2017 database, a query hint is being use to tell the database optimizer to always use a city name of Seattle. This hint will not guarantee the best execution plan for all city values, but it will be predictable. The value of ‘Seattle’ for @city_name will only be used during optimization. During execution, the actual supplied value (‘Ascheim’) will be used.

```
DECLARE @city_name nvarchar(30) = 'Ascheim',
        @postal_code nvarchar(15) = 86171;

SELECT * 
FROM Person.Address
WHERE City = @city_name 
      AND PostalCode = @postal_code
OPTION (OPTIMIZE FOR (@city_name = 'Seattle');
```
 
As seen in the example, the query uses a hint (the OPTION clause) to tell the optimizer to use a specific variable value to build its execution plan.

**Parameter sniffing**
 
SQL Server caches query execution plans for future use. Since the execution plan retrieval process is based on the hash value of a query, the query text has to be identical for every execution of the query for the cached plan to be used. In order to support multiple values in the same query, many developers use parameters, passed in through stored procedures, as seen in the example below:

```
CREATE PROC GetAccountID (@Param INT)
AS

<other statements in procedure>

SELECT accountid FROM CustomerSales WHERE sales > @Param;

<other statements in procedure>

RETURN;

-- Call the procedure:

EXEC GetAccountID 42;
```
 
Queries can also be explicitly parameterized using the procedure sp_executesql. However, explicit parameterization of individual queries is usually done through the application with some form (depending on the API) of PREPARE and EXECUTE. When the database engine executes that query for the first time, it will optimize the query based on the initial value of the parameter, in this case, 42. This behavior, called parameter sniffing, allows for the overall workload of compiling queries to be reduced on the server. However, in the event that there is data skew, query performance could vary widely. For example, a table that had 10 million records, and 99% of those records have an ID of 1, and the other 1% are unique numbers, performance will be based on which ID was initially used to optimize the query. This wildly fluctuating performance is indicative of data skew and is not an inherent problem with parameter sniffing. This behavior is a fairly common performance problem that you should be aware of. You should understand the options for alleviating the issue. There a few ways to address this problem, but they each come with tradeoffs:

Use the RECOMPILE hint in your query, or the WITH RECOMPILE execution option in your stored procedures. This hint will cause the query or procedure to be recompiled every time it is executed, which will increase CPU utilization on the server but will always use the current parameter value.
You can use the OPTIMIZE FOR UNKNOWN query hint. This hint will cause the optimizer to choose to not sniff parameters and compare the value with column data histogram. This option will not get you the best possible plan but will allow for a consistent execution plan.
Rewrite your procedure or queries by adding logic around parameter values to only RECOMPILE for known troublesome parameters. In the example below, if the SalesPersonID parameter is NULL, the query will be executed with the OPTION (RECOMPILE).
```
CREATE OR ALTER PROCEDURE GetSalesInfo (@SalesPersonID INT = NULL)
AS
DECLARE  @Recompile BIT = 0
         , @SQLString NVARCHAR(500)

SELECT @SQLString = N'SELECT SalesOrderId, OrderDate FROM Sales.SalesOrderHeader WHERE SalesPersonID = @SalesPersonID'

IF @SalesPersonID IS NULL
BEGIN
     SET @Recompile = 1
END

IF @Recompile = 1
BEGIN
    SET @SQLString = @SQLString + N' OPTION(RECOMPILE)'
END

EXEC sp_executesql @SQLString
    ,N'@SalesPersonID INT'
    ,@SalesPersonID = @SalesPersonID
GO
```
 
The example above is a good solution but it does require a fairly large development effort, and a firm understanding of your data distribution. It also may require maintenance as the data changes. 
 
### <h3 id="section2-2">Explore query performance optimization</h3>

Read and understand various forms of execution plans. Compare estimated vs actual plans. Learn how and why plans are generated. Understand the purpose and benefits of the Query Store.
 
The most important skill you should acquire in database performance tuning is being able to read and understand query execution plans. The plans explain the behavior of the database engine as it executes queries and retrieves the results.

Query Store helps you quickly identify your most expensive queries, and find any changes in performance. The Query Store provides a powerful data collection including the automation of query plan and execution runtime. SQL Server and Azure SQL provide locking and blocking in order to manage concurrency and ensure data consistency. Finally, you can adjust the isolation levels in SQL Server to help manage concurrency.
 
#### Understand query plans

It's helpful to have a basic understanding of how database optimizers work before taking a deeper dive into execution plan details. SQL Server uses what is known as cost-based query optimizer. The query optimizer calculates a cost for multiple possible plans based on the statistics it has on the columns being utilized, and the possible indexes that can be used for each operation in each query plan. Based on this information, it comes up with a total cost for each plan. Some complex queries can have thousands of possible execution plans. The optimizer doesn't evaluate every possible plan, but uses heuristics to determine plans that are likely to have good performance. The optimizer will then choose the lowest cost plan of all the plans evaluated for a given query.

Because the query optimizer is cost-based, it's important that it has good inputs for decision making. The statistics SQL Server uses to track the distribution of data in columns and indexes need be kept up to date, or it can cause suboptimal execution plans to be generated. SQL Server automatically updates its statistics as data changes in a table; however, more frequent updates may be needed for rapidly changing data. The engine uses many factors when building a plan including compatibility level of the database, row estimates based on statistics and available indexes.

When a user submits a query to the database engine, the following process happens:

1. The query is parsed for proper syntax and a parse tree of database objects is generated if the syntax is correct.
2. The parse tree from Step 1 is taken as input to a database engine component called the Algebrizer for binding. This step validates that columns and objects in the query exist and identifies the data types that are being processed for a given query. This step outputs a query processor tree, which is in the input for step 3.
3. Because query optimization is a relatively expensive process in terms of CPU consumption, the database engine caches execution plans in a special area of memory called the plan cache. If a plan for a given query already exists, that plan is retrieved from the cache. The queries whose plans are stored in cache will each have a hash value generated based on the T-SQL in the query. This value is referred to as the query_hash. The engine will generate a query_hash for the current query and then look to see if it matches any existing queries in the plan cache.
4. If the plan doesn't exist, the Query Optimizer then uses its cost-based optimizer to generate several execution plan options based on the statistics about the columns, tables, and indexes that are used in the query, as described above. The output of this step is a query execution plan.
5. The query is then executed using an execution plan that is pulled from the plan cache, or a new plan generated in step 4. The output of this step is the results of your query.
 
 **Note**

To learn more about how the query processor works, see Query Processing Architecture Guide

Let’s look at an example. Consider the following query:

```
SELECT orderdate,
        AVG(salesAmount)
FROM FactResellerSales
WHERE ShipDate = '2013-07-07'
GROUP BY orderdate;
```
 
In this example SQL Server will check for the existence of the OrderDate, ShipDate, and SalesAmount columns in the table FactResellerSales. If those columns exist, it will then generate a hash value for the query, and examine the plan cache for a matching hash value. If there's plan for a query with a matching hash the engine will try to reuse that plan. If there's no plan with a matching hash, it will examine the statistics it has available on the OrderDate and ShipDate columns. The WHERE clause referencing the ShipDate column is what is known as the predicate in this query. If there's a nonclustered index that includes the ShipDate column SQL Server will most likely include that in the plan, if the costs are lower than retrieving data from the clustered index. The optimizer will then choose the lowest cost plan of the available plans and execute the query.

Query plans combine a series of relational operators to retrieve the data, and also capture information about the data such as estimated row counts. Another element of the execution plan is the memory required to perform operations such as joining or sorting data. The memory needed by the query is called the memory grant. The memory grant is a good example of the importance of statistics. If SQL Server thinks an operator is going to return 10,000,000 rows, when it's only returning 100, a much larger amount of memory is granted to the query. A memory grant that is larger than necessary can cause a twofold problem. First, the query may encounter a RESOURCE_SEMAPHORE wait, which indicates that query is waiting for SQL Server to allocate it a large amount of memory. SQL Server defaults to waiting for 25 times the cost of the query (in seconds) before executing, up to 24 hours. Second, when the query is executed, if there isn't enough memory available, the query will spill to tempdb, which is much slower than operating in memory.

The execution plan also stores other metadata about the query, including, but not limited to, the database compatibility level, the degree of parallelism of the query, and the parameters that are supplied if the query is parameterized.

Query plans can be viewed either in a graphical representation or in a text-based format. The text-based options are invoked with SET commands and apply only to the current connection. Text-based plans can be viewed anywhere you can run T-SQL queries.

Most DBAs prefer to look at plans graphically, because a graphical plan allows you to see the plan as a whole, including what’s called the shape of the plan, easily. There are several ways you can view and save graphical query plans. The most common tool used for this purpose is SQL Server Management Studio, but estimated plans can also be viewed in Azure Data Studio. There are also third-party tools that support viewing graphical execution plans.

There are three different types of execution plans that can be viewed.

**Estimated Execution Plan**
 
This type is the execution plan as generated by the query optimizer. The metadata and size of query memory grant are based on estimates from the statistics as they exist in the database at the time of query compilation. To see a text-based estimated plan run the command SET SHOWPLAN_ALL ON before running the query. When you run the query, you'll see the steps of the execution plan, but the query will NOT be executed, and you won't see any results. The SET option will stay in effect until you set it OFF.

**Actual Execution Plan**
 
This type is same plan as the estimated plan; however this plan also contains the execution context for the query, which includes the estimated and actual row counts, any execution warnings, the actual degree of parallelism (number of processors used) and elapsed and CPU times used during the execution. To see a text-based actual plan run the command SET STATISTICS PROFILE ON before running the query. The query will execute, and you get the plan and the results.

**Live Query Statistics**
 
This plan viewing option combines the estimated and actual plans into an animated plan that displays execution progress through the operators in the plan. It refreshes every second and shows the actual number of rows flowing through the operators. The other benefit to Live Query Statistics is that it shows the handoff from operator to operator, which may be helpful in troubleshooting some performance issues. Because the type of plan is animated, it's only available as a graphical plan.
 
#### Explain estimated and actual query plans
 
#### Describe dynamic management views and functions

SQL Server provides several hundred dynamic management objects. These objects contain system information that can be used to monitor the health of a server instance, diagnose problems, and tune performance. Dynamic management views and functions return internal data about the state of the database or the instance. Dynamic Management Objects can be either views (DMVs) or functions (DMFs), but most people use the acronym DMV to refer to both types of object.

There are two levels of DMVs, server scoped and database scoped.

* Server scoped objects – require VIEW SERVER STATE permission on the server
* Database scoped objects – require the VIEW DATABASE STATE permission within the database
 
The names of the DMVs are all prefixed with sys.dm_ followed by the functional area and then the specific function of the object. SQL Server supports three categories of DMVs:

* Database-related dynamic management objects
* Query execution related dynamic management objects
* Transaction related dynamic management objects
 
To learn about queries to monitor server and database performance, see Monitoring Microsoft Azure SQL Database and Azure SQL Managed Instance performance using dynamic management views.

 **Note**

For older versions of SQL Server where the query store is not available, you can use the view sys.dm_exec_cached_plans in conjunction with the functions sys.dm_exec_sql_text and sys.dm_exec_query_plan to return information about execution plans. However, unlike with Query Store, you will not be able to see changes in plans for a given query.

Azure SQL Database has a slightly different set of the DMVs available than SQL Server; some objects are available only in Azure SQL Database, while other objects are only available in SQL Server. Some are scoped at the server level and aren't applicable in the Azure model (the waits_stats DMV below is an example of a server-scoped DMV), while others are specific to Azure SQL Database, like sys.dm_db_resource_stats and provide Azure-specific information that isn't available in (or relevant to) SQL Server. 
 
#### Explore Query Store

The SQL Server Query Store is a per-database feature that automatically captures a history of queries, plans, and runtime statistics to simplify performance troubleshooting and query tuning. It also provides insight into database usage patterns and resource consumption.

In total, the Query Store contains three stores:

* Plan store - used for storing estimated execution plan information
* Runtime stats store - used for storing execution statistics information
* Wait stats store - for persisting wait statistics information
 
Screenshot of the Query Store components.

Enable the Query Store
The Query Store is enabled by default in Azure SQL databases. If you want to use it with SQL Server and Azure Synapse Analytics, you need to enable it first. To enable the Query Store feature, use the following query valid for your environment:

```
-- SQL Server
ALTER DATABASE <database_name> SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE);

-- Azure Synapse Analytics
ALTER DATABASE <database_name> SET QUERY_STORE = ON;
```
 
How the Query Store collects data
The Query Store integrates with the query processing pipeline at many stages. Within each integration point, data is collected in memory and written to disk asynchronously to minimize I/O overhead. The integration points are as follows:

When a query executes for the first time, its query text and initial estimated execution plan are sent to the Query Store and persisted.

The plan updates in the Query Store when a query recompiles. If the recompile results in a newly generated execution plan, it also persists in the Query Store to augment the previous plans. In addition, the Query Store keeps track of the execution statistics for each query plan for comparison purposes.

During the compile and check for recompile phases, the Query Store identifies if there's a forced plan for the query to be executed. The query is recompiled if the Query Store provides a forced plan different from the plan in the procedure cache.

When a query executes, its runtime statistics persist in the Query Store. The Query Store aggregates this data to ensure an accurate representation of every query plan.

Screenshot of the Query Store integration points in the query execution pipeline displayed as a flow chart.

To learn more about how Query Store collects data, see How Query Store collects data.

Common scenarios
The SQL Server Query Store provides valuable insight into the performance of the operations performed in a database. The most common scenarios include:

Identifying and fixing performance regression due to inferior query execution plan selection

Identifying and tuning the highest resource consumption queries

A/B testing to evaluate the impacts of database and application changes

Ensuring performance stability after SQL Server upgrades

Determining the most frequently used queries

Audit the history of query plans for a query

Identifying and improving ad hoc workloads

Understand the prevalent wait categories of a database and the contributing queries and plans affecting wait times

Analyze database usage patterns over time as it applies to resource consumption (CPU, I/O, Memory)

Discover the Query Store views
Once Query Store is enabled on a database, the Query Store folder is visible for the database in Object Explorer. For Azure Synapse Analytics, the Query Store views are displayed under System Views. The Query Store views provide aggregated, quick insights into the performance aspects of the SQL Server database.

Screenshot of S S M S Object Explorer with the Query Store views highlighted.

Regressed Queries
A regressed query is a query that is experiencing performance degradation over time due to execution plan changes. Estimated execution plans change due to many factors, including schema changes, statistics changes, and index changes. The first instinct may be to investigate the procedure cache, but the problem with the procedure cache is that it only stores the latest execution plan for a query; even then, plans are evicted based on the memory demands of the system. However, the Query Store persists several execution plans stored for each query, thus providing the flexibility to choose a specific plan in a concept known as plan forcing to solve the issue of a query performance regression caused by a plan change.

The Regressed Queries view can pinpoint queries whose execution metrics are regressing due to execution plan changes over a specified timeframe. The Regressed Queries view allows filtering based on selecting a metric (such as duration, CPU time, row count, and more) and a statistic (total, average, min, max, or standard deviation). Then, the view lists the top 25 regressed queries based on the filter provided. A graphical bar chart view of the queries displays by default, but you can optionally view the queries in a grid format.

The plan summary pane displays the persisted query plans associated with the query over time after selecting a query from the top-left query pane. You'll see a graphical query plan in the bottom pane by selecting a query plan in the Plan Summary pane. In addition, toolbar buttons are available in both the plan summary pane and graphical query plan pane to force the selected plan for the selected query. This pane structure and behavior is consistently used across all SQL Query views.

Screenshot of the Query Store Regressed Queries view displaying each of the different panes.

Alternatively, you can use the sp_query_store_force_plan stored procedure to use plan forcing.

```
EXEC sp_query_store_force_plan @query_id=73, @plan_id=79
 ```
 
Overall Resource Consumption
The Overall Resource Consumption view allows for analyzing total resource consumption for multiple execution metrics (such as execution count, duration, wait time, and more) for a specified timeframe. The rendered charts are interactive; when selecting a measure from one of the charts, a drill through view displaying the queries associated with the chosen measure displays in a new tab.

Screenshot of the SQL Query Store Overall resource consumption view with a configuration dialog indicating the different metrics available for display.

The details view provides the top 25 resource consumer queries that contributed to the metric that was selected. This details view uses the consistent interface that allows for the inspection of the associated queries and their details, evaluate saved estimated query plans, and optionally use plan forcing to improve performance. This view is valuable when system resource contention becomes an issue, such as when CPU usage reaches capacity.

Screenshot of the top 25 resource consumption for the database.

Top Resource Consuming Queries
The Top Resource Consuming Queries view is similar to the details drill down of the Overall Resource Consumption view. It also allows for selecting a metric and a statistic as a filter. However, the queries it displays are the top 25 most impactful queries based on the chosen filter and timeframe.

Screenshot of the top resource consuming queries view for the database.

The Top Resource Consuming Queries view provides the first indication of the ad hoc nature of the workload when identifying and improving ad hoc workloads. For example, in the following image, the Execution Count metric and the Total statistic are selected to unveil that approximately 90% of the top resource-consuming queries are only executed once.

Screenshot of the top resource consuming queries filtered by execution count.

Queries With Forced Plans
The Queries With Forced Plans view provides a quick look into the queries that have forced query plans. This view becomes relevant if a forced plan no longer performs as expected and needs to be reevaluated. This view provides the ability to review all persisted estimated execution plans for a selected query easily determining if another plan is now better suited for performance. If so, toolbar buttons are available to unforce a plan as required.

Screenshot of the queries with forced plans.

Queries With High Variation
Query performance can vary between executions. The Queries with High Variation view contains an analysis of queries that have the highest variation or standard deviation for a selected metric. The interface is consistent with most Query Store views allowing for query detail inspection, execution plan evaluation, and optionally forcing a specific plan. Use this view to tune unpredictable queries into a more consistent performance pattern.

Screenshot with the queries with high variation.

Query Wait Statistics
The Query Wait Statistics view analyzes the most active wait categories for the database and renders a chart. This chart is interactive; selecting a wait category drills into the details of the queries that contribute to the wait time statistic.

Screenshot of the queries with high variation view displays.

The details view interface is also consistent with most query store views allowing for query detail inspection, execution plan evaluation, and optionally forcing a specific plan. This view helps identify queries that are affecting user experience across applications.

Tracking Query
The Tracking Query view allows analyzing a specific query based on an entered query ID value. Once run, the view provides the complete execution history of the query. A checkmark on an execution indicates a forced plan was used. This view can provide insight into queries such as those with forced plans to verify that query performance is remaining stable.

Screenshot of the Tracking Query view filtering by a specific query ID.

Using the Query Store to find query waits
When the performance of a system begins to degrade, it makes sense to consult query wait statistics to potentially identify a cause. In addition to identifying queries that need tuning, it can also shed light on potential infrastructure upgrades that would be beneficial.

The SQL Query Store provides the Query Wait Statistics view to provide insight into the top wait categories for the database. Currently, there are 23 wait categories.

A bar chart displays the most impactful wait categories for the database when you open the Query Wait Statistics view. In addition, a filter located in the toolbar of the wait categories pane allows for the wait statistics to be calculated based on total wait time (default), average wait time, minimum wait time, maximum wait time, or standard deviation wait time.

Screenshot of the Query Wait Statistics view displaying the most impactful categories as a bar chart.

Selecting a wait category will drill through to the details of the queries that contribute to that wait category. From this view, you have the ability to investigate individual queries that are the most impactful. You can access the persisted estimated execution plans display in the Plan summary pane by selecting a query in the query pane. Selecting a query plan from the Plan summary pane will display the graphical query plan in the bottom pane. From this view, you have the ability to force or unforce a query plan for the query to improve performance.

Screenshot of the Query Wait Statistics view displaying the most impactful queries for the wait category. 
 
#### Identify problematic query plans
 
#### Describe blocking and locking

One feature of relational databases is locking, which is essential to maintain the atomicity, consistency, and isolation properties of the ACID model. All RDBMSs will block actions that would violate the consistency and isolation of writes to a database. SQL programmers are responsible for starting and ending transactions at the right point, in order to ensure the logical consistency of their data. In turn, the database engine provides locking mechanisms that also protect the logical consistency of the tables affected by those queries. These actions are a foundational part of the relational model.

On SQL Server, blocking occurs when one process holds a lock on a specific resource (row, page, table, database), and a second process attempts to acquire a lock with an incompatible lock type on the same resource. Typically, locks are held for a short period, and when the process holding the lock releases it, the blocked process can then acquire the lock and complete its transaction.

SQL Server locks the smallest amount of data needed to successfully complete the transaction. This behavior allows maximum concurrency. For example, if SQL Server is locking a single row, all other rows in the table are available for other processes to use, so concurrent work can go on. However, each lock requires memory resources, so it’s not cost-effective for one process to have thousands of individual locks on a single table. SQL Server tries to balance concurrency with cost. One technique used is called lock escalation. If SQL Server needs to lock more than 5000 rows on a single object in a single statement, it will escalate the multiple row locks to a single table lock.

Locking is normal behavior and happens many times during a normal day. Locking only become a problem when it causes blocking that isn't quickly resolved. There are two types of performance issues that can be caused by blocking:

* A process holds locks on a set of resources for an extended period of time before releasing them. These locks cause other processes to block, which can degrade query performance and concurrency.

* A process gets locks on a set of resources, and never releases them. This problem requires administrator intervention to resolve.

Another blocking scenario is deadlocking, which occurs when one transaction has a lock on a resource, and another transaction has a lock on a second resource. Each transaction then attempts to take a lock on the resource, which is currently locked by the other transaction. Theoretically, this scenario would lead to an infinite wait, as neither transaction could complete. However, the SQL Server engine has a mechanism for detecting these scenarios and will kill one of the transactions in order to alleviate the deadlock, based on which transaction has performed the least of amount of work that would need to be rolled back. The transaction that is killed is known as the deadlock victim. Deadlocks are recorded in the system_health extended event session, which is enabled by default.

It's important to understand the concept of a transaction. Auto-commit is the default mode of SQL Server and Azure SQL Database, which means the changes made by the statement below would automatically be recorded in the database's transaction log.

```
INSERT INTO DemoTable (A) VALUES (1);
```
 
In order to allow developers to have more granular control over their application code, SQL Server also allows you to explicitly control your transactions. The query below would take a lock on a row in the DemoTable table what wouldn't be released until a subsequent command to commit the transaction was added.

```
BEGIN TRANSACTION

INSERT INTO DemoTable (A) VALUES (1);
```
 
The proper way to write the above query is as follows:

```
BEGIN TRANSACTION

INSERT INTO DemoTable (A) VALUES (1);

COMMIT TRANSACTION
```
 
The COMMIT TRANSACTION command explicitly commits a record of the changes to the transaction log. The changed data will eventually make its way into the data file asynchronously. These transactions represent a unit of work to the database engine. If the developer forgets to issue the COMMIT TRANSACTION command, the transaction will stay open and the locks won't be released. This is one of the main reasons for long running transactions.

The other mechanism the database engine uses to help the concurrency of the database is row versioning. When a row versioning isolation level is enabled to the database, engine maintains versions of each modified row in TempDB. This is typically used in mixed use workloads, in order to prevent reading queries from blocking queries that are writing to the database.

To monitor open transactions awaiting commit or rollback run the following query:

```
SELECT tst.session_id, [database_name] = db_name(s.database_id)
    , tat.transaction_begin_time
    , transaction_duration_s = datediff(s, tat.transaction_begin_time, sysdatetime()) 
    , transaction_type = CASE tat.transaction_type  WHEN 1 THEN 'Read/write transaction'
        WHEN 2 THEN 'Read-only transaction'
        WHEN 3 THEN 'System transaction'
        WHEN 4 THEN 'Distributed transaction' END
    , input_buffer = ib.event_info, tat.transaction_uow     
    , transaction_state  = CASE tat.transaction_state    
        WHEN 0 THEN 'The transaction has not been completely initialized yet.'
        WHEN 1 THEN 'The transaction has been initialized but has not started.'
        WHEN 2 THEN 'The transaction is active - has not been committed or rolled back.'
        WHEN 3 THEN 'The transaction has ended. This is used for read-only transactions.'
        WHEN 4 THEN 'The commit process has been initiated on the distributed transaction.'
        WHEN 5 THEN 'The transaction is in a prepared state and waiting resolution.'
        WHEN 6 THEN 'The transaction has been committed.'
        WHEN 7 THEN 'The transaction is being rolled back.'
        WHEN 8 THEN 'The transaction has been rolled back.' END 
    , transaction_name = tat.name, request_status = r.status
    , tst.is_user_transaction, tst.is_local
    , session_open_transaction_count = tst.open_transaction_count  
    , s.host_name, s.program_name, s.client_interface_name, s.login_name, s.is_user_process
FROM sys.dm_tran_active_transactions tat 
INNER JOIN sys.dm_tran_session_transactions tst  on tat.transaction_id = tst.transaction_id
INNER JOIN Sys.dm_exec_sessions s on s.session_id = tst.session_id 
LEFT OUTER JOIN sys.dm_exec_requests r on r.session_id = s.session_id
CROSS APPLY sys.dm_exec_input_buffer(s.session_id, null) AS ib
ORDER BY tat.transaction_begin_time DESC;
```
 
**Isolation levels**
 
SQL Server offers several isolation levels to allow you to define the level of consistency and correctness you need guaranteed for your data. Isolation levels let you find a balance between concurrency and consistency. The isolation level doesn't affect the locks taken to prevent data modification, a transaction will always get an exclusive lock on the data that is modifying. However, your isolation level can affect the length of time that your locks are held. Lower isolation levels increase the ability of multiple user process to access data at the same time, but increase the data consistency risks that can occur. The isolation levels in SQL Server are as follows:

* Read uncommitted – Lowest isolation level available. Dirty reads are allowed, which means one transaction may see changes made by another transaction that haven't yet been committed.

* Read committed – allows a transaction to read data previously read, but not modified by another transaction with without waiting for the first transaction to finish. This level also releases read locks as soon as the select operation is performed. This is the default SQL Server level.

* Repeatable Read – This level keeps read and write locks that are acquired on selected data until the end of the transaction.

* Serializable – This is the highest level of isolation where transactions are isolated. Read and write locks are acquired on selected data and not released until the end of the transaction.

SQL Server also includes two isolation levels that include row-versioning.

* Read Committed Snapshot – In this level read operations take no row or page locks, and the engine presents each operation with a consistent snapshot of the data as it existed at the start of the query. This level is typically used when users are running frequent reporting queries against an OLTP database, in order to prevent the read operations from blocking the write operations.

* Snapshot – This level provides transaction level read consistency through row versioning. This level is vulnerable to update conflicts. If a transaction running under this level reads data modified by another transaction, an update by the snapshot transaction will be terminated and roll back. This isn't an issue with read committed snapshot isolation.

Isolation levels are set for each session with the T-SQL SET command, as shown:

```
SET TRANSACTION ISOLATION LEVEL

 { READ UNCOMMITTED

 | READ COMMITTED

 | REPEATABLE READ

 | SNAPSHOT

 | SERIALIZABLE

 }
```
 
There's no way to set a global isolation level all queries running in a database, or for all queries run by a particular user. It's a session level setting.

Monitoring for blocking problems
Identifying blocking problem can be troublesome as they can be sporadic in nature. There's a DMV called sys.dm_tran_locks, which can be joined with sys.dm_exec_requests in order to provide further information on locks that each session is holding. A better way to monitor for blocking problems is to do so on an ongoing basis using the Extended Events engine.

Blocking problems typically fall into two categories:

Poor transactional design. As shown above, a transaction that has no COMMIT TRANSACTION will never end. While that is a simple example, trying to do too much work in a single transaction or having a distributed transaction, which uses a linked server connection, can lead to unpredictable performance.

Long running transactions caused by schema design. Frequently this can be an update on a column with a missing index, or poorly designed update query.

Monitoring for locking-related performance problems allows you to quickly identity performance degradation related to locking.

For more information about how to monitor blocking, see Understand and resolve SQL Server blocking problems.
 
### <h3 id="section2-3">Explore performance-based design</h3>

Explore normalization for relational databases. Investigate the impact of proper datatype usage. Compare types of indexes.

Database design is an important aspect of database performance, even though it’s not always under the control of the database administrator. You may be working with third- party vendor applications that you didn't build. Whenever possible, it’s important to design your database properly for the workload, whether it’s an online transaction processing (OLTP) or data warehouse workload. Many design decisions, such as choosing the right datatypes, can make large differences in the performance of your databases.
 
#### Describe normalization

Database normalization is a design process used to organize a given set of data into tables and columns in a database. Each table should contain data relating to a specific ‘thing’ and only have data that supports that same ‘thing’ included in the table. The goal of this process is to reduce duplicate data contained within your database, to reduce performance degradation of database inserts and updates. For example, a customer address change is much easier to implement if the only place of the customer address is stored in the Customers table. The most common forms of normalization are first, second, and third normal form and are described below.

**First normal form**
 
First normal form has the following specifications:

* Create a separate table for each set of related data
* Eliminate repeating groups in individual tables
* Identify each set of related data with a primary key
 
In this model, you shouldn't use multiple columns in a single table to store similar data. For example, if product can come in multiple colors, you shouldn't have multiple columns in a single row containing the different color values. The first table, below (ProductColors), isn't in first normal form as there are repeating values for color. For products with only one color, there's wasted space. And what if a product came in more than three colors? Rather than having to set a maximum number of colors, we can recreate the table as shown in the second table, ProductColor. We also have a requirement for first normal form that there's a unique key for the table, which is column (or columns) whose value uniquely identifies the row. Neither of the columns in the second table is unique, but together, the combination of ProductID and Color is unique. When multiple columns are needed, we call that a composite key. 

**Second normal form**
 
Second normal form has the following specification, in addition to those required by first normal form:

* If the table has a composite key, all attributes must depend on the complete key and not just part of it.
 
Second normal form is only relevant to tables with composite keys, like in the table ProductColor, which is the second table above. Consider the case where the ProductColor table also includes the product’s price. This table has a composite key on ProductID and Color, because only using both column values can we uniquely identify a row. If a product’s price doesn't change with the color, we might see data as shown in this table: 
 
The table above is not in second normal form. The price value is dependent on the ProductID but not on the Color. There are three rows for ProductID 1, so the price for that product is repeated three times. The problem with violating second normal form is that if we have to update the price, we have to make sure we update it everywhere. If we update the price in the first row, but not the second or third, we would have something called an ‘update anomaly’. After the update, we wouldn’t be able to tell what the actual price for ProductID 1 was. The solution is to move the Price column to a table that has ProductID as a single column key, because that is the only column that Price depends on. For example, we could use Table 3 to store the Price.

If the price for a product was different based on its color, the fourth table would be in the second normal form, since the price would depend on both parts of the key: the ProductID and the Color.

Third normal form
Third normal form is typically the aim for most OLTP databases. Third normal form has the following specification, in addition to those required by second normal form:

All nonkey columns are non-transitively dependent on the primary key.
The transitive relationship implies that one column in a table is related to other columns, through a second column. Dependency means that a column can derive its value from another, as a result of a dependency. For example, your age can be determined from your date of birth, making your age dependent on your date of birth. Refer back to the third table, ProductInfo. This table is in second normal form, but not in third. The ShortLocation column is dependent on the ProductionCountry column, which isn't the key. Like second normal form, violating third normal form can lead to update anomalies. We would end up with inconsistent data if we updated the ShortLocation in one row but didn't update it in all the rows where that location occurred. To prevent this, we could create a separate table to store country names and their shortened forms.

Denormalization
While the third normal form is theoretically desirable, it isn't always possible for all data. In addition, a normalized database doesn't always give you the best performance. Normalized data frequently requires multiple join operations to get all the necessary data returned in a single query. There's a tradeoff between normalizing data when the number of joins required to return query results has high CPU utilization, and denormalized data that has fewer joins and less CPU required, but opens up the possibility of update anomalies.

 Note

Denormalized data is not the same as unnormalized. For denormalization, we start by designing tables that are normalized. Then we can add additional columns to some tables to reduce the number of joins required, but as we do so, we are aware of the possible update anomalies. We then make sure we have triggers or other kinds of processing that will make sure that when we perform an update, all the duplicate data is also updated.

Denormalized data can be more efficient to query, especially for read heavy workloads like a data warehouse. In those cases, having extra columns may offer better query patterns and/or more simplistic queries.

Star schema
While most normalization is aimed at OLTP workloads, data warehouses have their own modeling structure, which is usually a denormalized model. This design uses fact tables, which record measurements or metrics for specific events like a sale, and joins them to dimension tables, which are smaller in terms of row count, but may have a large number of columns to describe the fact data. Some example dimensions would include inventory, time, and/or geography. This design pattern is used to make the database easier to query and offer performance gains for read workloads.

A Sample Star Schema

The above image shows an example of a star schema, including a FactResellerSales fact table, and dimensions for date, currency, and products. The fact table contains data related to the sales transactions, and the dimensions only contain data related to a specific element of the sales data. For example, the FactResellerSales table contains only a ProductKey to indicate which product was sold. All of the details about each product are stored in the DimProduct table, and related back to the fact table with the ProductKey column.

Related to star schema design is a snowflake schema, which uses a set of more normalized tables for a single business entity. The following image shows an example of a single dimension for a snowflake schema. The Products dimension is normalized and stored in three tables called DimProductCategory, DimProductSubcategory, and DimProduct.

Sample Snowflake Schema

The main difference between star and snowflake schemas is that the dimensions in a snowflake schema are normalized to reduce redundancy, which saves storage space. The tradeoff is that your queries require more joins, which can increase your complexity and decrease performance.

#### Choose appropriate data types

SQL Server offers a wide variety of data types to choose from, and your choice can affect performance in many ways. While SQL Server can convert some data types automatically (we call this an ‘implicit conversion’, conversion can be costly and can also negatively affect query plans. The alternative is an explicit conversion, where you use the CAST or CONVERT function in your code to force a data type conversion.

Additionally, choosing data types that are much larger than needed can cause wasted space and require more pages than is necessary to be read. It's important to choose the right data types for a given set of data—which will reduce the total storage required for the database and improve the performance of queries executed.

 **Note**

In some cases, conversions are not possible at all. For example, a date cannot be converted to a bit. Conversions can negatively impact query performance by causing index scans where seeks would have been possible, and additional CPU overhead from the conversion itself.

The image below indicates in which cases SQL Server can do an implicit conversion and in which cases you must explicitly convert data types in your code.

Chart of Type Conversions in SQL Server and Azure SQL

SQL Server offers a set of system supplied data types for all data that can be used in your tables and queries. SQL Server allows the creation of user defined data types in either T-SQL or the .NET framework. 
 
#### Design indexes

SQL Server has several index types to support different types of workloads. At a high level, an index can be thought of as an on-disk structure that is associated with a table or a view, that enables SQL Server to more easily find the row or rows associated with the index key (which consists of one or more columns in the table or view), compared to scanning the entire table.

**Clustered indexes**
 
A common DBA job interview question is to ask the candidate the difference between a clustered and nonclustered index, as indexes are a fundamental data storage technology in SQL Server. A clustered index is the underlying table, stored in sorted order based on the key value. There can only be one clustered index on a given table, because the rows can be stored in one order. A table without a clustered index is called a heap, and heaps are typically only used as staging tables. An important performance design principle is to keep your clustered index key as narrow as possible. When considering the key column(s) for your clustered index, you should consider columns that are unique or that contain many distinct values. Another property of a good clustered index key is for records that are accessed sequentially, and are used frequently to sort the data retrieved from the table. Having the clustered index on the column used for sorting can prevent the cost of sorting every time that query executes, because the data will be already stored in the desired order.

 **Note**

When we say that the table is ‘stored’ in a particular order, we are referring to the logical order, not necessarily the physical, on-disk order. Indexes have pointers between pages, and the pointers help create the logical order. When scanning an index ‘in order’, SQL Server follows the pointers from page to page. Immediately after creating an index, it is most likely also stored in physical order on the disk, but after you start making modifications to the data, and new pages need to be added to the index, the pointers will still give us the correct logical order, but the new pages will most like not be in physical disk order.

**Nonclustered indexes**
 
Nonclustered indexes are a separate structure from the data rows. A nonclustered index contains the key values defined for the index, and a pointer to the data row that contains the key value. You can add another nonkey columns to the leaf level of the nonclustered index to cover more columns using the included columns feature in SQL Server. You can create multiple nonclustered indexes on a table.

An example of when you need to add an index or add columns to an existing nonclustered index is shown below:

Query and Query Execution Plan with a Key Lookup operator

The query plan indicates that for each row retrieved using the index seek, more data will need to be retrieved from the clustered index (the table itself). There's a nonclustered index, but it only includes the product column. If you add the other columns in the query to a nonclustered index as shown below, you can see the execution plan change to eliminate the key lookup.

Changing the Index and the Query Plan with No Key Lookup

The index created above is an example of a covering index, where in addition to the key column you're including extra columns to cover the query and eliminate the need to access the table itself.

Both nonclustered and clustered indexes can be defined as unique, meaning there can be no duplication of the key values. Unique indexes are automatically created when you create a PRIMARY KEY or UNIQUE constraint on a table.

The focus of this section is on b-tree indexes in SQL Server—these are also known as row store indexes. The general structure of a b-tree is shown below:

The B-tree architecture of an index in SQL Server and Azure SQL

Each page in an index b-tree is a called an index node, and the top node of b-tree is called the root node. The bottom nodes in an index are called leaf nodes and the collection of leaf nodes is the leaf level.

Index design is a mixture of art and science. A narrow index with few columns in its key requires less time to update and has lower maintenance overhead; however it may not be useful for as many queries as a wider index that includes more columns. You may need to experiment with several indexing approaches based on the columns selected by your application’s queries. The query optimizer will generally choose what it considers to be the best existing index for a query; however, that doesn't mean that there isn't a better index that could be built.

Properly indexing a database is a complex task. When planning your indexes for a table, you should keep a few basic principles in:

Understand the workloads of the system. A table that is used mainly for insert operations will benefit far less from extra indexes than a table used for data warehouse operations that are 90% read activity.
Understand what queries are run most frequently, and optimize your indexes around those queries
Understand the data types of the columns used in your queries. Indexes are ideal for integer data types, or unique or non-null columns.
Create nonclustered indexes on columns that are frequently used in predicates and join clauses, and keep those indexes as narrow as possible to avoid overhead.
Understand your data size/volume – A table scan on a small table will be a relatively cheap operation and SQL Server may decide to do a table scan simply because it's easy (trivial) to do. A table scan on a large table would be costly.
Another option SQL Server provides is the creation of filtered indexes. Filtered indexes are best suited to columns in large tables where a large percentage of the rows has the same value in that column. A practical example would be an employee table, as shown below, that stored the records of all employees, including ones who had left or retired.

```
CREATE TABLE [HumanResources].[Employee](
     [BusinessEntityID] [int] NOT NULL,
     [NationalIDNumber] [nvarchar](15) NOT NULL,
     [LoginID] [nvarchar](256) NOT NULL,
     [OrganizationNode] [hierarchyid] NULL,
     [OrganizationLevel] AS ([OrganizationNode].[GetLevel]()),
     [JobTitle] [nvarchar](50) NOT NULL,
     [BirthDate] [date] NOT NULL,
     [MaritalStatus] [nchar](1) NOT NULL,
     [Gender] [nchar](1) NOT NULL,
     [HireDate] [date] NOT NULL,
     [SalariedFlag] [bit] NOT NULL,
     [VacationHours] [smallint] NOT NULL,
     [SickLeaveHours] [smallint] NOT NULL,
     [CurrentFlag] [bit] NOT NULL,
     [rowguid] [uniqueidentifier] ROWGUIDCOL NOT NULL,
     [ModifiedDate] [datetime] NOT NULL)
```
 
In this table, there's a column called CurrentFlag, which indicates if an employee is currently employed. This example uses the bit datatype, indicating only two values, one for currently employed and zero for not currently employed. A filtered index with a WHERE CurrentFlag = 1, on the CurrentFlag column would allow for efficient queries of current employees.

You can also create indexes on views, which can provide significant performance gains when views contain query elements like aggregations and/or table joins.

**Columnstore indexes**
 
Columnstore offers improved performance for queries that run large aggregation workloads. This type of index was originally targeted at data warehouses, but over time columnstore indexes have been used in many other workloads in order to help solve query performance issues on large tables. As of SQL Server 2014, there are both nonclustered and clustered columnstore indexes. Like b-tree indexes, a clustered columnstore index is the table itself stored in a special way, and nonclustered columnstore indexes are stored independently of the table. Clustered columnstore indexes inherently include all the columns in a given table. However, unlike rowstore clustered indexes, clustered columnstore indexes are NOT sorted.

Nonclustered columnstore indexes are typically used in two scenarios, the first is when a column in the table has a data type that isn't supported in a columnstore index. Most data types are supported but XML, CLR, sql_variant, ntext, text, and image aren't supported in a columnstore index. Since a clustered columnstore always contains all the columns of the table (because it IS the table), a nonclustered is the only option. The second scenario is a filtered index—this scenario is used in an architecture called hybrid transactional analytic processing (HTAP), where data is being loaded into the underlying table, and at the same time reports are being run on the table. By filtering the index (typically on a date field), this design allows for both good insert and reporting performance.

Columnstore indexes are unique in their storage mechanism, in that each column in the index is stored independently. It offers a two-fold benefit. A query using a columnstore index only needs to scan the columns needed to satisfy the query, reducing the total IO performed, and it allows for greater compression, since data in the same column is likely to be similar in nature.

Columnstore indexes perform best on analytic queries that scan large amounts of data, like fact tables in a data warehouse. Starting with SQL Server 2016 you can augment a columnstore index with another b-tree nonclustered index, which can be helpful if some of your queries do lookups against singleton values.

Columnstore indexes also benefit from batch execution mode, which refers to processing a set of rows (typically around 900) at a time versus the database engine processing those rows one at time. Instead of loading each record independently and processing them, the query engine computes the calculation in that group of 900 records. This processing model reduces the number of CPU instructions dramatically.

```
SELECT SUM(Sales) FROM SalesAmount;
```
 
Batch mode can provide significant performance increase over traditional row processing. SQL Server 2019 also includes batch mode for rowstore data. While batch mode for rowstore doesn't have the same level of read performance as a columnstore index, analytical queries may see up to a 5x performance improvement.

The other benefit columnstore indexes offer to data warehouse workloads is an optimized load path for bulk insert operations of 102,400 rows or more. While 102,400 is the minimum value to load directly into the columnstore, each collection of rows, called a rowgroup, can be up to approximately 1,024,000 rows. Having fewer, but fuller, rowgroups makes your SELECT queries more efficient, because fewer row groups need to be scanned to retrieve the requested records. These loads take place in memory and are directly loaded to the index. For smaller volumes, data is written to a b-tree structure called a delta store, and asynchronously loaded into the index.

Columnstore Index Load Example

In this example, the same data is being loaded into two tables, FactResellerSales_CCI_Demo and FactResellerSales_Page_Demo. The FactResellerSales_CCI_Demo has a clustered columnstore index, and the FactResellerSales_Page_Demo has a clustered b-tree index with two columns and is page compressed. As you can see each table is loading 1,024,000 rows from the FaceResellerSalesXL_CCI table. When SET STATISTICS TIME is ON, SQL Server keeps track of the elapsed time of the query execution. Loading the data into the columnstore table took roughly 8 seconds, where loading into the page compressed table took nearly 20 seconds. In this example, all the rows going into the columnstore index are loaded into a single rowgroup.

If you load less than 102,400 rows of data into a columnstore index in a single operation, it's loaded in b-tree structure known as a delta store. The database engine moves this data into the columnstore index using an asynchronous process called the tuple mover. Having open delta stores can affect the performance of your queries, because reading those records is less efficient than reading from the columnstore. You can also reorganize the index with the COMPRESS_ALL_ROW_GROUPS option in order to force the delta stores to be added and compressed into the columnstore indexes.


### <h3 id="section2-4">Evaluate performance improvements</h3>

Evaluate possible changes to indexes. Determine the impact of changes to queries and indexes. Explore Query Store hints.

One of the challenges of the DBA’s role is to evaluate the changes they make to code or data structures on a busy, production system. While tuning a single query in isolation offers easy metrics such as elapsed time or logical reads, making minor tweaks on a busy system may require deeper evaluation.
 
#### Describe wait statistics

One holistic way of monitoring server performance is to evaluate what the server is waiting on. Wait statistics are complex, and SQL Server is instrumented with hundreds of waiting types, which monitors each running thread and logs what the thread is waiting on.

Detecting and troubleshooting SQL Server performance issues require an understanding of how wait statistics work, and how the database engine uses them while processing a request.

Screenshot of how wait statistics work.

Wait statistics are broken down into three types of waits: resource waits, queue waits, and external waits.

* Resource waits occur when a worker thread in SQL Server requests access to a resource that is currently being used by a thread. Examples of resources wait are locks, latches, and disk I/O waits.
* Queue waits occur when a worker thread is idle and waiting for work to be assigned. Example queue waits are deadlock monitoring and deleted record cleanup.
* External waits occur when SQL Server is waiting on an external process like a linked server query to complete. An example of an external wait is a network wait related to returning a large result set to a client application.
 
You can check sys.dm_os_wait_stats system view to explore all the waits encountered by threads that executed, and sys.dm_db_wait_stats for Azure SQL Database. The sys.dm_exec_session_wait_stats system view lists active waiting sessions.

These system views allow the DBA to get an overview of the performance of the server, and to readily identify configuration or hardware issues. This data is persisted from the time of instance startup, but the data can be cleared as needed to identify changes.

Wait statistics are evaluated as a percentage of the total waits on the server.

Screenshot of the top 10 waits by percentage.

The result of this query from sys.dm_os_wait_stats shows the wait type, and the aggregation of percent of time waiting (Wait Percentage column) and the average wait time in seconds for each wait type.

In this case, the server has Always On Availability Groups in place, as indicated by the REDO_THREAD_PENDING_WORK and PARALLEL_REDO_TRAN_TURN wait types. The relatively high percentage of CXPACKET and SOS_SCHEDULER_YIELD waits indicates that this server is under some CPU pressure.

As DMVs provide a list of wait types with the highest time accumulated since the last SQL Server startup, collecting and storing wait statistic data periodically could help you understand and correlate performance problems with other database events.

Considering that DMVs provide you with a list of wait types with the highest time accumulated since the last SQL Server startup, collecting and storing wait statistics periodically might help you understand and correlate performance problems with other database events.

There are several types of waits available in SQL Server, but some of them are common.

* RESOURCE_SEMAPHORE—this wait type is indicative of queries waiting on memory to become available, and may indicate excessive memory grants to some queries. This problem is typically observed by long query runtimes or even time outs. These wait types can be caused by out-of-date statistics, missing indexes, and excessive query concurrency.

* LCK_M_X—frequent occurrences of this wait type can indicate a blocking problem, that can be solved by either changing to the READ COMMITTED SNAPSHOT isolation level, or making changes in indexing to reduce transaction times, or possibly better transaction management within T-SQL code.

( PAGEIOLATCH_SH—this wait type can indicate a problem with indexes (or a lack of useful indexes), where SQL Server is scanning too much data. Alternatively, if the wait count is low, but the wait time is high, it can indicate storage performance problems. You can observe this behavior by analyzing the data in the waiting_tasks_count and the wait_time_ms columns in the sys.dm_os_wait_stats system view, to calculate an average wait time for a given wait type.

* SOS_SCHEDULER_YIELD—this wait type can indicate high CPU utilization, which is correlated with either high number of large scans, or missing indexes, and often with high numbers of CXPACKET waits.

* CXPACKET—if this wait type is high it can indicate improper configuration. Prior to SQL Server 2019, the max degree of parallelism default setting is to use all available CPUs for queries. Additionally, the cost threshold for parallelism setting defaults to 5, which can lead to small queries being executed in parallel, which can limit throughput. Lowering MAXDOP and increasing the cost threshold for parallelism can reduce this wait type, but the CXPACKET wait type can also indicate high CPU utilization, which is typically resolved through index tuning.

* PAGEIOLATCH_UP—this wait type on data pages 2:1:1 can indicate TempDB contention on Page Free Space (PFS) data pages. Each data file has one PFS page per 64 MB of data. This wait is typically caused by only having one TempDB file, as prior to SQL Server 2016 the default behavior was to use one data file for TempDB. The best practice is to use one file per CPU core up to eight files. It's also important to ensure your TempDB data files are the same size and have the same autogrowth settings to ensure they're used evenly. SQL Server 2016 and higher control the growth of TempDB data files to ensure they grow in a consistent, simultaneous fashion.

In addition to the aforementioned DMVs, the Query Store also tracks waits associated with a given query. However, waits data tracked by Query Store isn't tracked at the same granularity as the data in the DMVs, but it can provide a nice overview of what a query is waiting on.
 
####  Tune and maintain indexes

The most common (and most effective) method for tuning T-SQL queries is to evaluate and adjust your indexing strategy. Properly indexed databases perform fewer IOs to return query results, and with fewer IOs there's reduced pressure on both the IO and storage systems. Reducing IO even allows for better memory utilization. Keep in mind the read/write ratio of your queries.

A heavy write workload may indicate that the cost of writing rows to extra indexes isn't of much benefit. An exception would be if the workload performs mainly updates that also need to do lookup operations. Update operations that do lookups can benefit from extra indexes or columns added to an existing index. Your goal should always be to get the most benefit out of the smallest number of indexes on your tables.

A common performance tuning approach is as follows:

* Evaluate existing index usage using sys.dm_db_index_operational_stats and sys.dm_db_index_usage_stats.

* Consider eliminating unused and duplicate indexes, but this should be done carefully. Some indexes may only be used during monthly/quarterly/annual operations, and may be important for those processes. You may also consider creating indexes to support those operations just before the operations are scheduled, to reduce the overhead of having otherwise unused indexes on a table.

* Review and evaluate expensive queries from the Query Store, or Extended Events capture, and work to manually craft indexes to better serve those queries.

* Create the index(s) in a non-production environment, and test query execution and performance and observe performance changes. It's important to note any hardware differences between your production and non-production environments, as the amount of memory and the number of CPUs could affect your execution plan.

* After testing carefully, implement the changes to your production system.

Verify the column order of your indexes—the leading column drives column statistics and usually determines whether the optimizer will choose the index. Ideally, the leading column will be selective and used in the WHERE clause of many of your queries. Consider using a change control process for tracking changes that could affect application performance. Before dropping an index, save the code in your source control, so the index can be quickly recreated if an infrequently run query requires the index to perform well.

Finally, columns used for equality comparisons should precede columns used for inequality comparisons and that columns with greater selectivity should precede columns with fewer distinct values.

**Resumable index**
 
Resumable index allows index maintenance operations to be paused, or take place in a time window, and be resumed later. A good example of where to use resumable index operations is to reduce the impact of index maintenance in a busy production environment. You can then perform rebuild operations during a specific maintenance window giving you more control over the process.

Furthermore, creating an index for a large table can negatively affect the performance of the entire database system. The only way to fix this issue in versions prior to SQL Server 2019 is to kill the index creation process. Then you have to start the process over from the beginning if the system rolls back the session.

With resumable index, you can pause the build and then restart it later at the point it was paused.

The following example shows how to create a resumable index:

```
-- Creates a nonclustered index for the Customer table

CREATE INDEX IX_Customer_PersonID_ModifiedDate 
    ON Sales.Customer (PersonID, StoreID, TerritoryID, AccountNumber, ModifiedDate)
WITH (RESUMABLE=ON, ONLINE=ON)
GO
```
 
In a query window, resume the index operation:

```
ALTER INDEX IX_Customer_PersonID_ModifiedDate ON Sales.Customer PAUSE
GO
```
 
The statement above uses the PAUSE clause to temporarily stop the creation of the resumable online index.

You can check the current execution status for a resumable online index by querying the sys.index_resumable_operations system view.

 **Note**

Resumable index is only supported with online operations.
 
#### Understand query hints

Query hints are options or strategies that can be applied to enforce the query processor to use a particular operator in the execution plan for SELECT, INSERT, UPDATE, or DELETE statements. Query hints override any execution plan the query processor might select for a given query with the OPTION clause.

In most cases, the query optimizer selects an efficient execution plan based on the indexes, statistics, and data distribution. Database administrators rarely need to intervene manually.

You can change the execution plan of the query by adding query hints to the end of the query. For example, if you add OPTION (MAXDOP <integer_value>) to the end of a query that uses a single CPU, the query may use multiple CPUs (parallelism) depending on the value you choose. Or, you can use OPTION (RECOMPILE) to ensure that the query generates a new, temporary plan each time it's executed.

```
--With maxdop hint
SELECT ProductID, OrderQty, SUM(LineTotal) AS Total  
FROM Sales.SalesOrderDetail  
WHERE UnitPrice < $5.00  
GROUP BY ProductID, OrderQty  
ORDER BY ProductID, OrderQty  
OPTION (MAXDOP 2)
GO

--With recompile hint
SELECT City
FROM Person.Address
WHERE StateProvinceID=15 OPTION (RECOMPILE)
GO
```
                       
Although query hints may provide a localized solution to various performance-related issues, you should avoid using them in production environment for the following reasons.

Having a permanent query hint on your query can result in structural database changes that would be beneficial to that query not being applicable.
You can't benefit from new and improved features in subsequent versions of SQL Server if you bind a query to a specific execution plan.
However, there are several query hints available on SQL Server, which are used for different purposes. Let's discuss a few of them below:

FAST <integer_value>—retrieves the first <integer_value> number of rows while continuing query execution. It works better with small data sets and low value for fast query hint. As row count is increased, query cost becomes higher.

OPTIMIZE FOR—provides instructions to the query optimizer that a particular value for a local variable should be used when a query is compiled and optimized.

USE PLAN—the query optimizer will use a query plan specified by the xml_plan attribute.

RECOMPILE—creates a new, temporary plan for the query and discards it immediately after the query is executed.

{ LOOP | MERGE | HASH } JOIN—specifies all join operations are performed by LOOP JOIN, MERGE JOIN, or HASH JOIN in the whole query. The optimizer chooses the least expensive join strategy from among the options if you specify more than one join hint.

MAXDOP <integer_value>—overrides the max degree of parallelism value of sp_configure. The query specifying this option also overrides the Resource Governor.

You can also apply multiple query hints in the same query. The following example uses the HASH GROUP and FAST <integer_value> query hints in the same query.

```
SELECT ProductID, OrderQty, SUM(LineTotal) AS Total  
FROM Sales.SalesOrderDetail  
WHERE UnitPrice < $5.00  
GROUP BY ProductID, OrderQty  
ORDER BY ProductID, OrderQty  
OPTION (HASH GROUP, FAST 10);  
GO    
```
                       
To learn more about query hints, see Hints (Transact-SQL).

Query Store hints (in preview)
The Query Store hints feature in Azure SQL Database provides a simple method for shaping query plans without modifying application code.

Query Store hints are useful when the query optimizer doesn't generate an efficient execution plan, and when the developer or DBA can't modify to the original query text. In some applications, the query text may be hardcoded or automatically generated.

Screenshot of how Query Store hints work.

To use Query Store hints, you need to identify the Query Store query_id of the query statement you wish to modify through Query Store catalog views, built-in Query Store reports, or Query Performance Insight for Azure SQL Database. Then, execute sp_query_store_set_hints with the query_id and query hint string you wish to apply to the query.

The example below shows how to obtain the query_id for a specific query, and then use it to apply the RECOMPILE and MAXDOP hints to the query.

```
SELECT q.query_id, qt.query_sql_text
FROM sys.query_store_query_text qt 
    INNER JOIN sys.query_store_query q 
        ON qt.query_text_id = q.query_text_id 
WHERE query_sql_text like N'%ORDER BY CustomerName DESC%'  
  AND query_sql_text not like N'%query_store%'
GO

--Assuming the query_id returned by the previous query is 42
EXEC sys.sp_query_store_set_hints @query_id= 42, @query_hints = N'OPTION(RECOMPILE, MAXDOP 1)'
GO
```
                       
There are a few scenarios where Query Store hints can help with query-level performance issues.

Recompile a query on each execution.
Limit the maximum degree of parallelism for a statistic update operation.
Use a Hash join instead of a Nested Loops join.
Use compatibility level 110 for a specific query while keeping the database at the current compatibility.
 Note

Query Store hints are also supported by SQL Managed Instance.

For more information about Query Store hints, see Query Store hints.
