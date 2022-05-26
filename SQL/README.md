# T-SQL for Data Engineering

Microsoft database systems such as SQL Server, Azure SQL Database, Azure Synapse Analytics, and others use a dialect of SQL called Transact-SQL, or T-SQL. T-SQL includes language extensions for writing stored procedures and functions, which are application code that is stored in the database, and managing user accounts.

* <a href="#section1">T-SQL</a>
  * <a href="#section1-1">Querying with Transact-SQL</a>
  * <a href="#section1-2">Advanced Programming with Transact-SQL</a>
  * <a href="$section1-3">T-SQL</a>

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
 
### <h3 id="section1-2">Advanced Programming with Transact-SQL</h3>
  
## <h2 id="section2">Optimize query performance in Azure SQL</h2>

Analyze individual query performance and determine where improvements can be made. Explore performance-related Dynamic Management Objects. Investigate how indexes and database design affect queries.


### <h3 id="section2-1">Describe SQL Server query plans</h3>

Read and understand various forms of execution plans. Compare estimated vs actual plans. Learn how and why plans are generated.

### <h3 id="section2-2">Explore performance-based design</h3>

Explore normalization for relational databases. Investigate the impact of proper datatype usage. Compare types of indexes.

### <h3 id="section2-3">Evaluate performance improvements</h3>

Evaluate possible changes to indexes. Determine the impact of changes to queries and indexes. Explore Query Store hints.
