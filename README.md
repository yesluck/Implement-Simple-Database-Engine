# README

******************************************************************

## Part 1: Files Description

README.md		This file

/src:

	CSVCatalog.py	My implementation of CSV Catalog, based on the scaffolding code.
	
	CSVTable.py	My implementation of CSV Table, based on the scaffolding code.
	
	DataTableExceptions.py	Define the common exceptions
	
/test:

	unit_test_catalog-1.py		test code 1.
	
	unit_test_catalog-1.txt		My results for test code 1.
	
	unit_test_catalog-2.py		test code 2.
	
	unit_test_catalog-2.txt		My results for test code 2.
	
	unit_test_catalog-3.py		My own test code, added 5 more test functions.
	
	unit_test_catalog-3.txt		My results for my own test code.
	
	unit_test_csv_table-1.py	test code.
	
	unit_test_csv_table-1.txt	My results for test code.
	
	unit_test_csv_table-2.py	My own test code, added 1 more test functions (joining Batting and Appearances).
	
	unit_test_csv_table-2.txt	My results for my own test code.
	
	unit_tests_catalog_json.py	test code.
	
	unit_tests_catalog_json.txt	My results for test code.
	
/data:

	People.csv
	
	Teams.csv
	
	Batting.csv
	
	Appearances.csv
	
/sql:

	create.sql SQL statements to create your tables and constraints for the catalog.


******************************************************************

## Part 2: Main Designing Ideas

### 2.1 CSV Catalog

I mainly used the thoughts professor provided in class. I create 3 tables, “tables”, “columns” and “indexes”. And when dropping a table, corresponding rows will be dropped from “columns” and “indexes” immediately. When dropping a column definition, corresponding rows will be dropped from “indexes” immediately.

## 2.2 CSV Table

I did two optimizations in “JOIN”:
1. if probe table has index but scan table doesn't, we swap the scan and probe tables.
2. select pushdown


******************************************************************

## Part 3: My Test Cases

### 3.1 CSV Catalog

Note: Much more detailed descriptions of my test cases are in the comments of my code.

In the function test_table_6(), I tested column definition, dropping column definitions, index (and primary key) definition, dropping index definitions, get and load another table. And in the other four functions, I tested some common errors that will be raised by my program.

### 3.2 CSV Table

In order to challenge my code harder, I did a JOIN between “Batting” (104324 rows) and “Appearances” (104256 rows) on “playerID” and “teamID” where “teamID=SEA”. The final result contains 3189796 rows, which is a much larger amount than Prof’s example. But it only took my program less than 3s to complete this query.
