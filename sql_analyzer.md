# SQL Analyzer

## Problem

Data Analysts get lost in a sea of tables in their Data Warehouse. It takes time to find the right table and understand its schema. This is a problem that can be solved with a SQL Analyzer. Also, even if you know the table, it takes time to write the query. This is another problem that can be solved with a SQL Analyzer.

## Solution

An Analyzer that does the following tasks:
1. Collects all tables from a schema
2. Collects all metadata from tables
3. Takes samples from tables
4. Using an LLM, attempts to understand the data in the table using the sample and the metadata
5. Constructs a knowledge graph of how the tables interact with each other

Once SQL Analyzer has the knowledge graph, sample and metadata, it can do the following:
1. Given a SQL query, it can QA the query, verifying that the query is correct and suggesting improvements
2. Given a question for a complex metric, it can suggest the right tables to use and the right joins to use and generate the appropriate SQL query

# Resources and References

sqlparse
sqlalchemy reflect Metadata
sqlfluff
Meta UPM
NetworkX

# Implementation

## Schema Explorer

1. Collect all tables from a schema
2. Collect all metadata from tables
3. Takes samples from tables
4. Send to OpenAI for summarization and description
5. Construct a knowledge graph of how the tables interact with each other
6. Store the knowledge graph in a graph database

## SQL QA Assistant

1. Given question, access stored knowledge graph
2. Suggest tables to use
3. Generate prompt for OpenAI to generate SQL query
4. Generate SQL query and use react to test it against the data warehouse
5. If query is correct, return results

# Ideas 

Use ctags to compress code into the context window of GPT-4, and give it as much context as possible.
[universal ctags](https://github.com/universal-ctags/ctags)
