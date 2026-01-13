# Batch Data Processing Using Airflow

## Repository Outline

Explanation about the content of each file and folder:

1. hafizal_shakur_DAG_graph.jpg - Screen capture of the DAG graph while it is running.
2. hafizal_shakur_DAG.py - Python program that runs Apache Airflow.
3. hafizal_shakur_data_clean.csv - Dataset generated after the data cleaning process using Airflow.
4. hafizal_shakur_data_raw.csv - Original dataset to be inserted into PostgreSQL.
5. hafizal_shakur_ddl.txt - DDL and DML syntax to create tables and restore data in PostgreSQL.
6. hafizal_shakur_GX.ipynb - Notebook for Data Validation using GreatExpectations.
7. Images - Folder containing screen captures of graphs and insights from Kibana.

## Problem Background
Recent data shows a significant shift in how people shop for daily needs. Since the pandemic, many customers have moved away from large supermarkets and grocery stores. Instead, they prefer shopping online or visiting small, local minimarkets that are closer to home. This change has caused a decrease in revenue for many large-scale retailers while smaller, more agile stores continue to grow. Additionally, global economic conditions are making customers more sensitive to prices.

This Dashboard and Report are designed to help ABC Store management analyze their current inventory and sales strategy. By understanding the inventory data, the business can find ways to maximize revenue and reduce unnecessary costs. The analysis focuses on:

- Product Categories: Understanding what is currently in stock.
- Sales Trends: Reviewing performance over a one-year period.
- Customer Promotion: Identifying the most popular items to create effective marketing.
- Supplier Management: Evaluating if suppliers are providing the best quality and prices.


## Project Output

The products of this project are an Airflow program for data cleaning and data saving, a cleaned dataset file (csv), and a Kibana dashboard based on the analysis.

## Data

The dataset is dummy data obtained from the Kaggle website. It contains a product inventory list from a supermarket, consisting of 16 columns and 989 rows of item names. The data cleaning process was handled using Airflow to ensure missing values and duplicated data were removed.


## Stacks
`
1. Programming Language : Python, SQL
2. Tools                : Visual Studio Code, Airflow, Elastic Search, Kibana, PgADmin (postgres), Docker, GitHub
3. Library              : pandas, psycopg2, airflow, elasticsearch

## Reference

URL Dataset   : https://www.kaggle.com/datasets/salahuddinahmedshuvo/grocery-inventory-and-sales-dataset/data
---