# Introduction
SQL Data Warehouse Project using PostgreSQL (running in a Docker container). This project uses the medallion architecture.

## 🏗️ Data Architecture
The data architecture for this project follows Medallion Architecture  **Bronze**,  **Silver**, and  **Gold**  layers:  [![Data Architecture](https://github.com/tharrmeehan/SQL-Data-Warehouse-Project/raw/main/docs/data_architecture.png)](https://github.com/tharrmeehan/SQL-Data-Warehouse-Project/blob/main/docs/data_architecture.png)

1.  **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database. There are 6 different data sources.
2.  **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3.  **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.

## :book: Project Overview

This project involves:

1.  **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture  **Bronze**,  **Silver**, and  **Gold**  layers.
2.  **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3.  **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4.  **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.


## :star2: Credit/Acknowledgment
- Datawithbaraa

##  :lock: License
This project is licensed under the MIT License. You are free to use, modify, and share this project with proper attribution.