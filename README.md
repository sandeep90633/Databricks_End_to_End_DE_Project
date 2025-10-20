# Databricks_End_to_End_DE_Project

The goal of this project is to design and implement a scalable data pipeline in Databricks following the medallion architecture (Bronze → Silver → Gold). The project aims to automate the ingestion, transformation, and modeling of flight-related datasets—ensuring data reliability, incremental processing, and readiness for analytical and reporting use cases.

## Project Description

This project demonstrates a complete end-to-end data engineering workflow built in Databricks. It ingests and processes multiple raw data sources to create clean, structured, and analytics-ready tables.

#### 1. Bronze Layer (Raw Ingestion)

Ingests raw data from multiple folders dynamically (flights, passengers, airports, bookings).

Implements an automated ingestion Databricks Job using dynamic parameters, allowing new data to be appended seamlessly.

#### 2. Silver Layer (Transformation & CDC)

Cleans, standardizes, and enriches the bronze data.

Handles Change Data Capture (CDC) using Delta Live Tables (DLT) to maintain up-to-date and accurate datasets.

#### 3. Gold Layer (Data Modeling)

Develops dimensional and fact models for analytics:

* Dimensions: Flights, Airports, Passengers

* Fact Table: Bookings

Enables downstream analytics and dashboard reporting with consistent, high-quality data.

