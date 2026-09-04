# E-commerce Data Engineering Pipeline

##  Project Overview

An end-to-end batch data engineering pipeline built using **Python, Apache Spark (PySpark), and Parquet**.

The project processes e-commerce data through a **Bronze → Silver → Gold** architecture, applying data cleansing, validation, transformations, joins, aggregations, and business-level analytics.

The goal is to simulate a real-world data engineering workflow where raw transactional data is transformed into reliable, analytics-ready datasets.

---

## Architecture

```text
                 ┌──────────────────┐
                 │   Raw CSV Data   │
                 │                  │
                 │ customers        │
                 │ orders           │
                 │ order_items      │
                 │ products         │
                 │ reviews          │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │      BRONZE      │
                 │                  │
                 │ CSV → Parquet    │
                 │ Raw structured   │
                 │ datasets         │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │      SILVER      │
                 │                  │
                 │ Cleaning         │
                 │ Validation       │
                 │ Standardization  │
                 │ Derived columns  │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │       GOLD       │
                 │                  │
                 │ Business-ready   │
                 │ analytical data  │
                 └────────┬─────────┘
                          │
              ┌───────────┼───────────┐
              ▼           ▼           ▼
        Customer Sales  Product   Monthly Sales
                       Analytics
                           │
                           ▼
                    Product Ratings
```

---

##  Technologies

* **Python**
* **Apache Spark**
* **PySpark**
* **Parquet**
* **SQL**
* **Git / GitHub**
* **Java 21** for local Spark execution

---

##  Source Data

The project uses five e-commerce datasets:

| Dataset     | Records |
| ----------- | ------: |
| Customers   |   1,000 |
| Orders      |   5,000 |
| Order Items |  12,000 |
| Products    |     200 |
| Reviews     |   3,000 |

The datasets contain customer, order, product, transaction, and review information.

---

##  Bronze Layer

The Bronze layer converts the raw CSV files into **Parquet datasets**.

Each source dataset is processed independently because the CSV files have different schemas.

```text
data/bronze/
├── customers/
├── orders/
├── order_items/
├── products/
└── reviews/
```

### Bronze objectives

* Ingest raw data
* Preserve source information
* Convert CSV to Parquet
* Create structured datasets for downstream processing

---

##  Silver Layer

The Silver layer cleans and validates the Bronze datasets.

### Customers

* Trimmed string columns
* Standardized email values
* Validated customer IDs
* Validated registration dates
* Removed duplicate records

### Orders

* Trimmed and standardized status/payment values
* Validated order IDs and customer IDs
* Validated order dates
* Validated allowed order statuses
* Validated payment methods
* Removed duplicates

### Order Items

* Validated order and product IDs
* Validated positive quantities
* Validated prices
* Rounded prices
* Created derived `line_total`

```text
line_total = quantity × price
```

### Products

* Trimmed product information
* Standardized categories
* Validated product IDs
* Validated prices
* Rounded prices

### Reviews

* Trimmed review text
* Validated review/customer/product IDs
* Validated ratings between 1 and 5
* Validated review dates
* Removed duplicates

Silver datasets are stored as Parquet:

```text
data/silver/
├── customers/
├── orders/
├── order_items/
├── products/
└── reviews/
```

---

##  Gold Layer

The Gold layer contains business-ready analytical datasets.

### 1. Customer Sales

Provides customer-level sales metrics.

```text
customer_id
customer_name
total_orders
total_items
total_spend
```

Uses:

* Joins
* `countDistinct`
* Aggregations
* `sum`
* `coalesce`
* Left joins

The left join ensures customers without orders are still retained.

---

### 2. Product Performance

Provides product-level sales performance.

```text
product_id
product_name
category
units_sold
revenue
```

Calculates:

* Units sold
* Product revenue

---

### 3. Monthly Sales

Provides monthly sales performance.

```text
year
month
total_orders
total_items
revenue
```

Uses Spark date functions such as:

```python
year()
month()
```

The project contains monthly sales data from **September 2024 through September 2026**, based on the source data.

---

### 4. Product Ratings

Combines product sales and customer review information.

```text
product_id
product_name
category
units_sold
revenue
average_rating
review_count
```

A key design decision was to **aggregate sales and reviews separately before joining them**.

This prevents many-to-many join multiplication.

For example:

```text
50 order-item records
×
10 review records
=
500 joined records
```

Aggregating before joining prevents this from incorrectly inflating revenue, units sold, or review counts.

---

## Data Quality Validation

The project includes data-quality validation using PySpark.

One important validation checks for orphan order items.

```python
unmatched = items.join(
    orders,
    items.order_id == orders.order_id,
    "left_anti"
)
```

The validation confirmed:

```text
Order items:       12,000
Unmatched items:        0
```

This confirms that all order-item records have a corresponding order.

---

## Project Structure

```text
ecommerce-data-engineering/
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── docs/
│
├── notebooks/
│
├── sql/
│
├── src/
│   ├── ingestion/
│   │   └── generate_data.py
│   │
│   ├── transformations/
│   │   ├── bronze_*.py
│   │   ├── silver_*.py
│   │   └── gold_*.py
│   │
│   └── utils/
│
├── tests/
│
├── .gitignore
├── requirements.txt
└── README.md
```

Generated Bronze, Silver, and Gold Parquet data is excluded from Git using `.gitignore`.

---

## How to Run

### 1. Clone the repository

```bash
git clone <repository-url>
cd ecommerce-data-engineering
```

### 2. Create a virtual environment

```bash
python -m venv .venv
```

### 3. Activate the environment

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

### 4. Install dependencies

```bash
pip install -r requirements.txt
```

### 5. Run the PySpark transformations

Example:

```bash
python src/transformations/silver_customers.py
```

Gold transformations can then be executed to generate the analytical datasets.

---

## Key Data Engineering Concepts Demonstrated

This project demonstrates practical experience with:

* PySpark DataFrames
* Spark transformations
* Spark actions
* Reading and writing Parquet
* Schema handling
* Data cleansing
* Data validation
* Filtering
* Aggregations
* GroupBy
* Inner joins
* Left joins
* Left-anti joins
* `countDistinct`
* `sum`
* `avg`
* `coalesce`
* Date transformations
* Derived columns
* Multi-table transformations
* Bronze/Silver/Gold architecture
* Handling many-to-many join multiplication
* Batch ETL processing

---

## Future Cloud Architecture

The local project is designed as the foundation for a cloud-based data engineering implementation.

The next phase will migrate the pipeline to Azure:

```text
Data Sources
     │
     ▼
Azure Data Factory
     │
     ▼
Azure Data Lake Storage
     │
     ▼
Databricks
     │
     ├── Bronze
     ├── Silver
     └── Gold
     │
     ▼
Delta Lake
     │
     ▼
Power BI
```

Future enhancements will include:

* Azure Data Factory pipelines
* Azure Data Lake Storage Gen2
* Databricks
* Delta Lake
* Incremental data processing
* Job scheduling
* Cloud-based monitoring
* Power BI dashboards

---

## Project Objective

This project demonstrates how raw e-commerce data can be transformed into reliable, analytics-ready datasets using modern data engineering practices.

The local implementation focuses on **PySpark and batch ETL**, while the next phase extends the same architecture into **Azure and Databricks**.
