# E-commerce Data Warehouse Project

This project demonstrates a simple ETL (Extract, Transform, Load) data pipeline built using **Apache Airflow** and **MySQL**, simulating an e-commerce use case. The pipeline extracts data from CSV files, loads it into a MySQL database, and transforms it to create a customer sales summary.

---

## 📁 Project Structure

ecommerce_dwh_project/
│
├── dags/
│ ├── load_csv_to_mysql_dag.py # DAG to load CSVs to MySQL
│ ├── customer_sales_summary_dag.py # DAG to create summary table
│ └── ... # Other supporting DAGs
│
├── data/
│ ├── customers.csv
│ ├── orders.csv
│ └── products.csv
│
├── init.sql # SQL script to initialize MySQL tables
├── docker-compose.yml # Docker config to run Airflow and MySQL
└── README.md # Project documentation


---

## ⚙️ Technologies Used

- **Apache Airflow**
- **MySQL**
- **Docker**
- **Python**
- **SQL**
- **Pandas** (optional, for data handling)

---

## 🚀 How It Works

### Step 1: Load CSV files into MySQL

- **DAG**: `load_csv_to_mysql_dag.py`
- Reads `customers.csv`, `orders.csv`, and `products.csv`
- Inserts data into corresponding MySQL tables

### Step 2: Create Customer Sales Summary

- **DAG**: `customer_sales_summary_dag.py`
- Aggregates total orders, quantity, and sales for each customer
- Stores results in a new table: `customer_sales_summary`

---

## 🐳 Setup Instructions (Using Docker)

### 1. Clone the Repository

```bash
git clone https://github.com/ashwi1n/ecommerce_dwh_project.git
cd ecommerce_dwh_project


2. Start Docker Containers
docker-compose up --build
Airflow web UI: http://localhost:8080

MySQL: Port 3306

Username/Password: root/root

3 Access Airflow UI
Username: airflow

Password: airflow

Trigger DAGs manually or schedule them.

⚠️ Default credentials are for local testing only. Do not use them in production.

MySQL Tables
Tables Created
customers

orders

products

customer_sales_summary (transformed result)

Example SQL Query
SELECT customer_id, total_orders, total_quantity, total_sales
FROM customer_sales_summary;

Scheduling
    Airflow DAGs can be scheduled as follows:

schedule_interval='0 1 * * *'  # Every day at 1 AM

Sample Output

| customer\_id | total\_orders | total\_quantity | total\_sales |
| ------------ | ------------- | --------------- | ------------ |
| 1            | 3             | 4               | 1141.47      |
| 2            | 2             | 3               | 449.97       |
| 3            | 1             | 1               | 89.50        |

Key Learnings

Hands-on experience building ETL pipelines using Airflow

Integrating Docker, MySQL, and Airflow

Data modeling and transformation

Automating workflows using DAGs and scheduling

Contact
Created by Ashwin
Feel free to reach out or raise issues!
