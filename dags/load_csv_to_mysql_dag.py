from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import mysql.connector

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 15),
    'retries': 1,
}

dag = DAG(
    'load_csv_to_mysql_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Load CSVs into MySQL for E-Commerce Data Warehouse',
)

def get_mysql_connection():
    return mysql.connector.connect(
        host='mysql',  # service name in docker-compose
        user='root',
        password='root',
        database='ecommerce_dwh'
    )
def load_csv_to_mysql(file_name, table_name, columns, converters):
    file_path = f'/opt/airflow/data/{file_name}'
    # Added quotechar to correctly parse your CSV with quotes around rows
    df = pd.read_csv(file_path, delimiter=',', header=0, quoting=3)

    print(f"[INFO] Loading {file_name} into {table_name}")
    print("Columns found:", df.columns.tolist())

    # Validate CSV header
    for col in columns:
        if col not in df.columns:
            raise ValueError(f"Missing column '{col}' in {file_name}")

    conn = get_mysql_connection()
    cursor = conn.cursor()

    placeholders = ", ".join(["%s"] * len(columns))
    column_str = ", ".join(columns)

    # Build ON DUPLICATE KEY UPDATE clause (skip first column - assumed primary key)
    update_clause = ", ".join([f"{col}=VALUES({col})" for col in columns[1:]])

    insert_query = f"""
        INSERT INTO {table_name} ({column_str})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause}
    """

    # Convert each row’s columns according to converters list
    rows = [
        tuple(converters[i](row[col]) for i, col in enumerate(columns))
        for _, row in df.iterrows()
    ]

    cursor.executemany(insert_query, rows)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"[SUCCESS] Loaded {len(rows)} rows into {table_name}")



# Task functions
def load_orders():
    load_csv_to_mysql(
        file_name='orders.csv',
        table_name='fact_orders',
        columns=['order_id', 'customer_id', 'product_id', 'order_date', 'quantity', 'total_price'],
        converters=[int, int, int, str, int, float]
    )

def load_customers():
    load_csv_to_mysql(
        file_name='customers.csv',
        table_name='dim_customers',
        columns=['customer_id', 'name', 'email', 'join_date'],
        converters=[int, str, str, str]
    )

def load_products():
    load_csv_to_mysql(
        file_name='products.csv',
        table_name='dim_products',
        columns=['product_id', 'name', 'category', 'price'],
        converters=[int, str, str, float]
    )

# Airflow tasks
load_orders_task = PythonOperator(
    task_id='load_orders',
    python_callable=load_orders,
    dag=dag,
)

load_customers_task = PythonOperator(
    task_id='load_customers',
    python_callable=load_customers,
    dag=dag,
)

load_products_task = PythonOperator(
    task_id='load_products',
    python_callable=load_products,
    dag=dag,
)

# Order: load customers/products before orders
[load_customers_task, load_products_task] >> load_orders_task
