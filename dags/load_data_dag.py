from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
import pandas as pd
from datetime import datetime

# 1️⃣ Extract data from source tables
def extract_data():
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')

    # Extract orders, customers, products
    df_orders = mysql_hook.get_pandas_df('SELECT * FROM orders')
    df_customers = mysql_hook.get_pandas_df('SELECT * FROM customers')
    df_products = mysql_hook.get_pandas_df('SELECT * FROM products')

    # Save to CSVs for intermediate step
    df_orders.to_csv('/opt/airflow/dags/data_orders.csv', index=False)
    df_customers.to_csv('/opt/airflow/dags/data_customers.csv', index=False)
    df_products.to_csv('/opt/airflow/dags/data_products.csv', index=False)

# 2️⃣ Transform data into fact and dimension tables
def transform_data():
    # Load extracted CSVs
    df_orders = pd.read_csv('/opt/airflow/dags/data_orders.csv')
    df_customers = pd.read_csv('/opt/airflow/dags/data_customers.csv')
    df_products = pd.read_csv('/opt/airflow/dags/data_products.csv')

    # --- Dimension Tables ---
    dim_customers = df_customers[['customer_id', 'name', 'email', 'join_date']].drop_duplicates()
    dim_products = df_products[['product_id', 'name', 'category', 'price']].drop_duplicates()

    # --- Fact Table ---
    df_orders = df_orders.merge(df_products[['product_id', 'price']], on='product_id', how='left')
    df_orders['total_price'] = df_orders['quantity'] * df_orders['price']
    fact_orders = df_orders[['order_id', 'customer_id', 'product_id', 'order_date', 'quantity', 'total_price']]

    # Save transformed data
    dim_customers.to_csv('/opt/airflow/dags/dim_customers.csv', index=False)
    dim_products.to_csv('/opt/airflow/dags/dim_products.csv', index=False)
    fact_orders.to_csv('/opt/airflow/dags/fact_orders.csv', index=False)

# 3️⃣ Load transformed data into MySQL
def load_data():
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    # Load dim_customers
    dim_customers = pd.read_csv('/opt/airflow/dags/dim_customers.csv')
    for _, row in dim_customers.iterrows():
        cursor.execute("""
            REPLACE INTO dim_customers (customer_id, name, email, join_date)
            VALUES (%s, %s, %s, %s)
        """, tuple(row))

    # Load dim_products
    dim_products = pd.read_csv('/opt/airflow/dags/dim_products.csv')
    for _, row in dim_products.iterrows():
        cursor.execute("""
            REPLACE INTO dim_products (product_id, name, category, price)
            VALUES (%s, %s, %s, %s)
        """, tuple(row))

    # Load fact_orders
    fact_orders = pd.read_csv('/opt/airflow/dags/fact_orders.csv')
    for _, row in fact_orders.iterrows():
        cursor.execute("""
            REPLACE INTO fact_orders (order_id, customer_id, product_id, order_date, quantity, total_price)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, tuple(row))

    conn.commit()
    cursor.close()

# DAG definition
with DAG(
    dag_id='full_etl_pipeline',
    default_args={'owner': 'airflow', 'start_date': datetime(2025, 5, 7)},
    schedule_interval=None,
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    extract_task >> transform_task >> load_task
