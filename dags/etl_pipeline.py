from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
import pandas as pd
from datetime import datetime

# Define the function to extract data
def extract_data():
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    sql = 'SELECT * FROM orders;'
    df = mysql_hook.get_pandas_df(sql)
    df.to_csv('/opt/airflow/data/extracted_data.csv', index=False)


# Define the function to load data
def load_data():
    df = pd.read_csv('/opt/airflow/data/extracted_data.csv')

    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    for _, row in df.iterrows():
        sql = """
            INSERT INTO fact_orders (order_id, customer_id, product_id, order_date, quantity, total_price)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, tuple(row))
    conn.commit()
    cursor.close()

# Define the DAG
with DAG(
    dag_id='etl_pipeline',
    default_args={'owner': 'airflow', 'start_date': datetime(2025, 5, 7)},
    schedule_interval=None,
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data,
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_data,
    )

    extract_task >> load_task
