from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    'test_mysql_connection',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    test_mysql = MySqlOperator(
        task_id='test_mysql_connection',
        mysql_conn_id='mysql_conn',  # Replace this with your MySQL connection name in Airflow
        sql='SELECT 1;',
    )
