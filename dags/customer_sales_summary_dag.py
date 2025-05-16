from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    'customer_sales_summary_transform',
    default_args=default_args,
    schedule_interval='0 1 * * *',  # 🔁 This makes it run every day at 1 AM
    catchup=False,
) as dag:

    run_transformation = MySqlOperator(
        task_id='transform_customer_sales',
        mysql_conn_id='mysql_conn',
        sql="""
            INSERT INTO customer_sales_summary (customer_id, total_orders, total_quantity, total_sales)
            SELECT customer_id,
                   COUNT(order_id) AS total_orders,
                   SUM(quantity) AS total_quantity,
                   SUM(total_price) AS total_sales
            FROM fact_orders
            GROUP BY customer_id
            ON DUPLICATE KEY UPDATE
                total_orders = VALUES(total_orders),
                total_quantity = VALUES(total_quantity),
                total_sales = VALUES(total_sales);
        """
    )
