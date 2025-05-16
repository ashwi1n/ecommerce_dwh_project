import pandas as pd
from airflow.hooks.mysql_hook import MySqlHook

def load_csv_to_mysql(file_path, table_name):
    df = pd.read_csv(file_path)
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    for _, row in df.iterrows():
        placeholders = ', '.join(['%s'] * len(row))
        columns = ', '.join(row.index)
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(sql, tuple(row))

    conn.commit()
    cursor.close()

# Example usage inside Airflow container
load_csv_to_mysql('/opt/airflow/data/orders.csv', 'fact_orders')
load_csv_to_mysql('/opt/airflow/data/products.csv', 'dim_products')
load_csv_to_mysql('/opt/airflow/data/customers.csv', 'dim_customers')
