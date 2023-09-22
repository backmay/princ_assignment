from dagster import job, op, schedule, get_dagster_logger, DefaultScheduleStatus
import sqlite3
from sqlite3 import Error
from datetime import datetime
from py_topping.data_connection.database import lazy_SQL
import pandas as pd
import base64


@op
def create_connection():
    conn = None
    try:
        conn = sqlite3.connect('medcury-de.db')
    except Error as e:
        print(e)

    return conn


@op
def upsert_my_customers(conn):
    logger = get_dagster_logger()
    sql = f"""
    SELECT * FROM Customers
    """
    df = pd.read_sql(sql, con=conn)
    df['Phone'] = df['Phone'].str.replace(r'[^0-9]', '', regex=True)
    df['Fax'] = df['Fax'].str.replace(r'[^0-9]', '', regex=True)
    df['Address'] = df['Address'].apply(
        lambda x: base64.b64encode(x.encode()).decode())

    mysql = lazy_SQL(sql_type='sqlite',
                     host_name='medcury-de.db', database_name='')

    result = mysql.dump_replace(df, 'MyCustomers', list_key=[
        'CustomerID'])
    logger.info(result)


@op
def upsert_my_supplier(conn):
    logger = get_dagster_logger()
    sql = f"""
    SELECT * FROM Suppliers
    """
    df = pd.read_sql(sql, con=conn)
    df['Phone'] = df['Phone'].str.replace(r'[^0-9]', '', regex=True)
    df['Fax'] = df['Fax'].str.replace(r'[^0-9]', '', regex=True)
    df['Address'] = df['Address'].apply(
        lambda x: base64.b64encode(x.encode()).decode())

    mysql = lazy_SQL(sql_type='sqlite',
                     host_name='medcury-de.db', database_name='')

    result = mysql.dump_replace(df, 'MySuppliers', list_key=[
        'SupplierID'])
    logger.info(result)


@op
def scenario4_op(context):
    upsert_my_customers(create_connection())
    upsert_my_supplier(create_connection())


@job
def scenario4_job():
    scenario4_op()


@schedule(
    cron_schedule="00 07 * * *",
    job=scenario4_job,
    execution_timezone="Asia/Bangkok",
    default_status=DefaultScheduleStatus.RUNNING
)
def scenario4_scheduler():
    return {
        "ops": {
        }
    }
