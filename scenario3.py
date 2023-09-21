from dagster import job, op, schedule, get_dagster_logger, DefaultScheduleStatus
import sqlite3
from sqlite3 import Error
from datetime import datetime
from py_topping.data_connection.database import lazy_SQL
import pandas as pd


@op
def create_connection():
    conn = None
    try:
        conn = sqlite3.connect('medcury-de.db')
    except Error as e:
        print(e)

    return conn


@op
def update_shipper_performance(conn, query_date):
    logger = get_dagster_logger()
    sql = f"""
    SELECT p.SupplierID as SupplierID, o.ShipCountry as ShipCountry, AVG(julianday(o.ShippedDate) - julianday(o.OrderDate)) as 'duration(day)', CURRENT_TIMESTAMP as last_calculated_date FROM Orders o
    LEFT JOIN 'Order Details' od ON o.OrderID = od.OrderID
    LEFT JOIN Products p ON p.ProductID = od.ProductID
    WHERE o.OrderDate <= '{query_date}'
    GROUP BY SupplierID, ShipCountry
    """
    df = pd.read_sql(sql, con=conn)

    mysql = lazy_SQL(sql_type='sqlite',
                     host_name='medcury-de.db', database_name='')

    result = mysql.dump_replace(df, 'SupplierShipDuration', list_key=[
        'SupplierID', 'ShipCountry'])
    logger.info(result)


@op
def scenario3_op(context):
    query_date = context.op_config["query_date"] if context.op_config["query_date"] else datetime.now(
    ).strftime("%Y-%m")
    update_shipper_performance(create_connection(), query_date)


@job
def scenario3_job():
    scenario3_op()


@schedule(
    cron_schedule="30 0 * * *",
    job=scenario3_job,
    execution_timezone="Asia/Bangkok",
    default_status=DefaultScheduleStatus.RUNNING
)
def scenario3_scheduler():
    return {
        "ops": {
        }
    }
