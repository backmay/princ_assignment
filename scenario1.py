from dagster import job, op, schedule, get_dagster_logger, DefaultScheduleStatus
import sqlite3
from sqlite3 import Error
import pandas as pd
from datetime import datetime


@op
def create_connection():
    conn = None
    try:
        conn = sqlite3.connect('medcury-de.db')
    except Error as e:
        print(e)

    return conn


@op
def update_product_sales_amouth_by_month(conn, year_month):
    logger = get_dagster_logger()
    cur = conn.cursor()
    sql = f"""
    WITH SalesCTE AS (
        SELECT strftime('%Y-%m', o.OrderDate) as yearMonth, p.ProductID, p.ProductName, SUM(od.UnitPrice*od.Quantity) as salesAmount
        FROM  'Orders' o
        LEFT JOIN 'Order Details' od ON o.OrderID = od.OrderID
        LEFT JOIN Products p ON p.ProductID = od.ProductID
        GROUP BY yearMonth, p.ProductID
    )
    SELECT
        s1.YearMonth AS YearMonth,
        s1.ProductId,
        s1.ProductName,
        s1.SalesAmount AS SalesAmount,
        (1.0 * (s1.SalesAmount - s2.SalesAmount) / s2.SalesAmount) * 100 AS percentage_change
    FROM
        SalesCTE s1
    LEFT JOIN
        SalesCTE s2 ON s1.ProductId = s2.ProductId
                AND s1.YearMonth = strftime('%Y-%m', '{year_month}-01')
                AND s2.YearMonth = strftime('%Y-%m', '{year_month}-01', '-1 month')
    WHERE s1.YearMonth = '{year_month}'
    """
    cur.execute(sql)

    rows = cur.fetchall()

    for row in rows:
        # Modify the columns accordingly
        insert_sql = f"INSERT OR REPLACE INTO ProductSalesAmountByMonth VALUES (?, ?, ?, ?, ?)"
        cur.execute(insert_sql, row)

    conn.commit()
    conn.close()
    logger.info(rows)


@op
def scenario1_op(context):
    year_month = context.op_config["year_month"] if context.op_config["year_month"] else datetime.now(
    ).strftime("%Y-%m")
    update_product_sales_amouth_by_month(create_connection(), year_month)


@job
def scenario1_job():
    scenario1_op()


@schedule(
    cron_schedule="0 0 1 * *",
    job=scenario1_job,
    execution_timezone="Asia/Bangkok",
    default_status=DefaultScheduleStatus.RUNNING
)
def scenario1_scheduler():
    return {
        "ops": {
        }
    }
