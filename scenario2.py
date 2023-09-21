from dagster import job, op, schedule, get_dagster_logger, DefaultScheduleStatus
import sqlite3
from sqlite3 import Error
import pandas as pd
import calendar
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
import os


@op
def create_connection():
    conn = None
    try:
        conn = sqlite3.connect('medcury-de.db')
    except Error as e:
        print(e)

    return conn


def get_month_names_for_quarter(year, quarter):
    if quarter < 1 or quarter > 4:
        raise ValueError("Quarter must be between 1 and 4.")

    start_month = 3 * (quarter - 1) + 1

    month_names = []
    year_month = []
    for month in range(start_month, start_month + 3):
        month_name = calendar.month_abbr[month]
        month_names.append(month_name)
        year_month.append(f"{year}-{month}")
    return month_names, year_month


@op
def upload_to_goole(file_path):
    logger = get_dagster_logger()
    key_path = '/opt/dagster/app/princ_assignment/key.json'
    creds = service_account.Credentials.from_service_account_file(
        filename=key_path)

    try:
        # create drive api client
        service = build('drive', 'v3', credentials=creds)
        logger.info(os.path.basename(file_path))
        file_metadata = {'name': os.path.basename(file_path), 'parents': [
            '1l-CeKfqexf4WvhP_kSWwUrudABAyYWT6']}
        media = MediaFileUpload(file_path, mimetype='application/excel')
        file = service.files().create(body=file_metadata, media_body=media).execute()
        logger.info(file)

    except HttpError as error:
        logger.info(F'An error occurred: {error}')
        file = None


@op
def get_product_sales_amount_by_quarter(conn, year, quarter):
    logger = get_dagster_logger()

    month_name, year_month = get_month_names_for_quarter(year, quarter)
    logger.info(month_name)

    sql = f"""
    SELECT * FROM ProductSalesAmountByMonth WHERE yearMonth in {tuple(year_month)}
    """
    df = pd.read_sql(sql, con=conn)

    output_file = f'/opt/dagster/app/princ_assignment/{year}_Q{quarter}_data.xlsx'
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for month in range(0, 3):
            df[df['yearMonth'] == year_month[month]].to_excel(
                writer, sheet_name=month_name[month], index=False)
    return (output_file)


@op
def scenario2_op(context):
    year = context.op_config["year"] if context.op_config["year"] else 0
    quarter = context.op_config["quarter"] if context.op_config["quarter"] else 0
    upload_to_goole(get_product_sales_amount_by_quarter(
        create_connection(), year, quarter))


@job
def scenario2_job():
    scenario2_op()


@schedule(
    cron_schedule="30 0 * * *",
    job=scenario2_job,
    execution_timezone="Asia/Bangkok",
    default_status=DefaultScheduleStatus.RUNNING
)
def scenario2_scheduler():
    return {
        "ops": {
        }
    }
