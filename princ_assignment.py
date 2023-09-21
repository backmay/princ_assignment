from dagster import job, op, repository, schedule, get_dagster_logger, DefaultScheduleStatus
import requests


@op
def test_op():
    logger = get_dagster_logger()
    r = requests.get('https://tradestie.com/api/v1/apps/reddit')
    logger.info(r.json())
    logger.info('===== V2 =====')


@job
def test_job():
    test_op()


@schedule(
    cron_schedule="30 0 * * *",
    job=test_job,
    execution_timezone="Asia/Bangkok",
    default_status=DefaultScheduleStatus.RUNNING
)
def test_scheduler():
    return {
        "ops": {
        }
    }


@repository
def test_repo():
    return [
        test_job, test_scheduler,
    ]
