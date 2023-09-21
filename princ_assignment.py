from dagster import repository

from scenario1 import scenario1_job, scenario1_scheduler
from scenario2 import scenario2_job, scenario2_scheduler
from scenario3 import scenario3_job, scenario3_scheduler
from scenario4 import scenario4_job, scenario4_scheduler


@repository
def test_repo():
    return [
        scenario1_job, scenario1_scheduler,
        scenario2_job, scenario2_scheduler,
        scenario3_job, scenario3_scheduler,
        scenario4_job, scenario4_scheduler,
    ]
