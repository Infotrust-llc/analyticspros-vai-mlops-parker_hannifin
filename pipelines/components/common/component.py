from typing import NamedTuple

from pipelines import config, base_image
import kfp.dsl as dsl


@dsl.component(base_image=base_image)
def run_metadata_op(
    data_date_start_days_ago: int,
) -> NamedTuple("Outputs", [("run_id", str), ("date_start", str), ("date_end", str)],):
    import datetime

    return (
        datetime.datetime.utcnow().strftime("%Y%m%d%H%M"),
        (
            datetime.date.today() - datetime.timedelta(days=data_date_start_days_ago)
        ).strftime("%Y-%m-%d"),
        datetime.date.today().strftime("%Y-%m-%d"),
    )
