from typing import Optional

import kfp.dsl as dsl

from pipelines.components.activation.component import (
    validate_activation_config_op,
    activation_ga4mp_op,
    activation_bq_routine_op,
)
from pipelines.components.bigquery.component import (
    bq_call_create_dataset_op,
    bqml_predict_op,
)
from pipelines.components.common.component import run_metadata_op
from pipelines.components.vertex.component import (
    vai_get_default_model_op,
    vai_batch_prediction_op,
)


@dsl.pipeline()
def prediction_pipeline_bqml(
    gcp_project_id: str,
    gcp_region: str,
    bq_dataset_id: str,
    data_date_start_days_ago: int,
    activation_config: Optional[dict],
):
    run = (
        run_metadata_op(data_date_start_days_ago=data_date_start_days_ago)
        .set_cpu_limit("1")
        .set_memory_limit("1G")
    )

    ds = (
        bq_call_create_dataset_op(
            project=gcp_project_id,
            run_id=run.outputs["run_id"],
            dataset_id=bq_dataset_id,
            p_mode="INFERENCE",
            p_date_start=run.outputs["date_start"],
            p_date_end=run.outputs["date_end"],
        )
        .set_cpu_limit("1")
        .set_memory_limit("1G")
        .after(run)
    )

    bqml_best_model = (
        vai_get_default_model_op(
            project=gcp_project_id,
            region=gcp_region,
            dataset_id=bq_dataset_id,
        )
        .set_cpu_limit("1")
        .set_memory_limit("1G")
        .after(run)
    )

    bqml_predict = (
        bqml_predict_op(
            project=gcp_project_id,
            dataset_id=bq_dataset_id,
            run_id=run.outputs["run_id"],
            inference_table=ds.outputs["inference_table"],
            model=bqml_best_model.outputs["default_model"],
        )
        .set_cpu_limit("1")
        .set_memory_limit("1G")
    )

    # ga4mp activation
    act_ga4mp_cfg = (
        validate_activation_config_op(
            activation_type="ga4mp", activation_config=activation_config
        )
        .set_cpu_limit("1")
        .set_memory_limit("1G")
    )

    with dsl.Condition(
        name="check-if-activation-ga4mp-config-exists",
        condition=act_ga4mp_cfg.outputs["valid"] == True,
    ):
        activation_ga4mp_op(
            project=gcp_project_id,
            dataset_id=bq_dataset_id,
            run_id=run.outputs["run_id"],
            cfg=act_ga4mp_cfg.outputs["config"],
        ).after(bqml_predict)

    # bq_routine activation
    act_bq_routine_cfg = (
        validate_activation_config_op(
            activation_type="bq_routine", activation_config=activation_config
        )
        .set_cpu_limit("1")
        .set_memory_limit("1G")
    )

    with dsl.Condition(
        name="check-if-activation-bq-routine-config-exists",
        condition=act_bq_routine_cfg.outputs["valid"] == True,
    ):
        activation_bq_routine_op(
            project=gcp_project_id,
            dataset_id=bq_dataset_id,
            run_id=run.outputs["run_id"],
            cfg=act_bq_routine_cfg.outputs["config"],
        ).after(bqml_predict)


@dsl.pipeline()
def prediction_pipeline_custom(
    gcp_project_id: str,
    gcp_region: str,
    bq_dataset_id: str,
    data_date_start_days_ago: int,
    activation_config: Optional[dict],
):
    run = (
        run_metadata_op(data_date_start_days_ago=data_date_start_days_ago)
        .set_cpu_limit("1")
        .set_memory_limit("1G")
    )

    ds = (
        bq_call_create_dataset_op(
            project=gcp_project_id,
            run_id=run.outputs["run_id"],
            dataset_id=bq_dataset_id,
            p_mode="INFERENCE",
            p_date_start=run.outputs["date_start"],
            p_date_end=run.outputs["date_end"],
        )
        .set_cpu_limit("1")
        .set_memory_limit("1G")
        .after(run)
    )

    bqml_best_model = (
        vai_get_default_model_op(
            project=gcp_project_id,
            region=gcp_region,
            dataset_id=bq_dataset_id,
        )
        .set_cpu_limit("1")
        .set_memory_limit("1G")
        .after(run)
    )

    vai_predict = (
        vai_batch_prediction_op(
            project=gcp_project_id,
            dataset_id=bq_dataset_id,
            run_id=run.outputs["run_id"],
            inference_table=ds.outputs["inference_table"],
            model=bqml_best_model.outputs["default_model"],
        )
        .set_cpu_limit("1")
        .set_memory_limit("1G")
    )

    # ga4mp activation
    act_ga4mp_cfg = (
        validate_activation_config_op(
            activation_type="ga4mp", activation_config=activation_config
        )
        .set_cpu_limit("1")
        .set_memory_limit("1G")
    )

    with dsl.Condition(
        name="check-if-activation-ga4mp-config-exists",
        condition=act_ga4mp_cfg.outputs["valid"] == True,
    ):
        activation_ga4mp_op(
            project=gcp_project_id,
            dataset_id=bq_dataset_id,
            run_id=run.outputs["run_id"],
            cfg=act_ga4mp_cfg.outputs["config"],
        ).after(vai_predict)

    # bq_routine activation
    act_bq_routine_cfg = (
        validate_activation_config_op(
            activation_type="bq_routine", activation_config=activation_config
        )
        .set_cpu_limit("1")
        .set_memory_limit("1G")
    )

    with dsl.Condition(
        name="check-if-activation-bq-routine-config-exists",
        condition=act_bq_routine_cfg.outputs["valid"] == True,
    ):
        activation_bq_routine_op(
            project=gcp_project_id,
            dataset_id=bq_dataset_id,
            run_id=run.outputs["run_id"],
            cfg=act_bq_routine_cfg.outputs["config"],
        ).after(vai_predict)
