from typing import Optional

import kfp.dsl as dsl

from pipelines.components.bigquery.component import (
    bq_call_create_dataset_op,
    bqml_training_op,
    bqml_list_models_op,
    bqml_model_evaluate_op,
    bqml_model_cleanup_op,
    bq_calc_percentile_map_op,
)
from pipelines.components.vertex.component import (
    vai_list_models_op,
    vai_model_evaluate_op,
    vai_custom_training_op,
    vai_model_cleanup_op,
)
from pipelines.components.common.component import run_metadata_op


@dsl.pipeline()
def training_pipeline_bqml(
    gcp_project_id: str,
    bq_dataset_id: str,
    data_date_start_days_ago: int,
    create_model_params: dict,
    keep_n_best_models: Optional[int],
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
            p_mode="TRAINING",
            p_date_start=run.outputs["date_start"],
            p_date_end=run.outputs["date_end"],
        )
        .set_cpu_limit("1")
        .set_memory_limit("1G")
    )

    bqml_training = (
        bqml_training_op(
            project=gcp_project_id,
            run_id=run.outputs["run_id"],
            dataset_id=bq_dataset_id,
            training_table=ds.outputs["training_table"],
            create_model_params=create_model_params,
        )
        .set_cpu_limit("1")
        .set_memory_limit("1G")
    )

    bqml_models = (
        bqml_list_models_op(project=gcp_project_id, dataset_id=bq_dataset_id)
        .set_cpu_limit("1")
        .set_memory_limit("1G")
        .after(bqml_training)
    )

    with dsl.ParallelFor(
        name="eval-each-model", items=bqml_models.output, parallelism=1
    ) as model:
        bqml_eval = (
            bqml_model_evaluate_op(
                project=gcp_project_id,
                run_id=run.outputs["run_id"],
                model=model,
                training_table=ds.outputs["training_table"],
            )
            .set_cpu_limit("1")
            .set_memory_limit("1G")
        )

    bqml_model_cleanup = (
        bqml_model_cleanup_op(
            project=gcp_project_id,
            dataset_id=bq_dataset_id,
            keep_n_best_models=keep_n_best_models,
            eval_metrics=dsl.Collected(bqml_eval.outputs["metrics"]),
        )
        .set_cpu_limit("1")
        .set_memory_limit("1G")
    )

    bq_calc_percentile_map_op(
        project=gcp_project_id,
        dataset_id=bq_dataset_id,
        run_id=run.outputs["run_id"],
        model=bqml_model_cleanup.outputs["best_model"],
        training_table=ds.outputs["training_table"],
    ).set_cpu_limit("1").set_memory_limit("1G").after(bqml_model_cleanup)


@dsl.pipeline()
def training_pipeline_custom(
    gcp_project_id: str,
    gcp_region: str,
    bq_dataset_id: str,
    data_date_start_days_ago: int,
    custom_training_params: dict,
    keep_n_best_models: Optional[int],
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
            p_mode="TRAINING",
            p_date_start=run.outputs["date_start"],
            p_date_end=run.outputs["date_end"],
        )
        .set_cpu_limit("1")
        .set_memory_limit("1G")
    )

    vai_custom_training = (
        vai_custom_training_op(
            project=gcp_project_id,
            region=gcp_region,
            run_id=run.outputs["run_id"],
            dataset_id=bq_dataset_id,
            training_table=ds.outputs["training_table"],
            custom_training_params=custom_training_params,
        )
        .set_cpu_limit("1")
        .set_memory_limit("1G")
    )

    vai_models = (
        vai_list_models_op(
            project=gcp_project_id, region=gcp_region, dataset_id=bq_dataset_id
        )
        .set_cpu_limit("1")
        .set_memory_limit("1G")
        .after(vai_custom_training)
    )

    with dsl.ParallelFor(
        name="eval-each-model", items=vai_models.output, parallelism=1
    ) as model:
        vai_eval = (
            vai_model_evaluate_op(
                project=gcp_project_id,
                region=gcp_region,
                run_id=run.outputs["run_id"],
                model=model,
                training_table=ds.outputs["training_table"],
                custom_training_params=custom_training_params,
            )
            .set_cpu_limit("1")
            .set_memory_limit("1G")
        )

    vai_model_cleanup = (
        vai_model_cleanup_op(
            project=gcp_project_id,
            dataset_id=bq_dataset_id,
            keep_n_best_models=keep_n_best_models,
            eval_metrics=dsl.Collected(vai_eval.outputs["metrics"]),
        )
        .set_cpu_limit("1")
        .set_memory_limit("1G")
    )
