import os

import dagster as dg

from sagemaker.core.modules.configs import InputData
from sagemaker.core.shapes import AthenaDatasetDefinition, DatasetDefinition, DataSource
from sagemaker.train.model_trainer import ModelTrainer

@dg.asset(
    deps=[dg.AssetKey('feature_engineer_data')],
    key_prefix=["sagemaker"],
    compute_kind="sagemaker"
)
def create_training_dataset(context: dg.AssetExecutionContext):
    upstream_key = dg.AssetKey("feature_engineer_data")
    latest_materialization = context.instance.get_latest_materialization_event(upstream_key)

    metadata = latest_materialization.asset_materialization.metadata
    created_time = metadata.get("created_time").value
    run_id = metadata.get("run_id").value

    query = f'select * from {os.environ["ATHENA_TABLE_NAME"]} where created_time={created_time}'
    train_data_loc = f's3://{os.environ['TRAINING_BUCKET_NAME']}/{run_id}/training-data'

    athena_def = AthenaDatasetDefinition(
        catalog=os.environ['ATHENA_CATALOG'],
        database=os.environ['ATHENA_DB'],
        query_string=query,
        output_s3_uri=train_data_loc,
        output_format='PARQUET'
    )
    dataset_def = DatasetDefinition(
        local_path="/opt/ml/input/data/training",
        athena_dataset_definition=athena_def
    )

    yield dg.MaterializeResult(
        asset_key=dg.AssetKey(["sagemaker", "create_training_dataset"]),
        metadata={
            "athena_query": dg.MetadataValue.text(query),
            "train_data_loc": dg.MetadataValue.text(train_data_loc)
        }
    )
    return dataset_def

@dg.asset(
    key_prefix=["sagemaker"],
    compute_kind="sagemaker"
)
def create_training_job(create_training_dataset: DatasetDefinition):
    trainer = ModelTrainer(
        training_image=f"{os.environ['ACCOUNT_NUM']}.dkr.ecr.us-east-1.amazonaws.com/my-training-image:latest",
        role=os.environ['SAGEMAKER_ROLE_ARN'],
        base_job_name="training-job"
    )

    return trainer.train(
        input_data_config=[
            InputData(
                channel_name="training",
                data_source=DataSource(dataset_definition=create_training_dataset)
            ),
        ],
    )