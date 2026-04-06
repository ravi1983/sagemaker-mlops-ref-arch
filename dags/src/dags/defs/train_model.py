import json
import os
import time

import boto3
import dagster as dg
import mlflow
from mlflow import MlflowClient
from sagemaker.core import image_uris
from sagemaker.core.helper.session_helper import Session

from sagemaker.core.shapes import S3DataSource, OutputDataConfig
from sagemaker.core.training.configs import SourceCode, Compute, InputData
from sagemaker.train.model_trainer import ModelTrainer

athena = boto3.client('athena')


@dg.asset(
    deps=[dg.AssetKey('feature_engineer_data')],
    compute_kind="athena"
)
def load_training_data_to_s3(context: dg.AssetExecutionContext) -> dg.Output[dict]:
    upstream_key = dg.AssetKey(['emr', 'feature_engineer_data'])
    latest_materialization = context.instance.get_latest_materialization_event(upstream_key)

    metadata = latest_materialization.asset_materialization.metadata
    created_time = metadata.get("created_time").value
    run_id = metadata.get("run_id").value

    train_data_loc = f's3://{os.environ['TRAINING_BUCKET_NAME']}/training-data/{run_id}/'
    query = f"""
        UNLOAD
            (SELECT *
            FROM (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY Customer_ID 
                        ORDER BY write_time DESC
                    ) as row_num
                FROM {os.environ["ATHENA_TABLE_NAME"]}
                WHERE created_time <= '{created_time}'
            )
            WHERE row_num = 1 AND is_deleted = false)
        TO '{train_data_loc}'
        WITH (format='PARQUET')
    """

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': os.environ['ATHENA_DB'], 'Catalog': os.environ['ATHENA_CATALOG']}
    )
    exec_id = response['QueryExecutionId']

    # Loop until status is success
    while True:
        result = athena.get_query_execution(QueryExecutionId=exec_id)
        status = result['QueryExecution']['Status']['State']

        if status == 'SUCCEEDED':
            context.log.info(f"Athena query {exec_id} succeeded.")
            break

        elif status in ['FAILED', 'CANCELLED']:
            reason = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            raise Exception(f"Athena query {exec_id} failed with state {status}. Reason: {reason}")

        else:
            print(f"Query {exec_id} is still {status}. Waiting 5 seconds...")
            time.sleep(10)

    return dg.Output(
        value={
            "train_data_loc": train_data_loc,
            "offline_store_athena_table": os.environ["ATHENA_TABLE_NAME"],
        },
        metadata={
            "Training Data Load Query": dg.MetadataValue.text(query),
            "Training Data Loc": dg.MetadataValue.text(train_data_loc)
        }
    )


@dg.asset(key_prefix=["sagemaker"], kinds={"sagemaker"})
def create_training_job(load_training_data_to_s3: dict) -> dg.Output[str]:
    sm_bucket = 'mlflow-artifacts-98761'
    sagemaker_session = Session(default_bucket=sm_bucket)

    # Kick off training job and wait
    trainer = ModelTrainer(
        training_image=image_uris.retrieve(framework='sklearn', region='us-east-2', version='1.4-2'),
        role=f"arn:aws:iam::{os.environ['ACCOUNT_NUM']}:role/sm-exec-role",
        base_job_name="sklearn-classification",
        sagemaker_session=sagemaker_session,
        source_code=SourceCode(
            source_dir='/home/ravi/projects/public_repo/sagemaker-mlops-ref-arch/emr/src/train',
            entry_script='train.py',
            requirements='requirements.txt'
        ),
        compute=Compute(
            instance_type="ml.m5.xlarge",
            instance_count=1
        ),
        output_data_config=OutputDataConfig(
            s3_output_path=f"s3://{sm_bucket}/sklearn-classification"
        ),
        environment={
            'MLFLOW_TRACKING_URI': os.environ['MLFLOW_TRACKING_ARN'],
            'S3_HANDSHAKE_LOC': f's3://{sm_bucket}/dagster'
        }
    )
    trainer.train(
        input_data_config=[
            InputData(
                channel_name="training",
                data_source=S3DataSource(s3_uri=load_training_data_to_s3['train_data_loc'], s3_data_type="S3Prefix")
            )
        ]
    )

    # Read training result and materialize
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=sm_bucket, Key='dagster/dagster_data.json')
    dagster_data = json.loads(response['Body'].read().decode('utf-8'))

    return dg.Output(
        value=dagster_data['run_id'],
        metadata={
            'MLFlow Run ID': dg.MetadataValue.text(dagster_data['run_id']),
            'MLFlow Artifact Path': dg.MetadataValue.text(dagster_data['artifact_uri']),
            'SageMaker S3 Path': dg.MetadataValue.text(
                f's3://{sm_bucket}/sklearn-classification/{dagster_data['sm_job_name']}'
            ),
        }
    )


@dg.asset(key_prefix=["sagemaker"], kinds={"sagemaker"})
def check_and_register_model(create_training_job: str) -> dg.Output[dict]:
    # Get run information from mlflow
    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_ARN"])
    client = MlflowClient()
    run_info = client.get_run(create_training_job)

    # If accuracy is > 0.75 register the model and materialize it
    if run_info.data.metrics['accuracy'] > 0.75:
        model_uri = f"runs:/{create_training_job}/model"
        registered_model = mlflow.register_model(
            model_uri=model_uri,
            name="CustomerClassificationModel",
            tags={
                'run_id': create_training_job,
                'experiment': 'sk-classification'
            },
        )

        # Add champion alias
        client.set_registered_model_alias(
            name=registered_model.name,
            alias='champion',
            version=registered_model.version
        )

        return dg.Output(
            value={
                "registered_model": registered_model.name,
                "registered_model_version": registered_model.version
            },
            metadata={
                "Registered Model Name": registered_model.name,
                "Registered Model Version": registered_model.version,
            }
        )

    print("Model accuracy too low. Skipping registration.")
    return None
