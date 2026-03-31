import os
import time

import boto3
import dagster as dg
from sagemaker.core import image_uris

from sagemaker.core.shapes import S3DataSource
from sagemaker.core.training.configs import SourceCode, Compute, InputData
from sagemaker.train.model_trainer import ModelTrainer

athena = boto3.client('athena')


@dg.asset(
    deps=[dg.AssetKey('feature_engineer_data')],
    key_prefix=["sagemaker"],
    compute_kind="sagemaker"
)
def load_training_data_to_s3(context: dg.AssetExecutionContext) -> dg.Output[str]:
    upstream_key = dg.AssetKey("feature_engineer_data")
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
    print(query)

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
        value=train_data_loc,
        metadata={
            "athena_query": dg.MetadataValue.text(query),
            "train_data_loc": dg.MetadataValue.text(train_data_loc),
            'exec_id': dg.MetadataValue.text(exec_id)
        }
    )


@dg.asset(
    key_prefix=["sagemaker"],
    compute_kind="sagemaker"
)
def create_training_job(load_training_data_to_s3: str):
    trainer = ModelTrainer(
        training_image=image_uris.retrieve(framework='sklearn', region='us-east-2', version='1.2-1'),
        role=os.environ['SAGEMAKER_ROLE_ARN'],
        base_job_name="sklearn-classification",
        source_code=SourceCode(
            source_dir='/home/ravi/projects/public_repo/sagemaker-mlops-ref-arch/emr/src/train',
            entry_script='train.py'
        ),
        compute=Compute(
            instance_type="ml.m5.xlarge",
            instance_count=1
        )
    )

    return trainer.train(
        input_data_config=[
            InputData(
                channel_name="training",
                data_source=S3DataSource(s3_uri=load_training_data_to_s3, s3_data_type="S3Prefix")
            )
        ]
    )
