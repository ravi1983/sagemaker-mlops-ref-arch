import os
import time

import boto3
import dagster as dg

from dagster_aws.pipes import PipesEMRServerlessClient
from sagemaker.core import ScriptProcessor

athena = boto3.client('athena')


@dg.asset(compute_kind="athena")
def inference_data() -> dg.Output[str]:
    return dg.Output(
        value=os.environ['ATHENA_RAW_INFERENCE_TABLE'],
        metadata={
            'Raw Inference Data': os.environ['ATHENA_RAW_INFERENCE_TABLE']
        }
    )


@dg.asset(compute_kind="athena")
def enrich_inference_data(context: dg.AssetExecutionContext,
                          load_training_data_to_s3: dict,
                          inference_data: str) -> dg.Output[str]:
    enriched_inferenced_data = f's3://{os.environ['TRAINING_BUCKET_NAME']}/inference-data/{context.run_id}/'
    query = f"""
        UNLOAD
            (SELECT features.*
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY Customer_ID
                        ORDER BY write_time DESC
                    ) as row_num
                FROM {load_training_data_to_s3['offline_store_athena_table']}
            ) features
            INNER JOIN {inference_data} inference
                ON features.Customer_ID = inference.customer_id
            WHERE row_num = 1 AND is_deleted = false)
        TO '{enriched_inferenced_data}'
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
        value=enriched_inferenced_data,
        metadata={
            "Athena Query": dg.MetadataValue.text(query),
            "Enriched Inferenced Data": dg.MetadataValue.text(enriched_inferenced_data)
        }
    )


@dg.asset(key_prefix=["emr"], kinds={"spark", "s3"})
def perform_inference(context: dg.AssetExecutionContext,
                      pipes_emr_serverless_client: PipesEMRServerlessClient,
                      enrich_inference_data: str,
                      check_and_register_model: dict):


    model_name = check_and_register_model['registered_model']
    model_version = check_and_register_model['registered_model_version']

    run = pipes_emr_serverless_client.run(
        context=context,
        start_job_run_params={
            "applicationId": os.environ['EMR_APP_ID'],
            "executionRoleArn": f"arn:aws:iam::{os.environ['ACCOUNT_NUM']}:role/emr-job-runtime-role",
            "jobDriver": {
                "sparkSubmit": {
                    "entryPoint": f"s3://{os.environ['TRAINING_BUCKET_NAME']}/inference/inference.py",
                    "entryPointArguments": [os.environ['MLFLOW_TRACKING_ARN'], f'models:/{model_name}/{model_version}',
                                            enrich_inference_data,
                                            f"{os.environ['TRAINING_BUCKET_NAME']}/inference-result"],
                    "sparkSubmitParameters": (
                        f"--conf spark.archives=s3://{os.environ['TRAINING_BUCKET_NAME']}/transform/pyspark_venv.tar.gz#environment "
                        "--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python "
                        "--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python "
                        "--conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
                    ),
                }
            },
            "clientToken": f'{context.run_id}-1',
            "configurationOverrides": {
                "monitoringConfiguration": {
                    "cloudWatchLoggingConfiguration": {"enabled": True}
                }
            },
        },
    )

    yield from run.get_results()
