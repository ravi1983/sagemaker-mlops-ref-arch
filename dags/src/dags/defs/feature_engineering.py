import os

import dagster as dg

from dagster_aws.pipes import PipesEMRServerlessClient


@dg.asset
def training_data() -> dg.Output[str]:
    return dg.Output(
        value=os.environ['RAW_ASSET'],
        metadata={
            'raw_data': os.environ['RAW_ASSET']
        }
    )


@dg.asset
def feature_engineer_data(context: dg.AssetExecutionContext,
                          pipes_emr_serverless_client: PipesEMRServerlessClient,
                          training_data: str):
    run = pipes_emr_serverless_client.run(
        context=context,
        start_job_run_params={
            "applicationId": os.environ['EMR_APP_ID'],
            "executionRoleArn": f"arn:aws:iam::{os.environ['ACCOUNT_NUM']}:role/emr-job-runtime-role",
            "jobDriver": {
                "sparkSubmit": {
                    "entryPoint": f"s3://{os.environ['TRAINING_BUCKET_NAME']}/transform/transform.py",
                    "entryPointArguments": [training_data, os.environ['FEATURE_STORE_BUCKER_NAME'],
                                            os.environ['ACCOUNT_NUM'], os.environ['FEATURE_TABLE_KEY']],
                    "sparkSubmitParameters": (
                        f"--conf spark.archives=s3://{os.environ['TRAINING_BUCKET_NAME']}/transform/pyspark_venv.tar.gz#environment "
                        "--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python "
                        "--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python "
                        "--conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
                    ),
                }
            },
            "clientToken": context.run_id,
            "configurationOverrides": {
                "monitoringConfiguration": {
                    "cloudWatchLoggingConfiguration": {"enabled": True}
                }
            },
        },
    )

    yield from run.get_results()
