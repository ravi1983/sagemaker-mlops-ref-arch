from sagemaker.core import image_uris
from sagemaker.core.helper.session_helper import Session

from sagemaker.core.shapes import S3DataSource, OutputDataConfig
from sagemaker.core.training.configs import SourceCode, Compute, InputData
from sagemaker.train.model_trainer import ModelTrainer

sm_bucket = 'mlflow-artifacts-98761'
sagemaker_session = Session(default_bucket=sm_bucket)

trainer = ModelTrainer(
    training_image=image_uris.retrieve(framework='sklearn', region='us-east-2', version='1.4-2'),
    role='xxx',
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
        s3_output_path=f"s3://{sm_bucket}/sagemaker/sklearn-classification"
    ),
    environment={
        'MLFLOW_TRACKING_URI': 'xxxx'
    }
)

trainer.train(
    input_data_config=[
        InputData(
            channel_name="training",
            data_source=S3DataSource(s3_uri='s3://training-data-23421/training-data/7db6d841-da7b-43c3-9c20-ccddb68b696a/', s3_data_type="S3Prefix")
        )
    ]
)
