import os

import joblib
import mlflow
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

if __name__ == "__main__":
    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])

    # This evaluates to "/opt/ml/input/data/training"
    # input_data_dir = '/home/ravi/projects/public_repo/sagemaker-mlops-ref-arch/emr/src/train/part-00000-20a40138-b3cd-4c69-bd56-3d0cefc9b35a.c000.snappy.parquet'
    input_data_dir = '/opt/ml/input/data/training'

    # Pandas handles reading the directory and concatenating the files natively!
    print(f"Loading all parquet files from {input_data_dir}...")
    df = pd.read_parquet(input_data_dir)
    print(df.columns)

    # The rest of your script stays the same
    non_feature_cols = [
        'created_time',
        'Customer_ID',
        'Target',
        'write_time',
        'api_invocation_time',
        'is_deleted'
    ]

    X = df.drop(columns=non_feature_cols, errors='ignore')
    y = df['Target']
    print(f"Dataset loaded successfully. Shape: {X.shape}")

    with mlflow.start_run() as run:
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X, y)
        print(f'Model trained successful')

        # Log the model in mlflow
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            registered_model_name="CustomerClassificationModel"
        )

        # mlflow.evaluate()

        # Needed for Sagemaker to write to S3
        model_output_dir = os.environ.get('SM_MODEL_DIR', './')
        joblib.dump(model, os.path.join(model_output_dir, 'model.joblib'))
        print('Model saved locally for SageMaker artifact upload.')
