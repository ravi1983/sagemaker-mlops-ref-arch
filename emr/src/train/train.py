import json
import os

import joblib
import mlflow
import pandas as pd
from mlflow.models import infer_signature
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split

if __name__ == "__main__":
    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    mlflow.set_experiment('sk-classification')

    # This evaluates to "/opt/ml/input/data/training"
    input_data_dir = '/opt/ml/input/data/training'

    # Pandas handles reading the directory and concatenating the files natively!
    print(f"Loading all parquet files from {input_data_dir}...")
    df = pd.read_parquet(input_data_dir)
    print(df.columns)

    # The rest of your script stays the same
    non_feature_cols = [
        'created_time',
        'customer_id',
        'target',
        'write_time',
        'api_invocation_time',
        'is_deleted'
    ]

    X = df.drop(columns=non_feature_cols, errors='ignore')
    y = df['target']
    print(f"Dataset loaded successfully. Shape: {X.shape}")

    print("Splitting data into train and test sets...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    with mlflow.start_run() as run:
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X, y)
        print(f'Model trained successful')

        # Log the model in mlflow
        signature = infer_signature(X_test, model.predict(X_test))
        mlflow.sklearn.log_model(
            sk_model=model,
            name="model",
            signature=signature,
            input_example=X_test.iloc[[0]]
        )
        print("Model logged to MLflow")

        # Model eval
        y_pred = model.predict(X_test)

        # Log accuracy
        accuracy = accuracy_score(y_test, y_pred)
        mlflow.log_metric("accuracy", accuracy)
        print(f"Accuracy: {accuracy:.4f}")

        # Log classification metrics
        report = classification_report(y_test, y_pred, output_dict=True)
        for class_label, metrics in report.items():
            # 'accuracy' is already a single float in the dict, so skip it
            if class_label == 'accuracy':
                continue

            # Formats keys like: "class_0_precision" or "macro avg_f1-score"
            mlflow.log_metric(f"{class_label}_precision", metrics['precision'])
            mlflow.log_metric(f"{class_label}_recall", metrics['recall'])
            mlflow.log_metric(f"{class_label}_f1-score", metrics['f1-score'])


        # Needed by dagster
        dagster_data = {
            'run_name': run.info.run_name,
            'run_id': run.info.run_id,
            'experiment_name': 'sk-classification',
            'artifact_uri': run.info.artifact_uri
        }
        output_dir = os.environ.get('SM_MODEL_DIR', './')
        with open(os.path.join(output_dir, 'dagster_data.json'), 'w') as f:
            json.dump(dagster_data, f)

        # Needed for Sagemaker to write to S3
        model_output_dir = os.environ.get('SM_MODEL_DIR', './')
        joblib.dump(model, os.path.join(model_output_dir, 'model.joblib'))
        print('Model saved locally for SageMaker artifact upload.')
