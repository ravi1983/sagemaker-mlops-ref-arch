import sys

import mlflow
from dagster_pipes import open_dagster_pipes
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct

def main():
    with open_dagster_pipes() as pipes:
        if len(sys.argv) != 5:
            pipes.log.error("Error: Missing required arguments.")
            sys.exit(1)

        # 1. Start Spark
        spark = SparkSession.builder.appName("Offline-Inference").getOrCreate()

        # 2. Point to your AWS Managed MLflow Tracking Server
        mlflow.set_tracking_uri(sys.argv[1])
        run_id = pipes.run_id if pipes.run_id else "default_run"
        pipes.log.info(f'Started run for run id {run_id}')

        # 3. Pull the model as a Spark UDF
        # You can use the specific run_id or a registered model name
        model_uri = sys.argv[2]
        pipes.log.info(f'Model uri is  {model_uri}')
        predict_udf = mlflow.pyfunc.spark_udf(
            spark, model_uri=model_uri, result_type="double"
        )

        # 4. Load your enriched data (Assuming it is already joined with the Feature Store)
        enriched_df = spark.read.parquet(sys.argv[3])
        pipes.log.info('Successfully loaded DF')

        # 5. Extract features dynamically
        # Your script drops non-feature columns; we will target the ones kept.
        all_cols = enriched_df.columns
        non_feature_cols = [
            "created_time",
            "customer_id",
            "target",
            "write_time",
            "api_invocation_time",
            "is_deleted",
        ]
        feature_cols = [c for c in all_cols if c not in non_feature_cols]

        # 6. Perform the inference
        # We pass the features as a struct to match the Scikit-Learn pandas schema
        inference_df = enriched_df.withColumn(
            "prediction", predict_udf(struct(*[col(c) for c in feature_cols]))
        )
        pipes.log.info('Successfully performed prediction')

        # 7. Write output back to S3
        inference_df.select("customer_id", "prediction").write.mode(
            "overwrite"
        ).parquet(sys.argv[4] + '/' + run_id)
        pipes.log.info('Successfully wrote prediction to s3')

        pipes.report_asset_materialization(
            metadata={
                "Inference Result": {"raw_value": sys.argv[4] + '/' + run_id, "type": "text"}
            },
            data_version="alpha",
        )

        spark.stop()


if __name__ == "__main__":
    main()