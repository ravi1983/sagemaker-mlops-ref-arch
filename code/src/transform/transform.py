import logging
import sys
from datetime import datetime, timezone

from dagster_pipes import open_dagster_pipes
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline

logging.basicConfig(level=logging.INFO)

def transform_data(df, created_time, pipes):
    """Encapsulates the imputation and encoding logic."""
    cat_features = ['Gender', 'Region', 'Marital_Status', 'Employment_Status', 'Online_Shopping_Frequency']

    # Basic impute
    df = df.na.fill(value=0.0).na.fill(value="N/A")
    pipes.log.info("Imputed dataframe")

    # Add creation time
    df = df.withColumn('created_time', F.lit(created_time))

    # Define and fit Pipeline
    indexers = [StringIndexer(inputCol=c, outputCol=c + "_tmp", handleInvalid="keep") for c in cat_features]
    encoders = [OneHotEncoder(inputCol=c + "_tmp", outputCol=c + "_vec") for c in cat_features]

    pipeline = Pipeline(stages=indexers + encoders)
    model = pipeline.fit(df)
    encoded_df = model.transform(df)
    pipes.log.info("Transformed dataframe via Pipeline")

    # Flatten and cleanup
    for i, c in enumerate(cat_features):
        labels = model.stages[i].labels
        # Flattening vectors into individual columns
        for j, label in enumerate(labels[:-1]):
            col_name = f"{c}_{label}".replace(" ", "_")
            encoded_df = encoded_df.withColumn(col_name, F.udf(lambda v: int(v[j]), "int")(F.col(f"{c}_vec")))

        # Cleanup original and temp columns
        encoded_df = encoded_df.drop(c, f"{c}_tmp", f"{c}_vec")

    pipes.log.info("Flattened vectors and cleaned up columns")
    return encoded_df

def load_offline_feature_store(df, utc_now, pipes):
    utc_timestamp = utc_now.timestamp()

    df_prepared = df.withColumn("api_invocation_time", F.lit(int(utc_timestamp * 1000))) \
        .withColumn("write_time", F.lit(int(utc_timestamp * 1000))) \
        .withColumn("is_deleted", F.lit(False)) \
        .withColumn("year", F.year("created_time")) \
        .withColumn("month", F.lpad(F.month("created_time"), 2, '0')) \
        .withColumn("day", F.lpad(F.dayofmonth("created_time"), 2, '0')) \
        .withColumn("hour", F.lpad(F.hour("created_time"), 2, '0'))

    s3_uri = f"s3://{sys.argv[2]}/{sys.argv[3]}/sagemaker/us-east-2/offline-store/{sys.argv[4]}/data/"
    df_prepared.write \
        .partitionBy("year", "month", "day", "hour") \
        .mode("append") \
        .parquet(s3_uri)
    pipes.log.info("Successfully updated offline feature store")

    return s3_uri


def main():
    with open_dagster_pipes() as pipes:
        pipes.log.info(f"All arguments: {sys.argv}")
        if len(sys.argv) != 5:
            pipes.log.error("Error: Missing required arguments.")
            sys.exit(1)

        spark = (SparkSession.builder
                 .appName("customer-data-transform-app")
                 .getOrCreate())

        # Accessing the Run ID from Dagster Pipes context
        run_id = pipes.run_id if pipes.run_id else "default_run"

        df_schema = StructType([
            StructField('Customer_ID', StringType(), True),
            StructField('Age', IntegerType(), True),
            StructField('Gender', StringType(), True),
            StructField('Annual_Income', IntegerType(), True),
            StructField('Spending_score', IntegerType(), True),
            StructField('Region', StringType(), True),
            StructField('Marital_Status', StringType(), True),
            StructField('Num_of_Children', IntegerType(), True),
            StructField('Employment_Status', StringType(), True),
            StructField('Credit_Score', IntegerType(), True),
            StructField('Online_Shopping_Frequency', StringType(), True),
            StructField('Target', StringType(), True)
        ])

        # Load
        df = spark.read.csv(sys.argv[1], header=True, schema=df_schema)
        pipes.log.info("Loaded dataframe from S3")

        # Transform
        utc_now = datetime.now(timezone.utc)
        created_time = utc_now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        transformed_df = transform_data(df, created_time, pipes)

        # Write to offline feature store
        s3_uri = load_offline_feature_store(transformed_df, utc_now, pipes)

        # Report back to Dagster
        pipes.report_asset_materialization(
            metadata={
                "s3_path": {"raw_value": s3_uri, "type": "text"},
                "run_id": {"raw_value": run_id, "type": "text"},
                "created_time": {"raw_value": created_time, "type": "text"}
            },
            data_version="alpha",
        )

        spark.stop()

if __name__ == "__main__":
    main()