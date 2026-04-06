resource "aws_s3_bucket" "emr-studio-187623" {
  bucket = "emr-studio-187623"
  region = var.AWS_REGION
  force_destroy = true
}

resource "aws_s3_bucket" "training-data-23421" {
  bucket = "training-data-23421"
  region = var.AWS_REGION
  force_destroy = true
}

resource "aws_s3_bucket" "mlflow-artifacts-98761" {
  bucket = "mlflow-artifacts-98761"
  region = var.AWS_REGION
  force_destroy = true
}

resource "aws_s3_bucket" "feature-store-786q23" {
  bucket = "feature-store-786q23"
  region = var.AWS_REGION
  force_destroy = true
}

resource "aws_emrserverless_application" "training-spark-app" {
  name = "training-spark-app"
  release_label = "emr-7.6.0"
  type = "spark"

  initial_capacity {
    initial_capacity_type = "Driver"
    initial_capacity_config {
      worker_count = 1
      worker_configuration {
        cpu = "8 vCPU"
        memory = "40 GB"
      }
    }
  }

  auto_stop_configuration {
    enabled = true
    idle_timeout_minutes = 5
  }

  maximum_capacity {
    cpu = "12 vCPU"
    memory = "80 GB"
  }
}

resource "aws_sagemaker_domain" "ds-sm-studio" {
  domain_name = "ds-sm-studio"
  auth_mode = "IAM"
  vpc_id = var.VPC_ID
  subnet_ids = split(",", var.SUBNET_IDS)

  default_user_settings {
    execution_role = aws_iam_role.sm-exec-role.arn
  }
}

resource "aws_sagemaker_user_profile" "ds-user-profile" {
  domain_id = aws_sagemaker_domain.ds-sm-studio.id
  user_profile_name = "ds-user-profile"
}

locals {
  # Categorical/Identifier columns
  string_features = ["Customer_ID", "Target"]

  # Numerical and One-Hot Encoded columns
  integral_features = [
    "Age", "Annual_Income", "Spending_score", "Num_of_Children", "Credit_Score",
    "Gender_Male", "Region_North", "Region_East", "Region_West",
    "Marital_Status_Widowed", "Marital_Status_Single", "Marital_Status_Married",
    "Employment_Status_Unemployed", "Employment_Status_Student", "Employment_Status_Employed",
    "Online_Shopping_Frequency_18", "Online_Shopping_Frequency_19", "Online_Shopping_Frequency_5",
    "Online_Shopping_Frequency_4", "Online_Shopping_Frequency_17", "Online_Shopping_Frequency_0",
    "Online_Shopping_Frequency_7", "Online_Shopping_Frequency_14", "Online_Shopping_Frequency_15",
    "Online_Shopping_Frequency_16", "Online_Shopping_Frequency_3", "Online_Shopping_Frequency_9",
    "Online_Shopping_Frequency_13", "Online_Shopping_Frequency_10", "Online_Shopping_Frequency_1",
    "Online_Shopping_Frequency_2", "Online_Shopping_Frequency_12", "Online_Shopping_Frequency_6",
    "Online_Shopping_Frequency_11"
  ]
}

resource "aws_sagemaker_feature_group" "offline_store" {
  feature_group_name = "user-behavior-v1"
  record_identifier_feature_name = "Customer_ID"
  event_time_feature_name = "created_time"
  role_arn = aws_iam_role.sagemaker-feature-store-role.arn

  feature_definition {
    feature_name = "created_time"
    feature_type = "String"
  }

  dynamic "feature_definition" {
    for_each = local.string_features
    content {
      feature_name = feature_definition.value
      feature_type = "String"
    }
  }

  dynamic "feature_definition" {
    for_each = local.integral_features
    content {
      feature_name = feature_definition.value
      feature_type = "Integral"
    }
  }

  offline_store_config {
    s3_storage_config {
      s3_uri = "s3://${aws_s3_bucket.feature-store-786q23.bucket}"
    }
  }
}

resource "aws_glue_catalog_database" "inference_db" {
  name = "inference_db"
}

resource "aws_glue_catalog_table" "customer_table" {
  name          = "customer_table"
  database_name = aws_glue_catalog_database.inference_db.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"        = "csv"
    "skip.header.line.count" = "1"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.training-data-23421.bucket}/inference-data/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "csv_serde"
      serialization_library = "org.apache.hadoop.hive.serde2.OpenCSVSerde"

      parameters = {
        "separatorChar" = ","
        "quoteChar"     = "\""
        "escapeChar"    = "\\"
      }
    }

    columns {
      name = "customer_id"
      type = "string"
    }
  }
}

resource "aws_sagemaker_mlflow_tracking_server" "ds-tracking-server" {
  tracking_server_name = "ds-tracking-server"
  role_arn = aws_iam_role.sm-exec-role.arn
  artifact_store_uri = "s3://${aws_s3_bucket.mlflow-artifacts-98761.bucket}/mlflow"
  tracking_server_size = "Small"
  mlflow_version = "3.0"
}

output "sagemaker_feature_group_arn" {
  value = aws_sagemaker_feature_group.offline_store.arn
  description = "The Amazon Resource Name (ARN) of the SageMaker Feature Group"
}

output "emr_application_id" {
  value = aws_emrserverless_application.training-spark-app.id
  description = "Application ID of EMR serverless"
}

output "mlflow_tracking_server_arn" {
  value = aws_sagemaker_mlflow_tracking_server.ds-tracking-server.arn
  description = "The ARN of the MLflow Tracking Server"
}

output "mlflow_tracking_server_url" {
  value = aws_sagemaker_mlflow_tracking_server.ds-tracking-server.tracking_server_url
  description = "The DNS endpoint for the MLflow Tracking Server"
}
