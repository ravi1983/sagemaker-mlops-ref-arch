resource "aws_iam_role" "emr-job-runtime-role" {
  name = "emr-job-runtime-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "emr-serverless.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "emr-serverless-exec-role" {
  name = "emr-serverless-exec-role"
  role = aws_iam_role.emr-job-runtime-role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Effect = "Allow"
        Resource = [
          aws_s3_bucket.emr-studio-187623.arn,
          "${aws_s3_bucket.emr-studio-187623.arn}/*",
          aws_s3_bucket.training-data-23421.arn,
          "${aws_s3_bucket.training-data-23421.arn}/*",
          aws_s3_bucket.feature-store-786q23.arn,
          "${aws_s3_bucket.feature-store-786q23.arn}/*"
        ]
      },
      {
        "Effect": "Allow",
        "Action": [
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer"
        ],
        "Resource": "arn:aws:ecr:us-east-1:*:repository/emr-serverless-image"
      },
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams",
          "logs:DescribeLogGroups"
        ]
        Effect   = "Allow"
        Resource = ["arn:aws:logs:*:*:*"]
      }
    ]
  })
}

resource "aws_iam_role" "sm-exec-role" {
  name = "sm-exec-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "sagemaker.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "sm-exec-access" {
  name = "sm-exec-profile-access"
  role = aws_iam_role.sm-exec-role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sagemaker:DescribeUserProfile",
          "sagemaker:ListUserProfiles"
        ]
        Resource = aws_sagemaker_user_profile.ds-user-profile.arn
      },
      {
        Effect = "Allow"
        Action = [
          "sagemaker:DescribeApp",
          "sagemaker:DescribeSpace",
          "sagemaker:DescribeUserProfile",
          "sagemaker:DescribeDomain"
        ]
        Resource = [
          aws_sagemaker_domain.ds-sm-studio.arn,
          "${aws_sagemaker_domain.ds-sm-studio.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "sagemaker:ListSpaces",
          "sagemaker:ListApps",
          "sagemaker:ListUserProfiles",
          "sagemaker:Search",
          "sagemaker:createPresignedDomainUrl",
          "sagemaker:describeApp"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "sagemaker:DescribeFeatureGroup"
        ]
        Resource = aws_sagemaker_feature_group.offline_store.arn
      },
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams",
          "logs:DescribeLogGroups",
          "logs:GetLogEvents"
        ]
        Effect   = "Allow"
        Resource = ["arn:aws:logs:*:*:*"]
      },
      {
        Action = [
          # AWS does not provide built in MLFlow policy. For now, granting full access.
          "sagemaker-mlflow:*"
        ]
        Effect   = "Allow"
        Resource = aws_sagemaker_mlflow_tracking_server.ds-tracking-server.arn
      },
      {
        Action = [
          "sagemaker:CreateMlflowTrackingServer",
          "sagemaker:ListMlflowTrackingServers",
          "sagemaker:UpdateMlflowTrackingServer",
          "sagemaker:DeleteMlflowTrackingServer",
          "sagemaker:StartMlflowTrackingServer",
          "sagemaker:StopMlflowTrackingServer",
          "sagemaker:CreatePresignedMlflowTrackingServerUrl"
        ]
        Effect   = "Allow"
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetBucketAcl",
          "s3:PutObject",
          "s3:GetObject",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.training-data-23421.arn,
          "${aws_s3_bucket.training-data-23421.arn}/*",
          aws_s3_bucket.mlflow-artifacts-98761.arn,
          "${aws_s3_bucket.mlflow-artifacts-98761.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role" "sagemaker-feature-store-role" {
  name = "sagemaker-feature-store-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "sagemaker.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "sagemaker-full-access" {
  role       = aws_iam_role.sagemaker-feature-store-role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess"
}

resource "aws_iam_role_policy" "feature-store-s3-access" {
  name = "feature-store-s3-access"
  role = aws_iam_role.sagemaker-feature-store-role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetBucketAcl",
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.feature-store-786q23.arn,
          "${aws_s3_bucket.feature-store-786q23.arn}/*",
          aws_s3_bucket.training-data-23421.arn,
          "${aws_s3_bucket.training-data-23421.arn}/*"
        ]
      }
    ]
  })
}