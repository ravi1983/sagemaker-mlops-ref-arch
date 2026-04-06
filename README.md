# End-to-End MLOps Pipeline on AWS with Dagster, EMR Serverless, Athena, SageMaker, and Managed MLflow

This repository demonstrates an end-to-end MLOps workflow for **continuous training** and **continuous inference** on AWS.

It uses **Dagster assets** for orchestration and lineage, **Dagster Pipes** for visibility into external compute, **EMR Serverless** for scalable data processing and inference, **Athena** for feature enrichment queries, **SageMaker** for training, and **AWS Managed MLflow** for experiment tracking and model registry.

---

## Architecture

### Continuous Training
![Dagster Asset Graph](./images/CT.png)

<BR>
<BR>


### Inference
![Dagster Asset Graph](./images/Inference.png)

<BR>
<BR>

### Dagster Job
![Dagster Asset Graph](./images/Global_Asset_Lineage.svg)

---

## Overview

This project implements two core workflows:

1. **Continuous Training**
2. **Continuous Inference**

Each step materializes important metadata so the pipeline is observable, traceable, and easier to operate.

---

## Continuous Training Flow

### 1. Training data asset

A Dagster asset represents the raw training data input.

### 2. Feature engineering with EMR Serverless

A Dagster asset kicks off an **EMR Serverless** job using **Dagster Pipes**.

This job:

- imputes missing values
- encodes categorical features
- writes features to an **offline feature store** in S3

**Materialized outputs:**

- EMR job ID
- offline feature store S3 path

### 3. Build enriched training dataset with Athena

Because the offline feature store is an **append log**, the pipeline creates an **Athena query** to retrieve the **latest value for each feature** and writes the result back to S3.

This becomes the enriched training dataset used by the training job.

**Materialized outputs:**

- Athena query
- enriched training data S3 location

### 4. Train and evaluate model with SageMaker

The pipeline launches a **SageMaker training job** that trains a **classification model** using `scikit-learn` and evaluates it.

Training metadata is logged into **AWS Managed MLflow**, including:

- experiment
- run metadata
- model artifacts
- evaluation metrics

**Materialized outputs:**

- MLflow run ID
- MLflow artifact path

### 5. Conditional model registration and promotion

The pipeline reads model metrics from MLflow and applies a promotion rule.

If the model accuracy meets the configured threshold, the pipeline:

- registers the model
- promotes it to **champion**

**Materialized outputs:**

- registered model name
- registered model version

---

## Continuous Inference Flow

### 1. Raw inference data asset

A Dagster asset represents the raw inference input data.

### 2. Enrich inference data with offline feature store

The inference data is enriched by joining it with the offline feature store so that **all required features** are available before prediction.

This step uses **Athena** and writes the enriched inference dataset to S3.

**Materialized outputs:**

- Athena query
- enriched inference data S3 location

### 3. Batch inference with EMR Serverless

A Dagster asset launches an **EMR Serverless** job for inference using **Dagster Pipes**.

This job:

- loads the promoted model from the **MLflow Model Registry**
- uses an **MLflow UDF** for batch scoring
- writes predictions to S3

**Materialized outputs:**

- prediction results S3 location

---

## Design Choices

### Why Dagster?

I chose **Dagster** because it provides:

- native asset-based orchestration
- built-in lineage
- materialization metadata for every step
- a clean way to model ML systems as data products

### Why Dagster Pipes?

I used **Dagster Pipes** wherever possible so external compute systems such as EMR Serverless remain visible from Dagster.

This improves:

- observability
- debugging
- runtime metadata tracking
- integration between orchestration and execution layers

### Why EMR Serverless for inference?

I chose **EMR Serverless** for inference because it is more scalable for distributed batch workloads.

This is especially useful when:

- inference volume is large
- data is already in S3
- Spark-based processing is part of the scoring pipeline

### Why Athena for feature enrichment?

Because the offline feature store is implemented as an **append log**, I need a query layer to compute the latest valid feature values for downstream training and inference.

Athena provides a simple and serverless way to do that over S3-backed data.

---

## Production Readiness Gaps / Next Steps

To make this repository production-ready, the next improvements are:

### 1. CI/CD

Add CI/CD pipelines for EMR transform/inference code, training code and dagster dags. Add robust champion/challenger model promotion & rollback.

### 2. IAM hardening

Lock down IAM permissions using the **principle of least privilege**.

### 4. Testing strategy

Add unit/integration tests for transformations and pipeline components.

### 5. Online feature store

Add online feature and support for real time inference.

### 6. Event triggers

Automatically trigger training and inference asset creation and subsequent steps when new data arrives in s3.

---

## Materialized Metadata Across the Pipeline

One of the key goals of this project is to materialize useful operational metadata at each stage of the workflow.

Examples include:

- EMR job IDs
- Athena queries
- S3 output paths
- MLflow run IDs
- artifact locations
- registered model names and versions

This makes the pipeline easier to:

- debug
- audit
- observe
- explain to stakeholders

---

## Tech Stack

- **Dagster**
- **Dagster Pipes**
- **Amazon EMR Serverless**
- **Amazon Athena**
- **Amazon S3**
- **Amazon SageMaker**
- **AWS Managed MLflow**
- **scikit-learn**
- **PySpark**

---

## What This Repo Demonstrates

This project is meant to show how to build a practical ML platform workflow that combines:

- asset-based orchestration
- external compute orchestration with visibility
- offline feature enrichment
- model training and evaluation
- experiment tracking
- model registration and promotion
- scalable batch inference

---

## Disclaimer

This project is a reference implementation intended to demonstrate architecture and workflow design choices for production-style MLOps systems on AWS.