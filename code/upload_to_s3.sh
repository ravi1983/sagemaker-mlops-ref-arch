#!/bin/bash

# Upload transform.py
aws s3 sync ./src/transform/ s3://training-data-23421/transform/ --exact-timestamps
aws s3 sync ./src/inference/ s3://training-data-23421/inference/ --exact-timestamps

# Upload test/inference data
aws s3 cp ./data/Customer_Data.csv s3://training-data-23421/test-data/
aws s3 cp ./data/Inference.csv s3://training-data-23421/inference-data/