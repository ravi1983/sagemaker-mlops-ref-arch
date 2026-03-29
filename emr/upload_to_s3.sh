#!/bin/bash

# Upload transform.py
aws s3 sync ./src/transform/ s3://training-data-23421/transform/ --exact-timestamps

# Upload test data
aws s3 cp ./data/Customer_Data.csv s3://training-data-23421/test-data/