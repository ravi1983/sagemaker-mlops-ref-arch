docker run -it \
  --entrypoint /bin/bash \
  -u root \
  -p 7077:7077 \
  -p 8080:8080 \
  -p 4040:4040 \
  -p 8888:8888 \
  -e PYTHONPATH="/usr/lib/spark/python:/usr/lib/spark/python/lib/py4j-*-src.zip" \
  -v "/home/ravi/projects/public_repo/sagemaker-mlops-ref-arch/emr/data:/home/hadoop/test_data" \
  public.ecr.aws/emr-serverless/spark/emr-7.6.0:latest

# aws configure export-credentials --format env
# rm -rf ~/.aws/sso/cache
# rm -rf ~/.aws/cli/cache