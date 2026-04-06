python3 -m venv pyspark_venvsource
source pyspark_venvsource/bin/activate

pip3 install --upgrade pip

pip3 install venv-pack
echo "Installed venv-pack"
pip3 install dagster-pipes
pip3 install numpy
echo "Installed numpy and dagster-pipes"
pip3 install mlflow
pip3 install sagemaker-mlflow
echo "Installed mlflow"


venv-pack -f -o pyspark_venv.tar.gz
aws s3 cp pyspark_venv.tar.gz s3://training-data-23421/transform/
echo "Uploaded to S3"

rm -fr pyspark_venvsource