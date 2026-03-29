export SPARK_MASTER_OPTS="-Dspark.authenticate=false -Dspark.ui.authenticate=false"
export SPARK_WORKER_OPTS="-Dspark.authenticate=false -Dspark.ui.authenticate=false"

# Now start them
/usr/lib/spark/sbin/start-master.sh
/usr/lib/spark/sbin/start-worker.sh spark://localhost:7077

# 1. Locate the py4j-src.zip (the version number might vary slightly)
PY4J_ZIP=$(ls /usr/lib/spark/python/lib/py4j-*-src.zip)

# 2. Add Spark's python and py4j to your PYTHONPATH
export PYTHONPATH=$PYTHONPATH:/usr/lib/spark/python:$PY4J_ZIP

# 3. (Optional) Ensure the container uses the correct Python binary
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3

pip3 install jupyterlab
pip3 install dagster_pipes

jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root