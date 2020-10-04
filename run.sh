#!/usr/bin/env bash

INPUT_PATH=${INPUT_PATH}
OUTPUT_PATH=${OUTPUT_PATH}

SPARK_EXECUTOR_INSTANCES=4
SPARK_EXECUTOR_CORES=2
SPARK_EXECUTOR_MEMORY=2G
SPARK_DRIVER_CORES=2
SPARK_DRIVER_MEMORY=1G
SPARK_WRITE_PARTITIONS=1

SPARK_USER=${SPARK_USER}
SPARK_HOST=${SPARK_HOST}
SUBMIT_FOLDER=${SUBMIT_FOLDER}


echo "========================================"
echo "source folder: $INPUT_PATH"
echo "destination folder: $OUTPUT_PATH"
echo "number of spark executors: $SPARK_EXECUTOR_INSTANCES"

# submit job
ssh ${SPARK_USER}@${SPARK_HOST} "
    cd /opt/processing/$SUBMIT_FOLDER;

    spark-submit \
    --class com.glo.JobRunner \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.executor.instances=$SPARK_EXECUTOR_INSTANCES \
    --conf spark.executor.cores=$SPARK_EXECUTOR_CORES \
    --conf spark.executor.memory=$SPARK_EXECUTOR_MEMORY \
    --conf spark.driver.cores=$SPARK_DRIVER_CORES \
    --conf spark.driver.memory=$SPARK_DRIVER_MEMORY \
    --conf 'spark.executor.extraJavaOptions=-Dfile.encoding=UTF-8' \
    --conf 'spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8' \
    ./products-ratings.jar --input-path $INPUT_PATH --output-path $OUTPUT_PATH --partitions $SPARK_EXECUTOR_INSTANCES --write-partitions $SPARK_WRITE_PARTITIONS;
"