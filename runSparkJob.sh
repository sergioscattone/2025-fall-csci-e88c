rm -f spark/target/scala-2.13/SparkJob.jar;
sbt compile && sbt spark/assembly;
cp spark/target/scala-2.13/SparkJob.jar docker/apps/spark;

# Rebuild dashboard to pick up any app.py (dashboard) changes
echo "Rebuilding dashboard container...";
docker build -t 2025-fall-csci-e88c_evidence-dashboard ./docker/apps/spark/dashboard;

# Ensure any stale dashboard container is removed to avoid name conflicts
echo "Cleaning up any existing evidence-dashboard container...";
docker rm -f evidence-dashboard >/dev/null 2>&1 || true

docker-compose -f docker-compose-spark.yml up -d;
sleep 5;
docker exec spark-master /opt/spark/bin/spark-submit --class org.cscie88c.spark.SparkJob --master local[*] --executor-memory 512m --executor-cores 1 --driver-memory 512m /opt/spark-apps/SparkJob.jar /opt/spark-data/yellow_tripdata_2025-01.parquet /opt/spark-data/taxi_zone_lookup.csv /opt/spark-data/output/