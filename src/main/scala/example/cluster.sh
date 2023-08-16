gcloud dataproc clusters create cluster-7fa3 --enable-component-gateway --region us-central1 --zone us-central1-c --master-machine-type n2-standard-4 --master-boot-disk-size 500 --num-workers 2 --worker-machine-type n2-standard-4 --worker-boot-disk-size 500 --image-version 2.1-debian11 --properties spark:spark.jars.packages=io.delta:delta-core_2.12:2.3.0 --optional-components JUPYTER --project cf-data-analytics


spark:spark.jars.packages
io.delta:delta-core_2.12:2.3.0

spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")


gcloud  functions deploy my_function --region=us-central1 --runtime=python39 --gen2 --entry-point=hello_gcs --trigger-bucket=cf-json-ingest --source=gs://cf-cloud-function-source/source.zip