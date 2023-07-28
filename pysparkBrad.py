from pyspark.sql import SparkSession

# Iniciando a SparkSession
spark = SparkSession.builder.appName('pyspark-brad').getOrCreate()

df = spark.read.parquet('s3://csv-prod/voucher/', header=True)

df.show(truncate=False)

# Finalizando a SparkSession
spark.stop()
