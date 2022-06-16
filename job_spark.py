from pyspark.sql.functions import mean, max, min, col, count
from pyspark.sql import SparkSession


spark = (
    SparkSession.builder.appName("ExerciseSpark")
    .getOrCreate()

)

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++=
# ler os dados do ENEM 2020
enem = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ";")
    .load("s3://datalake-dcs-igti-edc/raw-data/enem/")
)
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++=
# criar o párquet particionado pelo ANO
(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("NU_ANO")
    .save("s3://datalake-dcs-igti-edc/staging/enem")
)
