from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
from delta import configure_spark_with_delta_pip

# =========================
# SPARK SESSION WITH DELTA
# =========================

builder = SparkSession.builder \
    .appName("Read_Delta") \
    .config("spark.jars", "/home/gm/Desktop/mssql-jdbc-12.6.1.jre11.jar") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# =========================
# READ DELTA TABLE
# =========================

person = spark.read.format("delta").load("/home/gm/Desktop/person_delta12")

# =========================
# FILTER AND ORDER
# =========================

# 1️⃣ Filter records
# person.where(person.BusinessEntityID <= 7).show()

# 2️⃣ Last 10 inserted/updated records based on modification date
person.orderBy(desc("ModifiedDate")).show(25)

# 3️⃣ Total number of records
print(person.count())
