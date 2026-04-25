from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable

# =========================
# JDBC Driver
# =========================
jdbc_jar = "/home/gm/Desktop/drivers/sqljdbc_13." ### Add Your jdbc_jar

# =========================
# Spark Session (Delta Created)
# =========================
spark = SparkSession.builder \
    .appName("MSSQL_to_Delta_Incremental") \
    .config("spark.jars", jdbc_jar) \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# =========================
# MSSQL CONNECTION
# =========================
DB_SQL_DATABASE = "demoDB"
DB_SQL_USER = "sa"
DB_SQL_PASSWORD = "" # add Your PassWord
DB_SQL_DRIVER = "com.microsoft.sqlserver.jdbc." # add Your Driver

ms_jdbc_url = f"jdbc:sqlserver://localhost:1433;database={DB_SQL_DATABASE};encrypt=false;"
mssql_properties = {
    "user": DB_SQL_USER,
    "password": DB_SQL_PASSWORD,
    "driver": DB_SQL_DRIVER
}

delta_path = "/home/gm/Desktop/person_delta12"

# =========================
# LAST 7 DAYS DATA FROM MSSQL
# =========================
query = "(SELECT * FROM dbo.person WHERE CAST(ModifiedDate AS DATE) >= CAST(DATEADD(DAY, -7, SYSDATETIME()) AS DATE)) as src"

print("🔍 Loading data from the last 7 days from MSSQL")
source_df = spark.read.jdbc(url=ms_jdbc_url, table=query, properties=mssql_properties)

# =========================
# CREATE DELTA TABLE IF NOT EXISTS
# =========================
import os

if not os.path.exists(delta_path) or len(os.listdir(delta_path)) == 0:
    print("📂 Delta table Not exsits, and We are Creating")
    source_df.write.format("delta").mode("overwrite").save(delta_path)
else:
    # =========================
    # MERGE NEW DATA
    # =========================
    print("🔄 Delta table exists, Making incremental merge")
    delta_table = DeltaTable.forPath(spark, delta_path)

    delta_table.alias("t").merge(
        source_df.alias("s"),
        "t.BusinessEntityID = s.BusinessEntityID"
    ).whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

# =========================
# VERIFY
# =========================
delta_df = spark.read.format("delta").load(delta_path)
print("✅ Final records:", delta_df.count())
delta_df.show(5)
