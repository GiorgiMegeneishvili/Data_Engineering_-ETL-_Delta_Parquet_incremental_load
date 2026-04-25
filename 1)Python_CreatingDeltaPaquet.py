from pyspark.sql import SparkSession

# =========================
# Creating session of Pyspark
# =========================
spark = SparkSession.builder \
    .appName("MSSQL_to_Delta") \
    .config("spark.jars", "/home/gm/Desktop/mssql-jdbc-12.6.1.jre11.jar") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# =========================
# Connect to a Database
# =========================
# MSSQL connection
DB_SQL_DATABASE = "demodb"
DB_SQL_USER = "sa"
DB_SQL_PASSWORD = ""  ###### add YourPasswor
DB_SQL_DRIVER = "com.microsoft.sqlserver.jdbc." ###### add Driver

jdbc_url = f"jdbc:sqlserver://localhost:1433;database={DB_SQL_DATABASE};encrypt=false;"


mssql_properties = {
    "user": DB_SQL_USER,
    "password": DB_SQL_PASSWORD,
    "driver": DB_SQL_DRIVER
}

# =========================
# READ MSSQL TABLE
# =========================

df = spark.read.jdbc(
    url=jdbc_url, table="dbo.person",properties=mssql_properties
)

df.show()

# =========================
# WRITE DELTA ON A DESKTOP
# =========================

df.write.format("delta").mode("overwrite").save("/home/gm/Desktop/person_delta12")
