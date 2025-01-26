df = spark.range(12500000)   ##Generate numbers from 0 to 12,490,000

#Store it as CSV
df.coalesce(1).write.option("Header",True).csv("/data/csv_12M_data/")

#Store it as Parquet
df.coalesce(1).write.parquet("/data/parq_12M_data/")

df_csv = spark.read.option("Header",True).csv("/data/csv_12M_data/")

#Cache csv
df_csv.cache().count()
#Check SPARK UI for the space taken in memory

df_csv.unpersist()

df_parq = spark.read.parquet("/data/parq_12M_data/")

#Caching Parquet
df_parq.cache().count()
#Check SPARK UI for the space taken in memory

df_parq.unpersist()

## Cast the datatype to long in CSV
import pyspark.sql.functions as F
df_csv_long_dtype = df_csv.withColumn("id",F.col("id").cast("long"))
df_csv_long_dtype.printSchema()
df_csv_long_dtype.cache().count()
#Check SPARK UI for the space taken in memory
