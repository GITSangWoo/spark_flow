from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import explode, col, size,explode_outer

year=sys.argv[1]

spark = SparkSession.builder.appName("MovieDynamic").getOrCreate()
jdf = spark.read.option("multiline","true").json(f'/home/centa/data/movies/year={year}/data.json')

jdf2=jdf.withColumn("director", explode_outer("directors"))
df_j=jdf2.withColumn("companys",explode_outer("companys"))

df_j.write.mode('append').parquet("/home/centa/data/movies/parsing/")

spark.stop()
