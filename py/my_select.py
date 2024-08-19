from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, size,explode_outer

spark = SparkSession.builder.appName("SelectData").getOrCreate()

df1 = spark.read.parquet(f"/home/centa/data/movies/parsing/")
df1.createOrReplaceTempView("parsing")


df2 = spark.sql(f"""
SELECT director,count('moviecCd') as cntmovie 
FROM parsing 
GROUP BY director
""")

df2.show()

df3 = spark.sql(f"""
SELECT companys, count('movieCd') as cntmovie 
FROM parsing 
GROUP BY companys
""")

df3.show()

spark.stop()
