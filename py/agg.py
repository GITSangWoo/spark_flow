from pyspark.sql import SparkSession
import sys

LOAD_DT=sys.argv[1]
spark = SparkSession.builder.appName("MovieiAgg").getOrCreate()

df1 = spark.read.parquet(f"/home/centa/data/movie/hive/load_dt={LOAD_DT}")
df1.createOrReplaceTempView("one_day")

df_renamed = df1.withColumnRenamed("coalesce(audiCnt, audiCnt)", "audiCnt").withColumnRenamed("coalesce(salesAmt, salesAmt)", "salesAmt").withColumnRenamed("coalesce(showCnt, showCnt)", "showCnt")
df_renamed.createOrReplaceTempView("one_day")

df2 = spark.sql(f"""
SELECT
    salesAmt,
    audiCnt,
    showCnt,
    multiMovieYn,
    repNationCd,
    '{LOAD_DT}' AS load_dt
FROM one_day

""")

df3 = spark.sql(f"""
SELECT
    SUM(salesAmt) AS total_sales,
    SUM(audiCnt) AS total_audiCnt,
    SUM(showCnt) AS total_showCnt,
    multiMovieYn,
    '{LOAD_DT}' AS load_dt
FROM one_day
GROUP BY multiMovieYn
""")

df3.write.mode('append').partitionBy("load_dt").parquet("/home/centa/data/movie/sum-multi/")

df4 = spark.sql(f"""
SELECT
    SUM(salesAmt) AS total_sales,
    SUM(audiCnt) AS total_audiCnt,
    SUM(showCnt) AS total_showCnt,
    repNationCd,
    '{LOAD_DT}' AS load_dt
FROM one_day
GROUP BY repNationCd
""")

df4.write.mode('append').partitionBy("load_dt").parquet("/home/centa/data/movie/sum-nation/")


spark.stop()
