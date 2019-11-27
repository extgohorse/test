from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

df = spark.read.options(delimiter=" ").csv("/user/root/nasa")

df = df.select(df[0].alias('hostname'), to_timestamp(substring(concat(df[3], lit(" "), df[4]), 2, 26), "dd/MMM/yyyy:HH:mm:ss Z").alias('time'), df[5].alias('request'), df[6].cast(IntegerType()).alias('return_code'), df[7].cast(IntegerType()).alias('bytes'))

df.show(truncate=False)

df.select(df["hostname"]).distinct().count()

df.where(df["return_code"] == 404).count()

df_filter = df.where(df["return_code"] == 404)

df_group = df_filter.groupBy(df_filter["request"]).count()

df_group.show()

df_rank = df_group.withColumn("rank", dense_rank().over(Window.orderBy(desc("count")))).where(col("rank") <= 5)

df_rank.show()

df_error_per_day = df_filter.groupBy(date_format(df_filter["time"], "yyyy/dd/dd").alias("day")).count()

df_error_per_day.show()

df.select(df["bytes"]).groupBy().sum().show()
