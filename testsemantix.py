from pyspark.sql.types import *
from pyspark.sql.functions import *

df = spark.read.options(delimiter=" ", quote="").csv("/user/root/nasa")

df = df.select(df[0].alias('hostname'), to_timestamp(substring(concat(df[3], lit(" "), df[4]), 2, 26), "dd/MMM/yyyy:HH:mm:ss Z").alias('time'), df[6].alias('url'), df[8].cast(IntegerType()).alias('return_code'), df[9].cast(IntegerType()).alias('bytes'))

df.show(truncate=False)

#Número de hosts únicos.
df.select(df["hostname"]).distinct().count()
#O total de erros 404.
df.where(df["return_code"] == 404).count()

df_filter = df.where(df["return_code"] == 404)

df_group = df_filter.groupBy(df_filter["url"]).count()

df_group = df_group.orderBy(df_group["count"].desc()).limit(5)

#Os 5 URLs que mais causaram erro 404.
df_group.show()

df_error_per_day = df_filter.groupBy(date_format(df_filter["time"], "yyyy/mm/dd").alias("day")).count()

#Quantidade de erros 404 por dia.
df_error_per_day.show()

#O total de bytes retornados.
df.select(df["bytes"]).groupBy().sum().show()
