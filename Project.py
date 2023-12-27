from pyspark.sql import SparkSession
import getpass
username = getpass.getuser()
spark = SparkSession. \
    builder. \
    config('spark.shuffle.useOldFetchProtocol', 'true'). \
    config("spark.sql.warehouse.dir", "/user/{username}/warehouse"). \
    enableHiveSupport(). \
    master('yarn'). \
    getOrCreate()

from pyspark.sql.types import *

users_schema = StructType([
    StructField("user_id", IntegerType(), nullable=False)
    , StructField("user_first_name", StringType(), nullable = False)
    , StructField("user_last_name", StringType(), nullable = False)
    , StructField("user_email", StringType(), nullable = False)
    , StructField("user_gender", StringType(), nullable = False)
    , StructField("user_phone_numbers", ArrayType(StringType()), nullable = True)
    , StructField("user_address", StructType([
        StructField("street", StringType(), nullable = False)
        , StructField("city", StringType(), nullable = False)
        , StructField("state", StringType(), nullable = False)
        , StructField("postal_code", StringType(), nullable = False)
    ]), nullable = False)
])

users_df = spark.read \
.format("json") \
.schema(users_schema) \
.load("/public/sms/users")

print(users_df.rdd.getNumPartitions())

print(users_df.count())

print(users_df.filter("user_address.state = 'New York'").count())

from pyspark.sql.functions import *
num_of_postal_codes_df = users_df.groupBy("user_address.state").agg(countDistinct("user_address.postal_code").alias('num_of_postal_codes'))

num_of_postal_codes_df.orderBy(desc("num_of_postal_codes")).show(1)

num_of_max_users_df = users_df.filter("user_address.city is not null").groupBy("user_address.city").agg(countDistinct("user_id").alias("num_of_users"))

num_of_max_users_df.orderBy(desc("num_of_users")).show(1)

bizjournal_users_df = users_df.select('user_id').where("user_email like '%bizjournals.com'")

print(bizjournal_users_df.distinct().count())

print(users_df.select("user_id").where("size(user_phone_numbers) = 4").distinct().count())

print(users_df.select("user_id").where("size(user_phone_numbers) = -1").distinct().count())

users_df.write \
.format("parquet") \
.mode("overwrite") \
.option("path", "/user/itv009538/week9/project") \
.save()