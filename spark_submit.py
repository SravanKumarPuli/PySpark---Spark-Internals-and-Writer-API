from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
   	config('spark.shuffle.useOldFetchProtocol', 'true'). \
    config("spark.sql.warehouse.dir", "/user/itv005857/warehouse"). \
    enableHiveSupport(). \
    master('yarn'). \
    getOrCreate()

orders_schema = "order_id long, order_date string, cust_id long, order_status string"

orders_df = spark.read \
.format("csv") \
.schema(orders_schema) \
.load("/public/trendytech/orders/orders_1gb.csv")

print(orders_df.rdd.getNumPartitions())

orders_df.createOrReplaceTempView("orders")

spark.sql("select order_status, count(*) as total from orders group by order_status").show()

spark.stop()