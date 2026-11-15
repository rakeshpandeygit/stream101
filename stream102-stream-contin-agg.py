from lib import utils
from pyspark.sql.functions import *

print("Creating Spark Session...")
spark = utils.get_session('local')

print("Reading JSON data...")
order_schema = "order_id long, customer_id integer, customer_fname string, customer_lname string,city string,state string, pincode long,line_items array<struct<order_item_id long,order_item_product_id long, order_item_quantity long, order_item_subtotal double, order_item_product_price double>>"
json_df = spark.readStream.format('json').schema(order_schema).option('multiline','false').load('data/streamJson')

print("Applying transformations...")
json_df.createOrReplaceTempView("orders")
table_df = spark.sql("select order_id, customer_id, customer_fname, customer_lname ,city, state, pincode ,explode(line_items) as line_items from orders")
table_df_1 = table_df.select("order_id", "customer_id", "customer_fname", "customer_lname", "city", "state", "pincode", "line_items.order_item_id", "line_items.order_item_product_id", "line_items.order_item_quantity", "line_items.order_item_subtotal", "line_items.order_item_product_price")
agg_df = table_df_1.groupBy("customer_id").agg(approx_count_distinct("order_id").alias("orders_placed"), count("order_item_id").alias("products_purchased"), sum("order_item_product_price").alias("amount_amount"))

print("Streaming Results to CSV...")
query = agg_df.writeStream.format("console").outputMode('complete').option("checkpointLocation", "data/checkpoint").start()
query.awaitTermination()

spark.stop()