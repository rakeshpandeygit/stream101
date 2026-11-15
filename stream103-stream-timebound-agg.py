from lib import utils
from pyspark.sql.functions import *

print("Creating Spark Session...")
spark = utils.get_session('local')

print("Reading socket data...")
# order_schema = "order_id long, customer_id integer, customer_fname string, customer_lname string,city string,state string, pincode long,line_items array<struct<order_item_id long,order_item_product_id long, order_item_quantity long, order_item_subtotal double, order_item_product_price double>>"
# json_df = spark.readStream.format('json').schema(order_schema).option('multiline','false').load('data/streamJson')

json_string_df = spark.readStream.format('socket').option('host','localhost').option('port', 9986).load()

# json_string_df.printSchema()

print("Applying transformations...")

order_schema = "order_id long, order_date timestamp, order_customer_id long, order_status string, amount double"
read_df = json_string_df.select(from_json(col("value"), order_schema).alias("json_value"))
read_df_1 = read_df.select("json_value.order_id", "json_value.order_date", "json_value.order_customer_id", "json_value.order_status", "json_value.amount")

win_agg = read_df_1.groupBy(window("order_date","15 minutes")).agg(sum("amount").alias("total_invoice"))

final_df = win_agg.select("window.start", "window.end", "total_invoice")


print("Streaming Results to CSV...")

final_df.writeStream.format("console").outputMode('update').option("checkpointLocation", "data/checkpoint").start().awaitTermination()

spark.stop()