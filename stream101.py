
from lib import utils

spark = utils.get_session('local')

order_schema = "order_id integer, order_date timestamp, order_customer_id integer, order_status string"

json_df=spark.readStream.format('json').schema(order_schema).option('multiline','false').load('data/streamJson')

json_df.createOrReplaceTempView("orders")

completed_ord_df = spark.sql("select * from orders where order_status = 'COMPLETE'")

query = ( 
    completed_ord_df.writeStream.format('console').
    outputMode('append').
    option("checkpointLocation", "data/checkpoint").
    start()
)

query.awaitTermination()

