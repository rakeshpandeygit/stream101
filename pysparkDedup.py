import os
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *




def main():
    builder = SparkSession.builder.appName("ETL")
    if os.name == "nt":
        local_app_data = os.environ.get("LOCALAPPDATA")
        if local_app_data is None:
            raise RuntimeError("LOCALAPPDATA is required for the Windows Hadoop filesystem setup.")
        local_fs_jar = (
            Path(local_app_data)
            / "Programs"
            / "hadoop-bare-naked-local-fs"
            / "hadoop-bare-naked-local-fs-0.1.0.jar"
        )
        if not local_fs_jar.is_file():
            raise FileNotFoundError(
                f"Required Windows Hadoop filesystem library not found: {local_fs_jar}"
            )
        builder = (
            builder.config("spark.driver.extraClassPath", str(local_fs_jar))
            .config(
                "spark.hadoop.fs.file.impl",
                "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem",
            )
        )

    spark = builder.getOrCreate()
    try:
        customer_df = spark.read.csv("data/customer.csv",header=True,schema="customer_id INT, status STRING, updated_at STRING")
        input_df = customer_df.withColumn("updated_at",to_timestamp("updated_at",'yyyy-MM-dd HH:mm'))

        transform_df = input_df.withColumn("rnum",row_number().over(Window.partitionBy("customer_id").orderBy(col("updated_at").desc()))).filter(col("rnum")==1).drop("rnum")


        
        print(input_df.show())
        print(input_df.printSchema())
        print(transform_df.show())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()


max_df = read_df.groupBy("customer_id").agg(max("updated_at").alias("max_updated_at"))
read_df.join(max_df, (read_df.customer_id == max_df.customer_id) & (read_df.updated_at == max_df.max_updated_at), "inner").drop(max_df.customer_id).drop(max_df.max_updated_at)