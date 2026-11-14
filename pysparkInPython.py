import os
from pathlib import Path
from pyspark.sql import SparkSession
from lib import utils

def transform(df):
    return df.dropDuplicates()


def main():
    print("Creating spark session...")
    spark = utils.get_session('local')

    print("Reading JSON file...")
    try:
        json_read_df = spark.read.option("multiLine", True).json("data/order.json")

        print("Transforming data...")
        (
            transform(json_read_df)
            .write.format("json")
            .mode("overwrite")
            .option("path", "data/order_transformed")
            .save()
        )
    finally:
        print("Stopping spark session...")
        spark.stop()


if __name__ == "__main__":
    main()

