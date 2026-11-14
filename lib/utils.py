from pyspark.sql import SparkSession
import os
from pathlib import Path

def get_session(env):
    if env == "local":
        local_app_data = os.environ.get("LOCALAPPDATA")
        local_fs_jar = (
                    Path(local_app_data)
                    / "Programs"
                    / "hadoop-bare-naked-local-fs"
                    / "hadoop-bare-naked-local-fs-0.1.0.jar"
                )
        builder = SparkSession.builder.appName("ETL")
        builder = (
                    builder.config("spark.driver.extraClassPath", str(local_fs_jar))
                    .config(
                        "spark.hadoop.fs.file.impl",
                        "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem",
                    )
                    .config(
                        "spark.sql.streaming.checkpointFileManagerClass",
                        "org.apache.spark.sql.execution.streaming.checkpointing.FileSystemBasedCheckpointFileManager",
                    )
        )



        spark = builder.appName("ETL").getOrCreate()
    else:
        spark = SparkSession.builder.appName("ETL").getOrCreate()
    return spark