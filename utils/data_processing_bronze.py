import logging
import os

from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def process_bronze_table(table_name, table_config, source_path, bronze_path, spark, snapshot_date_str=None):
    try:
        source_file_path = os.path.join(source_path, f"{table_name}.csv")
        df = spark.read.csv(source_file_path, header=True, inferSchema=True)

        if snapshot_date_str is not None:
            df = df.filter(F.col("snapshot_date") == snapshot_date_str)

        partition_col = table_config["partition_col"]
        output_path = os.path.join(bronze_path, table_config["table_dir"])

        writer = df.write.format("delta").option("mergeSchema", "true").partitionBy(partition_col)
        if snapshot_date_str is not None:
            writer.mode("overwrite").option("replaceWhere", f"snapshot_date = '{snapshot_date_str}'").save(output_path)
        else:
            writer.mode("append").save(output_path)

        logger.info(f"Processed {table_name} for {snapshot_date_str}. Bronze table written to {output_path}")

    except Exception as e:
        logger.error(e)
