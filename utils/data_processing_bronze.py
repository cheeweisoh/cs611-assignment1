import logging
import os

logger = logging.getLogger(__name__)


def process_bronze_table(table_name, table_config, source_path, bronze_path, spark):
    try:
        source_file_path = os.path.join(source_path, f"{table_name}.csv")
        df = spark.read.csv(source_file_path, header=True, inferSchema=True)

        partition_col = table_config["partition_col"]
        output_path = os.path.join(bronze_path, table_name)

        df.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy(partition_col).save(output_path)
        logger.info(f"Processed file {table_name}. Bronze table written to {output_path}")

    except Exception as e:
        logger.error(e)
