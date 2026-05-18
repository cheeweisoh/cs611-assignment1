import logging
import os

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, FloatType, IntegerType, StringType

logger = logging.getLogger(__name__)

_TYPE_MAP = {
    "string": StringType(),
    "integer": IntegerType(),
    "float": FloatType(),
    "date": DateType(),
}


def _clean_features_attributes(df):
    # Name: strip leading/trailing whitespace
    df = df.withColumn("Name", F.trim(F.col("Name")))

    # Age: remove underscores, then set values outside plausible range [0, 100] to null
    df = df.withColumn("Age", F.regexp_replace(F.col("Age").cast("string"), r"_+", ""))
    df = df.withColumn("Age", F.when((F.col("Age") < 0) | (F.col("Age") > 100), None).otherwise(F.col("Age")))

    # SSN: null out values that don't match the expected format
    df = df.withColumn("SSN", F.when(F.col("SSN").rlike(r"^\d{3}-\d{2}-\d{4}$"), F.col("SSN")).otherwise(None))

    # Occupation: set placeholder values consisting of underscores to null
    df = df.withColumn("Occupation", F.when(F.col("Occupation").rlike(r"^_+$"), None).otherwise(F.col("Occupation")))

    return df


def _clean_features_financials(df):
    # Annual_Income: remove underscores
    df = df.withColumn("Annual_Income", F.regexp_replace(F.col("Annual_Income").cast("string"), r"_+", ""))

    # Num_Bank_Accounts: set values outside plausible range [0, 20] to null
    df = df.withColumn("Num_Bank_Accounts", F.when((F.col("Num_Bank_Accounts") < 0) | (F.col("Num_Bank_Accounts") > 20), None).otherwise(F.col("Num_Bank_Accounts")))

    # Num_Credit_Card: set values outside plausible range [0, 20] to null
    df = df.withColumn("Num_Credit_Card", F.when((F.col("Num_Credit_Card") < 0) | (F.col("Num_Credit_Card") > 20), None).otherwise(F.col("Num_Credit_Card")))

    # Interest_Rate: set values outside plausible range [0, 100] to null
    df = df.withColumn("Interest_Rate", F.when((F.col("Interest_Rate") < 0) | (F.col("Interest_Rate") > 100), None).otherwise(F.col("Interest_Rate")))

    # Num_of_Loan: remove underscores, then set values outside plausible range [0, 20] to null
    df = df.withColumn("Num_of_Loan", F.regexp_replace(F.col("Num_of_Loan").cast("string"), r"_+", ""))
    df = df.withColumn("Num_of_Loan", F.when((F.col("Num_of_Loan") < 0) | (F.col("Num_of_Loan") > 20), None).otherwise(F.col("Num_of_Loan")))

    # Delay_from_due_date: set negative values to null
    df = df.withColumn("Delay_from_due_date", F.when(F.col("Delay_from_due_date") < 0, None).otherwise(F.col("Delay_from_due_date")))

    # Num_of_Delayed_Payment: remove underscores, then set values outside plausible range [0, 365] to null
    df = df.withColumn("Num_of_Delayed_Payment", F.regexp_replace(F.col("Num_of_Delayed_Payment").cast("string"), r"_+", ""))
    df = df.withColumn("Num_of_Delayed_Payment", F.when((F.col("Num_of_Delayed_Payment") < 0) | (F.col("Num_of_Delayed_Payment") > 365), None).otherwise(F.col("Num_of_Delayed_Payment")))

    # Changed_Credit_Limit: set placeholder values consisting of underscores to null
    df = df.withColumn("Changed_Credit_Limit", F.when(F.col("Changed_Credit_Limit").rlike(r"^_+$"), None).otherwise(F.col("Changed_Credit_Limit")))

    # Credit_Mix: set placeholder values consisting of underscores to null
    df = df.withColumn("Credit_Mix", F.when(F.col("Credit_Mix").rlike(r"^_+$"), None).otherwise(F.col("Credit_Mix")))

    # Outstanding_Debt: remove underscores
    df = df.withColumn("Outstanding_Debt", F.regexp_replace(F.col("Outstanding_Debt").cast("string"), r"_+", ""))

    # Total_EMI_per_month: set zero values to null
    df = df.withColumn("Total_EMI_per_month", F.when(F.col("Total_EMI_per_month") == 0, None).otherwise(F.col("Total_EMI_per_month")))

    # Amount_invested_monthly: set values wrapped in double underscores (possible placeholder) to null and remove underscores
    df = df.withColumn("Amount_invested_monthly", F.when(F.col("Amount_invested_monthly").rlike(r"^__-?[\d.]+__$"), None).otherwise(F.regexp_replace(F.col("Amount_invested_monthly"), r"_+$", "")))

    # Payment_Behaviour: set values that don't match the expected pattern to null
    df = df.withColumn("Payment_Behaviour", F.when(F.col("Payment_Behaviour").rlike(r"^(Low|High)_spent_(Small|Medium|Large)_value_payments$"), F.col("Payment_Behaviour")).otherwise(None))

    # Monthly_Balance: set values wrapped in double underscores (possible placeholder) to null and remove underscores
    df = df.withColumn("Monthly_Balance", F.when(F.col("Monthly_Balance").rlike(r"^__-?[\d.]+__$"), None).otherwise(F.regexp_replace(F.col("Monthly_Balance"), r"_+$", "")))

    return df


_CLEANERS = {
    "features_attributes": _clean_features_attributes,
    "features_financials": _clean_features_financials,
}


def process_silver_table(table_name, table_config, bronze_path, silver_path, spark, snapshot_date_str=None):
    try:
        input_path = os.path.join(bronze_path, table_name)
        df = spark.read.format("delta").load(input_path)

        if snapshot_date_str is not None:
            df = df.filter(F.col("snapshot_date") == snapshot_date_str)

        df = _CLEANERS.get(table_name, lambda df: df)(df)

        columns = table_config["columns"]

        for col_name, col_config in columns.items():
            spark_type = _TYPE_MAP[col_config["type"]]
            df = df.withColumn(col_name, F.col(col_name).cast(spark_type))

        primary_keys = [col for col, cfg in columns.items() if cfg.get("primary_key", False)]
        before_count = df.count()
        df = df.dropDuplicates(primary_keys)
        after_count = df.count()
        logger.info(f"{table_name}: dropped {before_count - after_count} duplicate(s) on {primary_keys}")

        partition_col = table_config["partition_col"]
        output_path = os.path.join(silver_path, table_name)

        writer = df.write.format("delta").option("overwriteSchema", "true").partitionBy(partition_col)
        if snapshot_date_str is not None:
            writer.mode("overwrite").option("replaceWhere", f"snapshot_date = '{snapshot_date_str}'").save(output_path)
        else:
            writer.mode("overwrite").save(output_path)

        logger.info(f"Processed silver table {table_name} for {snapshot_date_str}. Written to {output_path}")

    except Exception as e:
        logger.error(e)
