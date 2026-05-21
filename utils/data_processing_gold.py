import logging
import os
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, FloatType, IntegerType, StringType

logger = logging.getLogger(__name__)

_TYPE_MAP = {
    "string": StringType(),
    "integer": IntegerType(),
    "float": FloatType(),
    "date": DateType(),
}


def process_labels_gold_table(table_name, table_config, silver_path, gold_path, mob, dpd, spark, snapshot_date_str=None):
    try:
        input_path = os.path.join(silver_path, table_name)
        df = spark.read.format("delta").load(input_path)

        if snapshot_date_str is not None:
            df = df.filter(F.col("snapshot_date") == snapshot_date_str)

        # augment: add month on book
        df = df.withColumn("mob", F.col("installment_num").cast(IntegerType()))

        # augment: add days past due
        df = df.withColumn("installments_missed", F.coalesce(F.ceil(F.col("overdue_amt") / F.col("due_amt")).cast(IntegerType()), F.lit(0)))
        df = df.withColumn("first_missed_date", F.when(F.col("installments_missed") > 0, F.add_months(F.col("snapshot_date"), -1 * F.col("installments_missed"))).cast(DateType()))
        df = df.withColumn("dpd", F.when(F.col("overdue_amt") > 0.0, F.datediff(F.col("snapshot_date"), F.col("first_missed_date"))).otherwise(0).cast(IntegerType()))

        # filter: customers at mob
        df = df.filter(F.col("mob") == mob)

        # augment: get label
        df = df.withColumn("label", F.when(F.col("dpd") >= dpd, 1).otherwise(0))
        df = df.withColumn("label_def", F.lit(str(dpd) + "dpd_" + str(mob) + "mob"))

        # select: columns to save
        df = df.select("loan_id", "Customer_ID", "label", "label_def", "snapshot_date")

        # conform types from schema
        columns = table_config["columns"]
        for col_name, col_config in columns.items():
            spark_type = _TYPE_MAP[col_config["type"]]
            df = df.withColumn(col_name, F.col(col_name).cast(spark_type))

        partition_col = table_config["partition_col"]
        output_path = os.path.join(gold_path, "label_store")

        writer = df.write.format("delta").option("overwriteSchema", "true").partitionBy(partition_col)
        if snapshot_date_str is not None:
            writer.mode("overwrite").option("replaceWhere", f"snapshot_date = '{snapshot_date_str}'").save(output_path)
        else:
            writer.mode("overwrite").save(output_path)

        logger.info(f"Label store for {snapshot_date_str} written to {output_path}")

    except Exception as e:
        logger.error(e)


def _build_financial_features(financials_df):
    # augment: process credit history into months
    financials_df = financials_df.withColumn("ch_years", F.coalesce(F.regexp_extract(F.col("Credit_History_Age"), r"(\d+) Years", 1).cast(IntegerType()), F.lit(0)))
    financials_df = financials_df.withColumn("ch_months", F.coalesce(F.regexp_extract(F.col("Credit_History_Age"), r"(\d+) Months", 1).cast(IntegerType()), F.lit(0)))
    financials_df = financials_df.withColumn(
        "credit_history_months",
        F.when(F.col("Credit_History_Age").isNotNull(), (F.col("ch_years") * 12 + F.col("ch_months"))).otherwise(None),
    )
    financials_df = financials_df.drop("ch_years", "ch_months", "Credit_History_Age")

    # augment: encode credit mix
    financials_df = financials_df.withColumn(
        "credit_mix_encoded",
        F.when(F.col("Credit_Mix") == "Bad", 0).when(F.col("Credit_Mix") == "Standard", 1).when(F.col("Credit_Mix") == "Good", 2).otherwise(None).cast(IntegerType()),
    )
    financials_df = financials_df.drop("Credit_Mix")

    # augment: encode payment of min amount
    financials_df = financials_df.withColumn(
        "payment_of_min_amount_encoded",
        F.when(F.col("Payment_of_Min_Amount") == "No", 0).when(F.col("Payment_of_Min_Amount") == "Yes", 1).otherwise(None),
    )
    financials_df = financials_df.drop("Payment_of_Min_Amount")

    # augment: encode payment behaviour
    financials_df = financials_df.withColumn(
        "pb_spend_level",
        F.when(F.col("Payment_Behaviour").startswith("High"), 1).when(F.col("Payment_Behaviour").startswith("Low"), 0).otherwise(None).cast(IntegerType()),
    )
    financials_df = financials_df.withColumn(
        "pb_payment_value_level",
        F.when(F.col("Payment_Behaviour").contains("Small"), 0)
        .when(F.col("Payment_Behaviour").contains("Medium"), 1)
        .when(F.col("Payment_Behaviour").contains("Large"), 2)
        .otherwise(None)
        .cast(IntegerType()),
    )
    financials_df = financials_df.drop("Payment_Behaviour")

    # augment: handle type of loan
    financials_df = financials_df.withColumn(
        "Type_of_Loan",
        F.when(
            F.col("Type_of_Loan").isNotNull(),
            F.trim(F.regexp_replace(F.col("Type_of_Loan"), r",?\s+and\s+", ", ")),
        ).otherwise(None),
    )

    # augment: add additional features
    financials_df = financials_df.withColumn(
        "debt_to_income_ratio",
        F.when(
            F.col("Annual_Income").isNotNull() & (F.col("Annual_Income") > 0),
            (F.col("Outstanding_Debt") / F.col("Annual_Income")).cast(FloatType()),
        ).otherwise(None),
    )

    financials_df = financials_df.withColumn(
        "emi_to_income_ratio",
        F.when(
            F.col("Monthly_Inhand_Salary").isNotNull() & (F.col("Monthly_Inhand_Salary") > 0),
            (F.col("Total_EMI_per_month") / F.col("Monthly_Inhand_Salary")).cast(FloatType()),
        ).otherwise(None),
    )

    return financials_df


def _build_clickstream_features(clickstream_df, snapshot_date_str):
    fe_cols = [f"fe_{i}" for i in range(1, 21)]

    # augment: aggregate all columns for same date
    current_df = clickstream_df.filter(F.col("snapshot_date") == snapshot_date_str) if snapshot_date_str is not None else clickstream_df
    fe_exprs = [F.coalesce(F.col(c), F.lit(0.0)) for c in fe_cols]
    fe_sum = reduce(lambda a, b: a + b, fe_exprs)
    current_df = current_df.withColumn("fe_sum", fe_sum.cast(FloatType()))
    current_df = current_df.withColumn("fe_mean", (fe_sum / F.lit(20.0)).cast(FloatType()))
    current_df = current_df.select("Customer_ID", *fe_cols, "fe_sum", "fe_mean")

    # augment: aggregate columns for past dates
    history_df = clickstream_df.filter(F.col("snapshot_date") <= snapshot_date_str) if snapshot_date_str is not None else clickstream_df
    agg_exprs = []
    for c in fe_cols:
        agg_exprs.append(F.mean(F.col(c)).cast(FloatType()).alias(f"{c}_mean"))
        agg_exprs.append(F.stddev(F.col(c)).cast(FloatType()).alias(f"{c}_std"))
    agg_df = history_df.groupBy("Customer_ID").agg(*agg_exprs)

    current_df = current_df.join(agg_df, on="Customer_ID", how="left")

    return current_df


def _build_attribute_features(attributes_df):
    # select: drop identifier columns
    attributes_df = attributes_df.drop("Name", "SSN")

    # augment: add age bins
    attributes_df = attributes_df.withColumn(
        "age_group",
        F.when(F.col("Age") < 26, 0).when(F.col("Age") < 36, 1).when(F.col("Age") < 46, 2).when(F.col("Age") < 56, 3).otherwise(4).cast(IntegerType()),
    )

    return attributes_df


def process_features_gold_table(table_config, silver_path, gold_path, spark, snapshot_date_str=None):
    try:
        partition_col = table_config["partition_col"]
        output_path = os.path.join(gold_path, "feature_store")

        attributes_df = spark.read.format("delta").load(os.path.join(silver_path, "features_attributes"))
        if snapshot_date_str is not None:
            attributes_df = attributes_df.filter(F.col("snapshot_date") == snapshot_date_str)
        attributes_df_features = _build_attribute_features(attributes_df)

        financials_df = spark.read.format("delta").load(os.path.join(silver_path, "features_financials"))
        if snapshot_date_str is not None:
            financials_df = financials_df.filter(F.col("snapshot_date") == snapshot_date_str)
        financials_df_features = _build_financial_features(financials_df)

        clickstream_df = spark.read.format("delta").load(os.path.join(silver_path, "feature_clickstream"))
        clickstream_features_df = _build_clickstream_features(clickstream_df, snapshot_date_str)

        feature_df = financials_df_features.join(attributes_df_features.drop("snapshot_date"), on="Customer_ID", how="left")
        feature_df = feature_df.join(clickstream_features_df, on="Customer_ID", how="left")

        columns = table_config["columns"]
        for col_name, col_config in columns.items():
            spark_type = _TYPE_MAP[col_config["type"]]
            feature_df = feature_df.withColumn(col_name, F.col(col_name).cast(spark_type))

        writer = feature_df.write.format("delta").option("overwriteSchema", "true").partitionBy(partition_col)
        if snapshot_date_str is not None:
            writer.mode("overwrite").option("replaceWhere", f"snapshot_date = '{snapshot_date_str}'").save(output_path)
        else:
            writer.mode("overwrite").save(output_path)
        logger.info(f"Feature store for {snapshot_date_str} written to {output_path}")

    except Exception as e:
        logger.error(e)
