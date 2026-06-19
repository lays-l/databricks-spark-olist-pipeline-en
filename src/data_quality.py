from pyspark.sql import DataFrame
from pyspark.sql.functions import col


VALID_ORDER_STATUSES = [
    "created", "approved", "invoiced", "processing",
    "shipped", "delivered", "unavailable", "canceled"
]


def check_not_null(df: DataFrame, column: str) -> DataFrame:
    """Returns records where the column is null."""
    return df.filter(col(column).isNull())


def check_non_negative(df: DataFrame, column: str) -> DataFrame:
    """Returns records where the numeric value is negative."""
    return df.filter(col(column) < 0)


def check_valid_status(df: DataFrame, column: str, valid_values: list) -> DataFrame:
    """Returns records where the value is not in the list of allowed values."""
    return df.filter(~col(column).isin(valid_values))


def check_no_duplicates(df: DataFrame, key_column: str) -> DataFrame:
    """
    Returns key_column groups that appear more than once.
    Used to validate the uniqueness of a table's primary key (grain).
    """
    from pyspark.sql.functions import count
    return (
        df.groupBy(key_column)
        .agg(count("*").alias("occurrences"))
        .filter(col("occurrences") > 1)
    )
