"""Example of Horizontal Parallelism in Spark"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime


# Creating SparkSession
spark = SparkSession.builder.master("yarn").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

count_list = []


def get_count(table_name: str) -> dict:
    """Gets the number of records count in a table.

    Args:
        table_name (str): Name of the table.

    Returns:
        dict: Dictionary with keys "table_name" and "count"
    """
    try:
        count_dict = {}
        table_count = spark.sql(f"select count(*) from {table_name}").collect()[0][0]
        count_dict["table_name"] = table_name
        count_dict["count"] = table_count
        return count_dict
    except Exception:
        count_dict["table_name"] = table_name
        count_dict["count"] = 0
        return count_dict


def main(tables: list) -> None:
    """Main method to submit count jobs in parallel.

    Args:
        tables (list): list of table name.

    Raises:
        e: Exception in case of any failures
    """
    with ThreadPoolExecutor(max_workers=6) as executor:
        to_do_map = {}
        for table in tables:
            # Submitting jobs in parallel
            future = executor.submit(get_count, table)
            print(f"scheduled for {table}: {future}")
            to_do_map[future] = table
        done_iter = as_completed(to_do_map)

        for future in done_iter:
            try:
                count = future.result()
                print("result: ", count)
                count_list.append(count)
            except Exception as e:
                raise e


if __name__ == "__main__":
    CNT_TABLE = "db.table_counts"
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t",
        "--tables",
        nargs="*",
        required=True,
        help="List of tables for which count needs to be checked.",
    )

    args = parser.parse_args()
    TABLES = args.tables

    # call main method for getting counts in parallel
    main(TABLES)
    # count_list -> list of dict containing table_name and count
    print(count_list)
    # Create dataframe
    table_count_df = spark.createDataFrame(count_list)
    table_count_df = table_count_df.withColumn("insert_ts", lit(datetime.now()))

    # Write dataframe into a control table.
    col_order = spark.read.table(CNT_TABLE).limit(1).columns
    table_count_df.select(*col_order).coalesce(1).write.insertInto(
        CNT_TABLE, overwrite=True
    )
