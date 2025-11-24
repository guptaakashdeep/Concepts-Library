import io
from contextlib import ExitStack
from urllib.parse import urlparse

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
import pyarrow.fs as fs


class PyArrowFeedAggregator:
    """Aggreagtes the sum for the given columns from the provided feed file"""

    def __init__(self, feed_path, delimiter, columns, table_name):
        self.is_s3_uri = False  # initialized as false
        self.csv_file_path = self._get_fspath(feed_path)
        self.delimiter = delimiter
        self.columns_to_sum = columns
        self.invalid_records = []
        self.full_schema = None
        self.s3 = fs.S3FileSystem()
        self.col_type = {}
        # Initialize a dictionary to hold the running totals
        self.total_sums = self._initialize_sum(table_name)
        self.record_count = 0

    def _initialize_sum(self, table_name: str) -> dict:
        """Check the schema to initialize"""
        # Early return in case there are no columns to aggregate
        if not self.columns_to_sum:
            return {}
        client = boto3.client("glue", region_name="eu-west-1")
        db, table = table_name.split(".")
        table_details = client.get_table(DatabaseName=db, Name=table)
        catalog_cols = (
            table_details.get("Table").get("StorageDescriptor").get("Columns")
        )
        self.col_type = {
            d["Name"]: d["Type"]
            for d in list(
                filter(lambda x: x["Name"] in self.columns_to_sum, catalog_cols)
            )
        }
        print("--- Schema Identified from table: ---\n", self.col_type)
        if not self.col_type:
            raise ValueError(
                f"{self.columns_to_sum} not found in {table_name} schema. Please validate."
            )
        return {
            cname: 0 if ctype in {"bigint", "int"} else 0.0
            for cname, ctype in self.col_type.items()
        }

    def _get_fspath(self, path):
        """Check if path is local or S3 URI.
        If S3 path, update path as required to be read using PyArrow"""
        parsed = urlparse(path)
        if parsed.scheme != "s3":
            return path
        self.is_s3_uri = True
        return f"{parsed.netloc}/{parsed.path.lstrip('/')}"

    def _custom_invalid_handler(self, invalid_row):
        """Custom handler that tries to capture invalid row info"""
        self.invalid_records.append(invalid_row.text)
        # print(repr(invalid_row.text))
        return "skip"

    def _pre_scan_for_schema(self, read_options, parse_options):
        """Read small chunk for schema referencing."""
        try:
            with ExitStack() as stack:
                input_source = (
                    stack.enter_context(self.s3.open_input_stream(self.csv_file_path))
                    if self.is_s3_uri
                    else self.csv_file_path
                )
                reader = stack.enter_context(
                    pv.open_csv(
                        input_source,
                        read_options=read_options,
                        parse_options=parse_options,
                    )
                )
                first_batch = next(reader)
                if not first_batch:
                    raise Exception("No batches found during pre-scan")
                self.full_schema = first_batch.schema
                print("Full schema captured successfully.")
        except Exception as e:
            print(f"Could not pre-scan for schema. Error: {e}. Exiting.")
            exit()

    def _calculate_parse_failed_records(self):
        """Aggregation for records marked as invalid while parsing from PyArrow"""
        try:
            # 1. Clean the collected raw text rows.
            cleaned_rows = [
                row.replace("\x00", "").replace("\x1a", "").replace('""', '"')
                for row in self.invalid_records
            ]
            csv_content = "\n".join(cleaned_rows)
            # print(csv_content)

            # 2. Use Pandas' robust CSV parser to read the problematic rows into a DataFrame.
            # We provide the full list of column names from the pyarrow schema.
            # `on_bad_lines='warn'` will tell us if any rows are still unparseable even for Pandas.
            # drop_duplicates to avoid in case any parse failed records added during pre scan also.
            invalid_df = pd.read_csv(
                io.StringIO(csv_content),
                sep=self.delimiter,
                header=None,
                names=self.full_schema.names,
                quotechar='"',
                engine="python",  # for flexible parsing
                on_bad_lines="error",
            ).drop_duplicates()
            print("Successfully parsed invalid records using Pandas.")

            # update record count after removing duplicates that might have been added
            # during pre-scan phase
            self.record_count += len(invalid_df)

            # 3. Calculate sums from the new DataFrame and add them to the totals.
            # `numeric_only=True` ensures we only try to sum columns that are numbers.
            invalid_sums = invalid_df[self.columns_to_sum].sum(numeric_only=True)
            for col_name, value in invalid_sums.items():
                self.total_sums[col_name] += value

        except Exception as e:
            print(f"An error occurred during reprocessing of invalid records: {e}")
            raise e

    def calculate(self):
        """Aggregate values to get the sum of values"""
        # using 256MB block_size for better I/O performance
        read_options = pv.ReadOptions(block_size=256 * 1024 * 1024)
        parse_options = pv.ParseOptions(
            delimiter=self.delimiter,
            quote_char='"',
            double_quote=True,
            invalid_row_handler=self._custom_invalid_handler,
        )
        # Enforce schema for the include columns
        if self.col_type:
            schema = pa.schema(
                [
                    pa.field(cname, pa.int64())
                    if ctype in {"bigint", "int"}
                    else pa.field(cname, pa.float64())
                    for cname, ctype in self.col_type.items()
                ]
            )
        else:
            schema = pa.schema(
                [pa.field(cname, pa.float64()) for cname in self.columns_to_sum]
            )
        # This is the key for efficiency: only include the columns we need.
        # Pyarrow will not read the other columns from disk.
        convert_options = pv.ConvertOptions(
            include_columns=self.columns_to_sum, column_types=schema
        )

        # --- Pre-scan to get the full schema ---
        print("--- Pre-scanning file to determine full schema ---")
        self._pre_scan_for_schema(
            pv.ReadOptions(block_size=32 * 1024 * 1024), parse_options
        )

        print(f"\n--- Starting Main Processing of {self.csv_file_path} ---")
        try:
            with ExitStack() as stack:
                input_source = (
                    stack.enter_context(self.s3.open_input_stream(self.csv_file_path))
                    if self.is_s3_uri
                    else self.csv_file_path
                )
                reader = stack.enter_context(
                    pv.open_csv(
                        input_source,
                        read_options=read_options,
                        parse_options=parse_options,
                        convert_options=convert_options,
                    )
                )
                for batch_num, next_batch in enumerate(reader):
                    if next_batch is None:
                        break
                    self.record_count += next_batch.num_rows
                    print(f"Processing valid batch {batch_num + 1}...")
                    for col_name in self.columns_to_sum:
                        batch_sum = pc.sum(next_batch[col_name]).as_py()
                        if batch_sum is not None:
                            self.total_sums[col_name] += batch_sum
        except Exception as e:
            print(f"An unexpected error occurred during main processing: {e}")
            raise e

        print("--- Aggregation compelted of parsed records ---")
        for col, total in self.total_sums.items():
            print(f"Parsed records sum of column '{col}': {total}")
        print(
            f"\n--- Main processing complete. {len(self.invalid_records)} invalid rows were skipped. ---"
        )

        # --- Process Invalid Records (if any) ---
        if self.invalid_records and self.full_schema:
            print(
                f"--- Starting reprocessing of {len(self.invalid_records)} invalid records using Pandas ---"
            )
            self._calculate_parse_failed_records()

        # --- Final Results ---
        print("\n--- Aggregation Complete ---")
        for col, total in self.total_sums.items():
            print(f"Final total sum for column '{col}': {total}")
        return self.total_sums, self.record_count


if __name__ == "__main__":
    # Reading Feed from S3
    aggregator = PyArrowFeedAggregator(
                    "s3://landing-zone-bucket/data-out/test/merged.txt.gz",
                    "|",
                    ['fac_id'],
                    "db.fac_table")

    # Feed file from local
    # aggregator = PyArrowFeedAggregator(
    #                 "/mnt/test/merged.csv.gz",
    #                 "|",
    #                 ['fac_id'],
    #                 "db.fac_table")
    sums, num_records = aggregator.calculate()
    print(sums)
    print(num_records)
