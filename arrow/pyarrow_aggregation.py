# Aggregating sum of columns from a huge csv file that can't fit into memory using PyArrow
import pyarrow.csv as pv
import pyarrow.compute as pc
import pyarrow as pa
import io
import pandas as pd

csv_file_path = 'big_80gb_file.csv'
columns_to_sum = ['col1', 'col2', 'col3']

# Initialize a dictionary to hold the running totals
total_sums = {col: 0.0 for col in columns_to_sum}
file_schema = None

invalid_records = []

def custom_invalid_handler(invalid_row):
    """Custom handler that tries to capture invalid row info"""
    global invalid_records
    invalid_records.append(invalid_row.text)
    # print(repr(invalid_row.text))
    return 'skip'



# using 256MB block_size for better I/O performance
delimiter = '|'
read_options = pv.ReadOptions(block_size=256 * 1024 * 1024)
parse_options = pv.ParseOptions(delimiter=delimiter,
                quote_char='"',
                double_quote=True,
                invalid_row_handler=custom_invalid_handler)

# This is the key for efficiency: only include the columns we need.
# Pyarrow will not read the other columns from disk.
convert_options = pv.ConvertOptions(include_columns=columns_to_sum)

# --- Pre-scan to get the full schema ---
print("--- Pre-scanning file to determine full schema ---")
try:
    with pv.open_csv(csv_file_path, read_options=read_options, parse_options=parse_options) as reader:
        first_batch = next(reader)
        if first_batch:
            full_schema = first_batch.schema
            print("Full schema captured successfully.")
except Exception as e:
    print(f"Could not pre-scan for schema. Error: {e}. Exiting.")
    exit()

print(f"\n--- Starting Main Processing of {csv_file_path} ---")
try:
    with pv.open_csv(
        csv_file_path,
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options
    ) as reader:
        for batch_num, next_batch in enumerate(reader):
            if next_batch is None:
                break

            print(f"Processing valid batch {batch_num + 1}...")
            for col_name in columns_to_sum:
                batch_sum = pc.sum(next_batch[col_name]).as_py()
                if batch_sum is not None:
                    total_sums[col_name] += batch_sum

except Exception as e:
    print(f"An unexpected error occurred during main processing: {e}")

print("--- Aggregation compelted of parsed records ---")
for col, total in total_sums.items():
    print(f"Parsed records sum of column '{col}': {total}")

print(f"\n--- Main processing complete. {len(invalid_records)} invalid rows were skipped. ---")

# --- Process Invalid Records (if any) ---
if invalid_records and full_schema:
    print(f"--- Starting reprocessing of {len(invalid_records)} invalid records using Pandas ---")
    try:
        # 1. Clean the collected raw text rows.
        cleaned_rows = [row.replace('\x00', '').replace('\x1a', '').replace('""','"') for row in invalid_records]
        csv_content = "\n".join(cleaned_rows)

        # 2. Use Pandas' robust CSV parser to read the problematic rows into a DataFrame.
        # We provide the full list of column names from the pyarrow schema.
        # `on_bad_lines='warn'` will tell us if any rows are still unparseable even for Pandas.
        invalid_df = pd.read_csv(
            io.StringIO(csv_content),
            sep=delimiter,
            header=None,
            names=full_schema.names,
            quotechar='"',
            engine='python', # More robust for tricky parsing than the C engine
            on_bad_lines='warn'
        )
        print("Successfully parsed invalid records using Pandas.")

        # 3. Calculate sums from the new DataFrame and add them to the totals.
        # `numeric_only=True` ensures we only try to sum columns that are numbers.
        invalid_sums = invalid_df[columns_to_sum].sum(numeric_only=True)
        for col_name, value in invalid_sums.items():
            total_sums[col_name] += value

    except Exception as e:
        print(f"An error occurred during reprocessing of invalid records: {e}")

# --- Final Results ---
print("\n--- Aggregation Complete ---")
for col, total in total_sums.items():
    print(f"Final total sum for column '{col}': {total}")