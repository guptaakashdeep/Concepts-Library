import pyarrow.csv as pv
import pyarrow.compute as pc
import pyarrow as pa
import io

csv_file_path = '/mnt1/arc_fe_feed_generation_test/feed_ge_a31_exp_m_u_int_tc4_back_feed_2507171208.txt'
columns_to_sum = ['gross_dep_utilisation_amt']

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
    print(f"--- Starting reprocessing of {len(invalid_records)} invalid records ---")
    try:
        # 1. Clean the collected raw text rows
        cleaned_rows = [row.replace('\x00', '').replace('\x1a', '') for row in invalid_records]
        
        # 2. Create a single CSV string in memory, including a header
        # Using the full schema ensures the header matches the data.
        header = delimiter.join(full_schema.names)
        csv_content = header + '\n' + '\n'.join(cleaned_rows)

        # 3. Parse the cleaned data using the captured schema
        # We build a `column_types` dict to enforce the original schema.
        column_types = {name: type for name, type in zip(full_schema.names, full_schema.types)}
        
        invalid_convert_options = pv.ConvertOptions(column_types=column_types)
        
        invalid_table = pv.read_csv(
            io.BytesIO(csv_content.encode('utf-8')),
            parse_options=pv.ParseOptions(delimiter=delimiter, quote_char='"'),
            convert_options=invalid_convert_options
        )
        print("Successfully parsed cleaned invalid records.")

        # 4. Calculate sums from the invalid records' table and add to totals
        for col_name in columns_to_sum:
            invalid_sum = pc.sum(invalid_table[col_name]).as_py()
            if invalid_sum is not None:
                print(f"Adding sum for column '{col_name}' from invalid records: {invalid_sum}")
                total_sums[col_name] += invalid_sum

    except Exception as e:
        print(f"An error occurred during reprocessing of invalid records: {e}")

# --- Final Results ---
print("\n--- Aggregation Complete ---")
for col, total in total_sums.items():
    print(f"Final total sum for column '{col}': {total}")


"""'D|80001082208_TSYSCCTR_U|8.0001082208E10|||GRDLE|TSYS TS2 Credit Card Issuing Platform|N|U|15Jul2025|UNDRAWN-COMM|Term Loan|12157|PR_CARDS|Cards (prev Commercial & Purchasing Cards)|LOAN|OTHER ITEMS|VHABBGB|DFH ESTATES LIMITED|VHABBGB|GRDLE|30603548|6424999172|N|LE|Corporate|Corporate|K|Non Fin Corps (not Public)|51.22/0/0|Wholesale of flowers and plants|GB|United Kingdom|United Kingdom|GB|United Kingdom|United Kingdom|GB|United Kingdom|United Kingdom|Y|RR|N|VHM49GB|NEW LEAF PLANTS LTD|VHM49GB|6570263107|TSYSCCTR|30620444|6570263107|LE|N|Corporate|Corporate|K|Non Fin Corps (not Public)|51.22/0/0|Wholesale of flowers and plants|GB|United Kingdom|United Kingdom|GB|United Kingdom|United Kingdom|GB|United Kingdom|United Kingdom|Y|N|RR||23|Unknown|MID CORPORATE|DWN||Unknown|Unknown||414906700_FACRMP|UNKNOWN|6656168454||||611.00|1 year < maturity <= 3 years||GBP|||||Y|12073.00||||||||||B|N|N||N/A|013005104|BUSINESS CURRENT A C  AC|CORPOTHER|AC LC CORPORATE OTHER|ASSETS|TOTAL ASSETS|UKIG|N/A||N/A|N/A|N/A|605050360|NAT WEST ONE CARD|PND|BUSINESS & COMMERCIAL BANKING - CORE NWB|ENWBKGB|NATIONAL WESTMINSTER BANK PUBLICLIMITED COMPANY|NWBAI|NWB BANK PLC (AUTHORISED INSTITUTION)|NWBSOLO|NWB PLC (SOLO CONSOLIDATED)|CPBTOT3244|SME AND MID CORPS CENTRALS|CPBTOT2108|CMM EXCL LOMBARD AND IF|CPBTOT1002|COMMERCIAL AND MID  MARKET|CPBTOT|COMMERCIAL AND INSTITUTIONAL||STD|Standardized|AIRB||||0E-7|0.00|1207.30|1207.30|0.8500000|1026.21|1026.21|0E-7|0E-7|10.0000000||||1207.30|1207.30|0.8500000|1026.21|1026.21|||U|10.0000000|1.0||0.1032380|124.64|1207.30|1207.30|2.4857159|3001.13|3001.00|0.1988654|0.1988573|GBP|||U|1207.30|N||O|?|Unknown|U|Unknown|Unknown|Y||||-1.0||0.00|||4093895.57|||12157|NW MCI ONECARD||UNKNOWN||12073.00||1207.30|1207.30|0.00|||L|||100.0|United Kingdom|United Kingdom|United Kingdom|United Kingdom|United Kingdom|United Kingdom|82.10|||||Mid Corporate|Risk Rating Mid Corporate Calculation (Version 1.4)||||Unknown|||||||CPBTOT4024|EX C AND CCCENTRE|CPBTOT5042|EX C AND CC CENTRE|1.0|-1.0||||||2026-12-02||||||||Unrated|||||||||||||Other Agriculture|Agriculture|Agriculture|Corporate|0.1448200|UK-FSA|STD||Credit Cards|Credit Cards|Drawn|Credit Cards|||Y||3427485.0|8|8|6421627967|"DFH ESTATES LIMITED HMA\x1atfr Restruct 6/8/24\x00""|RMP|6421627967|"DFH ESTATES LIMITED HMA\x1atfr Restruct 6/8/24\x00""|RMP|N||N||N|Y|||N|||Unrated||NWBCON|NWB PLC (CONSOLIDATED)||||||||||Senior|||U|N|N|No|No|||01.12/0/1|51.22/0/0||||Unknown|RMP|UNKNOWN||NWBSOLO|||1026.21||C|N|N|N||UNITED KINGDOM|UK|UK|990129200|990129200|1.0|Wholesale Undrawn|2025-03-31|2025-03-31|||R||||||Unknown|N||Unknown|6034647.00|7208991.72||N||||N|||||||||||||N|||||OFF BALANCE SHEET|||||||||||0.1000000|U|0.8500000|||||||||1.4||||0|||||||||Y||0E-7|0E-7||||||||||||||4361.0||2.0|RCFs and Similar||||Pre and Post LGD Calc values are inherited from parent Facilities Pre and Post LGD Calc values|||||||||GB|United Kingdom|0.0800860|0.1988654|0.1988573|4093895.57|GBP|CRADLE|Y|Y||Y|3001.00|2509|NWP_PND_E|1.0|2.0|No limit|A|N|SGOVTGB|3.0|LNS/ADV TO CUSTS||||||NEW LEAF PLANTS LTD||NEW LEAF PLANTS LTD|Undrawn|||10.0000000|10.0000000|||||1207.30||GB|United Kingdom|||8|CCB Country is derived from Country of Operation|1|Country of Incorporation is not defaulted/derived or no fallback applied|4|Country of Operation is not defaulted/derived or no fallback applied|6|Country of Residence is not defaulted/derived or no fallback applied|N|CRADLE||GBP|||G|Wholesale and retail trade; repair of motor vehicles and motorcycles|||1207.30|1207.30|414906700_PLEL||7209413.41||CRADLE|CRADLE|||||MGS23|0.1448200|MGS23|0.1448200|MGS23||N||2018-01-05||||||||||ITRF|CORE||||||0.1448200|Risk Rating Mid Corporate Calculation (Version 1.4)||||||||VCAT4|Viability Category 4|RMP||0.7972954|||||||||Risk of Credit Loss||||211470177||||||||80001082208_TSYSCCTR|N|0.4000000|0.1448200|0.1115811|0.1115811|1.0|69.94|0.0579280|1.3947634|1683.90|1342.56|1683.90|0.4000000|0.1448200|||||||||||06771266||||||||||||0.0800860|||||||||||||||||||||||||||2014-12-24||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||PRA|PRA|170535001|100001183||||||||N|||||C|||||||N||1026.21|||||||||Corporate SMEs|Corporate|Corporate|Corp < 50||||N||||||||0.2500000||2120103|0.7128710|1207.30|0.1448200|0.7129000|0.1448200|Undrawn|||||||||||Unrated|1||0.0067092|1.674|||N|GBP|N||1207.30||||||||||||||||||||||||||ROCL|Risk of Credit Loss|0.1448200|N||||||0||Y||1207.30|0.7128710|0.8500000|0.7129000|||||0E-7|||||||LR0030|3427000.00|3001.13|1683.90|3427000.00|1207.30|N|N|N||||||||||N|818.19|818.19|1342.56|1342.56|2392.78|2392.69|1.3947634|2.4858170|414906700_FACRMP|414906700_FPLRMP||||N||||||||||||||||||||||||||||12073.00|12073.00|12073.00|||||||||||||BRYSCGB|VPE7ZGB|ENWBKGB|||||NATWEST GROUP PLC|NATWEST HOLDINGS LIMITED|NATIONAL WESTMINSTER BANK PUBLIC LIMITED COMPANY|||||ITRF||VPE7ZGB|ENWBKGB|Y|||N/A|N/A||2025-07-17|20250331|03|2025-07-17'"""