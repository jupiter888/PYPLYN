import pandas as pd
import sys
import csv

def detect_delimiter(filepath, sample_size=1000):
    """Detect the most likely delimiter from a sample of the CSV file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        sample = [next(f) for _ in range(sample_size)]
    sniffer = csv.Sniffer()
    return sniffer.sniff("\n".join(sample)).delimiter

def inspect_csv(filepath, sample_size=50000):
    print(f"🔍 Inspecting CSV file: {filepath}")

    # detect delimiter automatically
    delimiter = detect_delimiter(filepath)
    print(f"🧐 Detected delimiter: '{repr(delimiter)}'")

    dtype_fix = {
        'gbifID': 'object',
        'species': 'object',
        'decimalLongitude': 'float64',
        'decimalLatitude': 'float64',
        'countryCode': 'object',
        'elevation': 'float64',
        'datasetKey': 'object',
        'eventDate': 'object'
    }

    try:
        # load CSV with detected delimiter
        df = pd.read_csv(
            filepath,
            nrows=sample_size,
            dtype=dtype_fix,
            delimiter=delimiter,
            low_memory=False
        )
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return

    print("✅ CSV successfully loaded. Checking for mixed data types...\n")

    # columns to inspect
    issue_columns = {}

    for col in dtype_fix.keys():
        unique_types = df[col].apply(lambda x: type(x)).value_counts()
        if len(unique_types) > 1:
            issue_columns[col] = unique_types

    if issue_columns:
        print("\n🚨 **Columns with Mixed Data Types:**")
        for col, types in issue_columns.items():
            print(f"⚠️ Column: {col} - Unique data types: {types.to_dict()}")
    else:
        print("✅ No mixed types detected. Data types are consistent.")

    # check for missing values
    missing_values = df.isnull().sum()
    print("\n📉 **Missing Values Per Column:**")
    print(missing_values[missing_values > 0])

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python inspect_csv_columns.py <input_csv_file>")
        sys.exit(1)

    csv_file = sys.argv[1]
    inspect_csv(csv_file)
