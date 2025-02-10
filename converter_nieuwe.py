import sys
import os
import dask.dataframe as dd
import pandas as pd
import csv

def detect_delimiter(filepath, sample_size=1000):
    """Detects the delimiter of a CSV file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        sample = [next(f) for _ in range(sample_size)]
    sniffer = csv.Sniffer()
    return sniffer.sniff("\n".join(sample)).delimiter

def convert_csv_to_parquet(csv_path, output_dir):
    print(f"🔍 Validating CSV before conversion: {csv_path}")

    # detect delimiter
    delimiter = detect_delimiter(csv_path)
    print(f"🧐 Detected delimiter: '{repr(delimiter)}'")

    # ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    keep_columns = [
        'gbifID', 'species', 'decimalLongitude', 'decimalLatitude',
        'countryCode', 'elevation', 'datasetKey', 'eventDate'
    ]

    dtype_fix = {
        'gbifID': 'object',
        'species': 'object',
        'decimalLongitude': 'object',  # Will be converted later
        'decimalLatitude': 'object',   # Will be converted later
        'countryCode': 'object',
        'elevation': 'object',         # Will be converted later
        'datasetKey': 'object',
        'eventDate': 'object'
    }

    try:
        # reads CSV with detected delimiter and enforced dtypes
        ddf = dd.read_csv(
            csv_path,
            delimiter=delimiter,
            usecols=keep_columns,
            dtype=dtype_fix,
            assume_missing=True
        )
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        sys.exit(1)

    print("\n🚨 **Checking for mixed data types in columns...**")
    problematic_values = {}

    for col in ['decimalLongitude', 'decimalLatitude', 'elevation']:
        try:
            # convert to float, keeping track of errors
            ddf[col] = ddf[col].apply(pd.to_numeric, errors='coerce', meta=(col, 'float64'))
            non_numeric = ddf[col][ddf[col].isna()].compute().dropna().unique()
            if len(non_numeric) > 0:
                problematic_values[col] = non_numeric
        except Exception as e:
            print(f"⚠️ Error checking column {col}: {e}")

    # log problematic values
    if problematic_values:
        print("\n🚨 **Problematic values detected in numeric columns:**")
        for col, values in problematic_values.items():
            print(f"⚠️ Column: {col} - Invalid values: {values[:5]}... (showing first 5)")

    # display missing value report
    missing_report = ddf.isnull().sum().compute()
    print("\n📉 **Missing Values Per Column Before Conversion:**")
    print(missing_report[missing_report > 0])

    # handle missing data
    ddf = ddf.fillna({
        'species': 'Unknown',
        'countryCode': 'Unknown',
        'decimalLongitude': -9999,
        'decimalLatitude': -9999,
        'elevation': -9999,
        'eventDate': 'Unknown'
    })

    try:
        # convert to Parquet format
        ddf.to_parquet(output_dir, engine='pyarrow', write_index=False)
        print(f"✅ Conversion complete. Parquet files saved in: {output_dir}")
    except Exception as e:
        print(f"❌ Error writing Parquet files: {e}")
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python converter_newer.py <input_csv_file> <output_parquet_dir>")
        sys.exit(1)

    csv_file = sys.argv[1]
    output_dir = sys.argv[2]

    convert_csv_to_parquet(csv_file, output_dir)
