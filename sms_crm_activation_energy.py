#!/usr/bin/env python3
"""
Usage:
  # for plain csv
  python3 validate_csv_for_bigquery.py /tmp/filename.csv

  # for gzipped csv
  python3 validate_csv_for_bigquery.py /tmp/filename.csv.gz
"""
import csv, sys, gzip, argparse
from datetime import datetime

# -------------------------
# Customize to your schema
# -------------------------
EXPECTED_COLUMNS = [
    "SMS Campaign Name","Record ID","SMS Error Code","SMS Error Reason",
    "Phone Number","SMS Delivery Status","SMS Transaction ID",
    "SMS delivered timestamp","SMS opened timestamp","SMS Sent From",
    "SMS URL","SMS Brand Name","Clicked at"
]

DATETIME_COLS = [
    "SMS delivered timestamp","SMS opened timestamp","Clicked at"
]

DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y",
    "%Y-%m-%d"
]

def try_parse_datetime(s):
    s = s.strip()
    if s == "":
        return True
    for fmt in DATETIME_FORMATS:
        try:
            datetime.strptime(s, fmt)
            return True
        except Exception:
            pass
    return False

def open_maybe_gz(path):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace", newline='')
    else:
        return open(path, "r", encoding="utf-8", errors="replace", newline='')

def main(path):
    with open_maybe_gz(path) as fh:
        reader = csv.DictReader(fh)
        header = reader.fieldnames
        if header is None:
            print("ERROR: No header found in file.")
            return 1

        # header checks
        missing = [c for c in EXPECTED_COLUMNS if c not in header]
        unexpected = [c for c in header if c not in EXPECTED_COLUMNS]
        print("Header: columns found =", len(header))
        if missing:
            print("WARNING: Missing expected columns:", missing)
        if unexpected:
            print("WARNING: Unexpected columns in file (first 10):", unexpected[:10])

        # row-level checks
        problems = 0
        for line_no, row in enumerate(reader, start=2):  # start=2 because header is line 1
            errs = []
            # column count mismatch (csv module normally handles quoting; but check length)
            if len(row) != len(header):
                errs.append(f"column_count_mismatch (got {len(row)} vs header {len(header)})")

            # datetime checks
            for dt in DATETIME_COLS:
                if dt in row:
                    v = row.get(dt) or ""
                    if v.strip() != "" and not try_parse_datetime(v):
                        errs.append(f"invalid_datetime_in `{dt}` value=`{v}`")

            if errs:
                problems += 1
                print(f"Line {line_no}: " + "; ".join(errs))
                # optionally stop after N problems to save time; comment out to list all
                if problems >= 200:
                    print(f"Stopping after {problems} problems (change limit in script).")
                    break

        print(f"Completed. Found {problems} problematic rows (reported up to 200).")
    return 0

if __name__ == "__main__":
    main("/Users/pikachu_luffy/Documents/pikachu_coder/SMS Engagement Data.csv")