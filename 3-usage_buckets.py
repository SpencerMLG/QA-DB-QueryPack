import csv
import os
import time
from datetime import datetime

import pyodbc
from dotenv import load_dotenv

print(f"[{datetime.now().strftime('%H:%M:%S')}] Script started")

# Load environment variables
print(f"[{datetime.now().strftime('%H:%M:%S')}] Loading environment variables...")
load_dotenv()
print(f"[{datetime.now().strftime('%H:%M:%S')}] Environment variables loaded")

# Build connection string
print(f"[{datetime.now().strftime('%H:%M:%S')}] Building connection string...")
connection_string = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('DB_SERVER')};"
    f"DATABASE={os.getenv('DB_NAME')};"
    f"UID={os.getenv('DB_USER')};"
    f"PWD={os.getenv('DB_PASSWORD')};"
    f"APP={os.getenv('APP_NAME')};"
    "TrustServerCertificate=yes;"
    "Connection Timeout=25"
)
print(f"[{datetime.now().strftime('%H:%M:%S')}] Connection string built")

try:
    # Connect to database
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Attempting to connect to: {os.getenv('DB_SERVER')}"
    )
    start_time = time.time()
    conn = pyodbc.connect(connection_string)
    connect_time = time.time() - start_time
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Connection successful! (took {connect_time:.2f} seconds)"
    )

    # Step 1: Get usage counts
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Preparing usage count query...")
    cursor = conn.cursor()
    query = """
    SELECT
        CorelogicDataId,
        COUNT(*) as Usage
    FROM MonsterDataGeneration.dbo.UnusedDataOrder
    WHERE ProgramTypeId = 95 AND DropDate >= '2025-02-01'
    GROUP BY CorelogicDataId
    HAVING COUNT(*) > 1
    """
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Query prepared, executing...")

    # Execute query
    start_time = time.time()
    cursor.execute(query)
    query_time = time.time() - start_time
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Query executed (took {query_time:.2f} seconds)"
    )

    # Fetch results
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching results...")
    start_time = time.time()
    results = cursor.fetchall()
    fetch_time = time.time() - start_time
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Results fetched (took {fetch_time:.2f} seconds)"
    )

    # Get column names and determine correct case
    column_names = [column[0].lower() for column in cursor.description]
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Column names: {column_names}")

    # Find index of id and usage columns
    id_col_index = -1
    usage_col_index = -1

    for i, col in enumerate(column_names):
        if col.lower() == "corelogicdataid":
            id_col_index = i
        elif col.lower() == "usage":
            usage_col_index = i

    if id_col_index == -1 or usage_col_index == -1:
        raise ValueError(
            f"Required columns not found in result. Available columns: {column_names}"
        )

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Using column indices: id_col={id_col_index}, usage_col={usage_col_index}"
    )

    if results:
        result_count = len(results)
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Found {result_count} CorelogicDataIds with usage > 1"
        )

        # Display first few results
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Sample of raw data:")
        for i, row in enumerate(results[:3]):
            print(
                f"  Row {i+1}: CorelogicDataId: {row[id_col_index]}, Usage: {row[usage_col_index]}"
            )
            if i >= 2:
                break

        # Group by usage buckets
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Grouping results into buckets..."
        )
        usage_buckets = {"2": 0, "3": 0, "4": 0, "5": 0, "6": 0, "7+": 0}

        for row in results:
            usage = row[usage_col_index]
            if usage >= 7:
                usage_buckets["7+"] += 1
            else:
                usage_buckets[str(usage)] += 1

        # Display bucket counts
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Usage distribution:")
        for bucket, count in usage_buckets.items():
            print(f"  CorelogicDataIds with usage {bucket}: {count}")

        # Export both raw data and buckets to CSV
        output_raw = "corelogic_usage_counts.csv"
        output_buckets = "corelogic_usage_buckets.csv"

        # Export raw data
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Exporting raw data to {output_raw}..."
        )
        with open(output_raw, "w", newline="") as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(["CorelogicDataId", "Usage"])
            for row in results:
                csv_writer.writerow([row[id_col_index], row[usage_col_index]])

        # Export buckets
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Exporting bucket summary to {output_buckets}..."
        )
        with open(output_buckets, "w", newline="") as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(["UsageCategory", "Count"])
            for bucket in ["2", "3", "4", "5", "6", "7+"]:
                csv_writer.writerow([bucket, usage_buckets[bucket]])

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Export complete")
    else:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] No results found")

except pyodbc.Error as e:
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Database operation failed: {str(e)}"
    )
except Exception as e:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Unexpected error: {str(e)}")
finally:
    # Close connection
    if "conn" in locals():
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Closing database connection...")
        conn.close()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Connection closed")

total_time = time.time() - globals().get("start_time", time.time())
print(
    f"[{datetime.now().strftime('%H:%M:%S')}] Script completed in {total_time:.2f} seconds"
)
