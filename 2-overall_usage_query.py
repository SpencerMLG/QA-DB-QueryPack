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

    # Prepare query
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Preparing query...")
    cursor = conn.cursor()
    query = """
    SELECT CorelogicDataId, COUNT(*) as Usage
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

    if results:
        result_count = len(results)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Found {result_count} results")

        # Display sample
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Displaying sample results (first 5):"
        )
        for i, row in enumerate(results[:5]):
            print(
                f"  Row {i+1}: CorelogicDataId: {row.CorelogicDataId}, Usage: {row.Usage}"
            )
            if i >= 4:
                break

        # Export to CSV
        output_file = "corelogic_usage_results.csv"
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Exporting {result_count} results to CSV..."
        )
        start_time = time.time()

        with open(output_file, "w", newline="") as csvfile:
            csv_writer = csv.writer(csvfile)
            # Write header
            headers = [column[0] for column in cursor.description]
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] Writing headers: {', '.join(headers)}"
            )
            csv_writer.writerow(headers)

            # Write data with progress indicator for large result sets
            if result_count > 100:
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] Writing data to CSV with progress updates..."
                )
                for i, row in enumerate(results):
                    csv_writer.writerow(row)
                    if (i + 1) % 100 == 0:
                        print(
                            f"[{datetime.now().strftime('%H:%M:%S')}] Exported {i+1}/{result_count} rows ({((i+1)/result_count*100):.1f}%)"
                        )
            else:
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] Writing all {result_count} rows to CSV..."
                )
                csv_writer.writerows(results)

        export_time = time.time() - start_time
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Results exported to {output_file} (took {export_time:.2f} seconds)"
        )

        # Report file size
        file_size = os.path.getsize(output_file)
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] CSV file size: {file_size/1024:.2f} KB"
        )
    else:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] No results found for the query")

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
