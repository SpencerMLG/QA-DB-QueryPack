import os

import pyodbc
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Build connection string
connection_string = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('DB_SERVER')};"
    f"DATABASE={os.getenv('DB_NAME')};"
    f"UID={os.getenv('DB_USER')};"
    f"PWD={os.getenv('DB_PASSWORD')};"
    f"APP={os.getenv('APP_NAME')};"
    "TrustServerCertificate=yes;"
    "Connection Timeout=5"
)

try:
    print(f"\nAttempting to connect to: {os.getenv('DB_SERVER')}")
    conn = pyodbc.connect(connection_string)
    print("Connection successful!")

    # Simple test query
    cursor = conn.cursor()
    cursor.execute("SELECT @@version")
    version = cursor.fetchone()[0]
    print(f"\nSQL Server version: {version}")

except pyodbc.Error as e:
    print(f"\nConnection failed: {str(e)}")

finally:
    if "conn" in locals():
        conn.close()
        print("Connection closed.")
