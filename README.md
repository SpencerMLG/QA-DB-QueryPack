# QA-DB-QueryPack

Collection of database query scripts for SQL Server connections.

## Scripts

### 1. Database Connection Test
Simple script that tests connection to SQL Server and displays version information.

### 2. Top 50 Records Query
Fetches the top 50 most recent records from the `UnusedDataOrder` table, ordered by `DropDate` in descending order. Exports results to CSV with detailed progress logging.

### 3. CorelogicDataId Usage Counter
Identifies CorelogicDataIds with multiple usages:
- Queries records with `ProgramTypeId = 95` after Feb 1, 2025
- Groups by CorelogicDataId with usage count > 1
- Exports results with usage counts to CSV

### 4. Usage Distribution Analysis
Advanced analysis script that:
- Queries CorelogicDataIds with multiple usages
- Creates usage distribution buckets (2, 3, 4, 5, 6, 7+)
- Exports both raw data and bucketed summary to separate CSV files
