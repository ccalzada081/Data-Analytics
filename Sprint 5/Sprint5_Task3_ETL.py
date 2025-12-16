import pyodbc
import time
from datetime import datetime

# -------------------------------------------------------------------
# 1. SQL Server connection configuration
# -------------------------------------------------------------------
SQL_SERVER = "cis444.campus-quest.com,24000"
SQL_USERNAME = "sa"
SQL_PASSWORD = "Academic2025U04!"


SQL_DATABASE = "WarehouseDB"

CONN_STR = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={SQL_DATABASE};"
    f"UID={SQL_USERNAME};"
    f"PWD={SQL_PASSWORD}"
)

def get_connection():
    return pyodbc.connect(CONN_STR)

def execute_non_query(cursor, sql, params=None):
    """Execute statement that does not return rows."""
    if params is None:
        cursor.execute(sql)
    else:
        cursor.execute(sql, params)

def execute_scalar(cursor, sql, params=None):
    """Execute statement and return a single scalar value."""
    if params is None:
        cursor.execute(sql)
    else:
        cursor.execute(sql, params)
    row = cursor.fetchone()
    return row[0] if row else None

# -------------------------------------------------------------------
# 3. ETL for DataMart
# -------------------------------------------------------------------
def load_datamart(cursor):
    """
    Rebuilds DataMart tables from WarehouseDB.
    Returns total rows inserted (for logging).
    """

    print("\nRebuilding DataMart")

    print("  - Deleting existing rows from DataMart tables...")
    execute_non_query(cursor, "DELETE FROM DataMart.dbo.FactSalesMonthly;")
    execute_non_query(cursor, "DELETE FROM DataMart.dbo.FactFinanceMonthly;")

    print("Loading FactSalesMonthly from WarehouseDB.FactSales")

    sql_insert_sales = """
        INSERT INTO DataMart.dbo.FactSalesMonthly
            (ProductKey, CategoryKey, Year, Month, MonthName,
             TotalRevenue, TotalUnitsSold, AvgPrice, TotalDiscountAmount)
        SELECT
            fs.ProductKey,
            dp.CategoryKey,
            dd.Year,
            dd.Month,
            dd.MonthName,
            SUM(fs.UnitPrice * fs.Quantity - fs.Discount)       AS TotalRevenue,
            SUM(fs.Quantity)                                     AS TotalUnitsSold,
            AVG(fs.UnitPrice)                                    AS AvgPrice,
            SUM(fs.Discount)                                     AS TotalDiscountAmount
        FROM WarehouseDB.dbo.FactSales fs
        JOIN WarehouseDB.dbo.DimDate    dd ON fs.OrderDateKey = dd.DateKey
        JOIN WarehouseDB.dbo.DimProduct dp ON fs.ProductKey   = dp.ProductKey
        GROUP BY
            fs.ProductKey,
            dp.CategoryKey,
            dd.Year,
            dd.Month,
            dd.MonthName;
    """
    execute_non_query(cursor, sql_insert_sales)

    rows_sales = execute_scalar(
        cursor,
        "SELECT COUNT(*) FROM DataMart.dbo.FactSalesMonthly;"
    )
    print(f"FactSalesMonthly rows: {rows_sales}")

    print("Loading FactFinanceMonthly from WarehouseDB.FactSales + FactSalary")

    sql_insert_finance = """
        ;WITH SalesMonthly AS (
            SELECT
                dd.Year,
                dd.Month,
                dd.MonthName,
                SUM(fs.UnitPrice * fs.Quantity - fs.Discount) AS TotalRevenue
            FROM WarehouseDB.dbo.FactSales fs
            JOIN WarehouseDB.dbo.DimDate dd ON fs.OrderDateKey = dd.DateKey
            GROUP BY dd.Year, dd.Month, dd.MonthName
        ),
        PayrollMonthly AS (
            SELECT
                dd.Year,
                dd.Month,
                dd.MonthName,
                SUM(fs.SalaryAmount) AS TotalPayroll
            FROM WarehouseDB.dbo.FactSalary fs
            JOIN WarehouseDB.dbo.DimDate dd ON fs.ValidFromDateKey = dd.DateKey
            GROUP BY dd.Year, dd.Month, dd.MonthName
        )
        INSERT INTO DataMart.dbo.FactFinanceMonthly
            (Year, Month, MonthName, TotalRevenue, TotalPayroll, Profit)
        SELECT
            COALESCE(s.Year, p.Year)              AS Year,
            COALESCE(s.Month, p.Month)            AS Month,
            COALESCE(s.MonthName, p.MonthName)    AS MonthName,
            ISNULL(s.TotalRevenue, 0)             AS TotalRevenue,
            ISNULL(p.TotalPayroll, 0)             AS TotalPayroll,
            ISNULL(s.TotalRevenue, 0) - ISNULL(p.TotalPayroll, 0) AS Profit
        FROM SalesMonthly  s
        FULL OUTER JOIN PayrollMonthly p
            ON  s.Year  = p.Year
            AND s.Month = p.Month
            AND s.MonthName = p.MonthName;
    """
    execute_non_query(cursor, sql_insert_finance)

    rows_finance = execute_scalar(
        cursor,
        "SELECT COUNT(*) FROM DataMart.dbo.FactFinanceMonthly;"
    )
    print(f"FactFinanceMonthly rows: {rows_finance}")

    total_rows = (rows_sales or 0) + (rows_finance or 0)
    print(f"DataMart loaded successfully. Total rows: {total_rows}")
    return total_rows

# -------------------------------------------------------------------
# 4. Data Governance: ETL_Runs + Validation execution
# -------------------------------------------------------------------
def start_etl_run(cursor, job_name):
    """
    Inserts a new ETL_Runs record and returns the Run_ID.
    """
    sql = """
        INSERT INTO DGDB.dbo.ETL_Runs
            (ETL_Job_Name, Start_DateTime, Status, Duration_Seconds,
             Records_Processed, Records_Rejected, Notes)
        OUTPUT INSERTED.Run_ID
        VALUES (?, GETDATE(), 'Running', NULL, NULL, NULL, NULL);
    """
    run_id = execute_scalar(cursor, sql, (job_name,))
    print(f"Started ETL run. Run_ID = {run_id}")
    return run_id

def finish_etl_run(cursor, run_id, status, duration_seconds,
                   records_processed, records_rejected, notes=None):
    """
    Updates ETL_Runs with final status and metrics.
    """
    sql = """
        UPDATE DGDB.dbo.ETL_Runs
        SET Status = ?,
            Duration_Seconds = ?,
            Records_Processed = ?,
            Records_Rejected = ?,
            Notes = ?
        WHERE Run_ID = ?;
    """
    execute_non_query(cursor, sql, (
        status,
        duration_seconds,
        records_processed,
        records_rejected,
        notes,
        run_id
    ))
    print(f"ETL run {run_id} finished with status = {status}")

def run_validations(cursor, run_id):
    """
    Executes all active validation rules and writes to Validation_Results.
    Returns (total_failed_critical, total_failed_all).
    """
    print("\nRunning data quality validations")

    sql_rules = """
        SELECT Rule_ID, Rule_Name, Severity_Level, Rule_Logic
        FROM DGDB.dbo.Validation_Rules
        WHERE Is_Active = 1 OR Is_Active IS NULL;
    """
    cursor.execute(sql_rules)
    rules = cursor.fetchall()

    total_failed_all = 0
    total_failed_critical = 0

    for rule_id, rule_name, severity, rule_logic in rules:
        print(f"  - Executing rule [{rule_id}] {rule_name} (Severity: {severity})")

        try:
            failed_count = execute_scalar(cursor, rule_logic)
            if failed_count is None:
                failed_count = 0
        except Exception as e:
            print(f"    ! Error executing rule {rule_name}: {e}")
            failed_count = -1
            rule_note = f"Rule execution error: {e}"
        else:
            rule_note = None

        sql_insert_result = """
            INSERT INTO DGDB.dbo.Validation_Results
                (Run_ID, Rule_ID, Validation_DateTime,
                 Records_Checked, Records_Passed, Records_Failed)
            VALUES (
                ?, ?, GETDATE(),
                NULL, NULL, ?
            );
        """
        execute_non_query(cursor, sql_insert_result, (run_id, rule_id, failed_count))

        if failed_count > 0:
            print(f"    > FAILED rows: {failed_count}")
            total_failed_all += failed_count
            if severity and severity.lower() == "critical":
                total_failed_critical += failed_count
        elif failed_count == 0:
            print("    > All records passed.")
        elif failed_count == -1:
            print("    > Rule could not be executed (SQL error).")

    print(f"Validation summary: {total_failed_all} failing rows in total.")
    print(f"Critical failures: {total_failed_critical}")
    return total_failed_critical, total_failed_all

# -------------------------------------------------------------------
# 5. Main ETL orchestration
# -------------------------------------------------------------------
def main():
    job_name = "DataMart_ETL_With_Governance"
    start_time = time.time()
    records_processed = 0
    records_rejected = 0
    status = "Success"
    notes = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        run_id = start_etl_run(cursor, job_name)
        conn.commit()

        records_processed = load_datamart(cursor)
        conn.commit()

        failed_critical, failed_all = run_validations(cursor, run_id)
        records_rejected = failed_all

        if failed_critical > 0:
            status = "Failed"
            notes = f"ETL completed but {failed_critical} critical validation failures were detected."
        else:
            status = "Success"
            notes = "ETL and validations completed successfully."

        duration_seconds = int(time.time() - start_time)
        finish_etl_run(
            cursor,
            run_id,
            status,
            duration_seconds,
            records_processed,
            records_rejected,
            notes
        )
        conn.commit()

    except Exception as e:
        duration_seconds = int(time.time() - start_time)
        error_text = f"Unhandled ETL error: {e}"

        print(f"\n[ERROR] {error_text}")

        try:
            conn2 = get_connection()
            cur2 = conn2.cursor()
            run_id = start_etl_run(cur2, job_name)
            finish_etl_run(
                cur2,
                run_id,
                "Failed",
                duration_seconds,
                records_processed,
                records_rejected,
                error_text
            )
            conn2.commit()
            conn2.close()
        except Exception as log_err:
            print(f"[ERROR] Could not log ETL failure to DGDB: {log_err}")

    finally:
        try:
            conn.close()
        except:
            pass

    print("\n========== ETL FINISHED ==========")
    print(f"Status           : {status}")
    print(f"Rows processed   : {records_processed}")
    print(f"Rows rejected    : {records_rejected}")
    print("==================================")

if __name__ == "__main__":
    print("Starting DataMart ETL with Data Governance...")
    main()

