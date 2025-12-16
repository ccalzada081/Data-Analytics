import pytds
from pymongo import MongoClient
from datetime import datetime, date
from decimal import Decimal

# ============================================================
# SQL CONNECTION
# ============================================================
def sql_conn():
    return pytds.connect(
        server="cis444.campus-quest.com",
        port=24000,
        database="WarehouseDB",
        user="sa",
        password="Academic2025U04!",
        autocommit=False
    )

# ============================================================
# MONGO CONNECTION
# ============================================================
mongo_uri = (
    "mongodb://admin:Academic2025U04!@cis444.campus-quest.com:24010/"
    "?tls=true&tlsInsecure=true"
)
mongo_client = MongoClient(mongo_uri)
mongo_emp = mongo_client["employees_mongo"]
print("Connected to MongoDB employees_mongo!")


def normalize_date(d):
    if d is None:
        return None
    if isinstance(d, dict) and "$date" in d:
        d = d["$date"]
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    try:
        return datetime.fromisoformat(str(d).replace("Z", "+00:00")).date()
    except:
        return None

def ensure_date(cursor, dt):
    if dt is None:
        return None
    date_key = int(dt.strftime("%Y%m%d"))

    cursor.execute("SELECT DateKey FROM DimDate WHERE DateKey=%s", (date_key,))
    if cursor.fetchone():
        return date_key

    cursor.execute(
        """
        INSERT INTO DimDate (
            DateKey, FullDate, Year, Quarter, Month, Day,
            DayName, MonthName, WeekOfYear, IsWeekend
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            date_key,
            dt,
            dt.year,
            (dt.month - 1)//3 + 1,
            dt.month,
            dt.day,
            dt.strftime("%A"),
            dt.strftime("%B"),
            int(dt.strftime("%U")),
            1 if dt.weekday() >= 5 else 0
        )
    )
    return date_key

def fetch_key(cursor, query, params):
    cursor.execute(query, params)
    row = cursor.fetchone()
    return row[0] if row else None


# ============================================================
# LOAD FUNCTIONS
# ============================================================

# ----------------------------
# 1. DimDepartment
# ----------------------------
def load_departments(conn):
    cursor = conn.cursor()
    print("Loading DimDepartment...")

    count = 0
    for doc in mongo_emp["department"].find({}).limit(1000):
        cursor.execute(
            """
            INSERT INTO DimDepartment (
                SourceSystem, SourceDepartmentId, DepartmentName
            )
            VALUES (%s, %s, %s)
            """,
            ("EMPLOYEES", doc.get("dept_no"), doc.get("dept_name"))
        )
        count += 1

    cursor.close()
    print(f"DimDepartment inserted: {count}")


# ----------------------------
# 2. DimEmployee
# ----------------------------
def load_employees(conn):
    cursor = conn.cursor()
    print("Loading DimEmployee... with limiting 1000 because it was crashing )")

    count = 0
    for doc in mongo_emp["employee"].find({}).limit(1000):

        emp_no = str(doc.get("emp_no"))
        birth_dt = normalize_date(doc.get("birth_date"))
        hire_dt = normalize_date(doc.get("hire_date"))
        hire_key = ensure_date(cursor, hire_dt) if hire_dt else None

        cursor.execute(
            """
            INSERT INTO DimEmployee (
                SourceSystem, SourceEmployeeId, EmpNo,
                FirstName, LastName, Title,
                Gender, BirthDate, HireDate,
                Email, Phone, Extension,
                AddressKey, ManagerKey, OrgUnitKey,
                ValidFromDateKey, ValidToDateKey, IsCurrent,
                NWEmployeeId
            )
            VALUES (%s, %s, %s,
                    %s, %s, NULL,
                    %s, %s, %s,
                    NULL, NULL, NULL,
                    NULL, NULL, NULL,
                    %s, NULL, 1,
                    NULL)
            """,
            (
                "EMPLOYEES", emp_no, emp_no,
                doc.get("first_name"),
                doc.get("last_name"),
                doc.get("gender"),
                birth_dt,
                hire_dt,
                hire_key
            )
        )

        count += 1

    cursor.close()
    print(f"DimEmployee inserted: {count}")


# ----------------------------
# 3. FactSalary
# ----------------------------
def load_salary_fact(conn):
    cursor = conn.cursor()
    print("Loading FactSalary...")

    count = 0
    for doc in mongo_emp["salary"].find({}).limit(1000):

        emp_no = str(doc.get("emp_no"))
        emp_key = fetch_key(
            cursor,
            "SELECT EmployeeKey FROM DimEmployee WHERE SourceSystem=%s AND SourceEmployeeId=%s",
            ("EMPLOYEES", emp_no)
        )

        if not emp_key:
            continue

        from_dt = normalize_date(doc.get("from_date"))
        to_dt = normalize_date(doc.get("to_date"))

        cursor.execute(
            """
            INSERT INTO FactSalary (
                EmployeeKey, SalaryAmount,
                ValidFromDateKey, ValidToDateKey,
                SourceSystem, SourceEmpNo
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                emp_key,
                Decimal(str(doc.get("amount"))),
                ensure_date(cursor, from_dt),
                ensure_date(cursor, to_dt),
                "EMPLOYEES",
                emp_no
            )
        )
        count += 1

    cursor.close()
    print(f"FactSalary inserted: {count}")


# ----------------------------
# 4. FactEmployeeTitles
# ----------------------------
def load_titles_fact(conn):
    cursor = conn.cursor()
    print("Loading FactEmployeeTitles...")

    count = 0
    for doc in mongo_emp["title"].find({}).limit(1000):

        emp_no = str(doc.get("emp_no"))
        emp_key = fetch_key(
            cursor,
            "SELECT EmployeeKey FROM DimEmployee WHERE SourceSystem=%s AND SourceEmployeeId=%s",
            ("EMPLOYEES", emp_no)
        )

        if not emp_key:
            continue

        from_dt = normalize_date(doc.get("from_date"))
        to_dt = normalize_date(doc.get("to_date"))

        cursor.execute(
            """
            INSERT INTO FactEmployeeTitles (
                EmployeeKey, Title,
                ValidFromDateKey, ValidToDateKey,
                SourceSystem
            )
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                emp_key,
                doc.get("title"),
                ensure_date(cursor, from_dt),
                ensure_date(cursor, to_dt),
                "EMPLOYEES"
            )
        )
        count += 1

    cursor.close()
    print(f"FactEmployeeTitles inserted: {count}")


# ----------------------------
# 5. FactDeptAssignment
# ----------------------------
def load_dept_assignment_fact(conn):
    cursor = conn.cursor()
    print("Loading FactDeptAssignment...")

    count = 0
    for doc in mongo_emp["dept_emp"].find({}).limit(1000):

        emp_no = str(doc.get("emp_no"))
        dept_no = doc.get("dept_no")

        emp_key = fetch_key(
            cursor,
            "SELECT EmployeeKey FROM DimEmployee WHERE SourceSystem=%s AND SourceEmployeeId=%s",
            ("EMPLOYEES", emp_no)
        )

        dept_key = fetch_key(
            cursor,
            "SELECT DepartmentKey FROM DimDepartment WHERE SourceSystem=%s AND SourceDepartmentId=%s",
            ("EMPLOYEES", dept_no)
        )

        if not emp_key or not dept_key:
            continue

        from_dt = normalize_date(doc.get("from_date"))
        to_dt = normalize_date(doc.get("to_date"))


        cursor.execute(
            """
            INSERT INTO FactDeptAssignment (
                EmployeeKey, DepartmentKey,
                ValidFromDateKey, ValidToDateKey,
                SourceSystem
            )
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                emp_key,
                dept_key,
                ensure_date(cursor, from_dt),
                ensure_date(cursor, to_dt),
                "EMPLOYEES"
            )
        )

        count += 1

    cursor.close()
    print(f"FactDeptAssignment inserted: {count}")


# ----------------------------
# 6. FactDeptManager
# ----------------------------
def load_dept_manager_fact(conn):
    cursor = conn.cursor()
    print("Loading FactDeptManager...")

    count = 0
    for doc in mongo_emp["dept_manager"].find({}).limit(1000):

        emp_no = str(doc.get("emp_no"))
        dept_no = doc.get("dept_no")

        emp_key = fetch_key(
            cursor,
            "SELECT EmployeeKey FROM DimEmployee WHERE SourceSystem=%s AND SourceEmployeeId=%s",
            ("EMPLOYEES", emp_no)
        )

        dept_key = fetch_key(
            cursor,
            "SELECT DepartmentKey FROM DimDepartment WHERE SourceSystem=%s AND SourceDepartmentId=%s",
            ("EMPLOYEES", dept_no)
        )

        if not emp_key or not dept_key:
            continue

        from_dt = normalize_date(doc.get("from_date"))
        to_dt = normalize_date(doc.get("to_date"))

        cursor.execute(
            """
            INSERT INTO FactDeptManager (
                EmployeeKey, DepartmentKey,
                ValidFromDateKey, ValidToDateKey,
                SourceSystem, SourceEmpNo
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                emp_key,
                dept_key,
                ensure_date(cursor, from_dt),
                ensure_date(cursor, to_dt),
                "EMPLOYEES",
                emp_no
            )
        )

        count += 1

    cursor.close()
    print(f"FactDeptManager inserted: {count}")


# ============================================================
# EXECUTION
# ============================================================
def run_employees_etl():
    conn = sql_conn()
    try:
        print("Connected to SQL Server WarehouseDB (Employees ETL)")

        load_departments(conn)
        load_employees(conn)
        load_salary_fact(conn)
        load_titles_fact(conn)
        load_dept_assignment_fact(conn)
        load_dept_manager_fact(conn)

        conn.commit()
        print("Employees ETL completed")

    except Exception as e:
        print("ERROR during Employees ETL:", e)
        conn.rollback()

    finally:
        conn.close()
        print("SQL connection closed.")


run_employees_etl()

