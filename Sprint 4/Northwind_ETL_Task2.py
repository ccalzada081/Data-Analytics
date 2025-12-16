import pytds
from pymongo import MongoClient
from datetime import datetime, date
from decimal import Decimal

# ==================== SQL SERVER CONNECTION ====================
def sql_conn():
    return pytds.connect(
        server="cis444.campus-quest.com",
        port=24000,
        database="WarehouseDB",
        user="sa",
        password="Academic2025U04!",
        autocommit=False
    )

# ==================== MONGO CONNECTION ====================
mongo_uri = "mongodb://admin:Academic2025U04!@cis444.campus-quest.com:24010/?tls=true&tlsInsecure=true"
mongo_client = MongoClient(mongo_uri)
mongo_nw = mongo_client["northwind_mongo"]
print("Connected to MongoDB northwind_mongo!")

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
    """
    Ensure a DimDate row exists and return DateKey (int YYYYMMDD).
    Same pattern as Sakila ETL.
    """
    if dt is None:
        return None
    date_key = int(dt.strftime("%Y%m%d"))

    cursor.execute("SELECT DateKey FROM DimDate WHERE DateKey = %s", (date_key,))
    if cursor.fetchone():
        return date_key

    cursor.execute("""
        INSERT INTO DimDate (
            DateKey, FullDate, Year, Quarter, Month, Day,
            DayName, MonthName, WeekOfYear, IsWeekend
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
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
    ))
    return date_key

def fetch_key(cursor, query, params):
    cursor.execute(query, params)
    row = cursor.fetchone()
    return row[0] if row else None


def load_nw_categories(conn):
    cursor = conn.cursor()
    print("Loading DimCategory from Northwind...")


    cursor.execute("DELETE FROM DimCategory WHERE SourceSystem = %s", ("NORTHWIND",))

    for doc in mongo_nw["Categories"].find({}):
        cursor.execute("""
            INSERT INTO DimCategory (
                SourceSystem, SourceCategoryId, CategoryName, Description
            )
            VALUES (%s, %s, %s, %s)
        """, (
            "NORTHWIND",
            doc.get("CategoryID"),
            doc.get("CategoryName"),
            doc.get("Description")
        ))
    print("DimCategory loaded.")
    cursor.close()

def load_nw_suppliers(conn):
    cursor = conn.cursor()
    print("Loading DimSupplier from Northwind...")

    cursor.execute("DELETE FROM DimSupplier WHERE SourceSystem = %s", ("NORTHWIND",))

    for doc in mongo_nw["Suppliers"].find({}):
        cursor.execute("""
            INSERT INTO DimSupplier (
                SourceSystem, SourceSupplierId,
                CompanyName, ContactName, ContactTitle,
                AddressLine, CityName, Region, PostalCode, CountryName,
                Phone
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            "NORTHWIND",
            doc.get("SupplierID"),
            doc.get("CompanyName"),
            doc.get("ContactName"),
            doc.get("ContactTitle"),
            doc.get("Address"),
            doc.get("City"),
            doc.get("Region"),
            doc.get("PostalCode"),
            doc.get("Country"),
            doc.get("Phone"),
        ))
    print("DimSupplier loaded.")
    cursor.close()

def load_nw_products(conn):
    cursor = conn.cursor()
    print("Loading DimProduct from Northwind...")

    cursor.execute("DELETE FROM DimProduct WHERE SourceSystem = %s", ("NORTHWIND",))

    for doc in mongo_nw["Products"].find({}):
        category_key = fetch_key(
            cursor,
            "SELECT CategoryKey FROM DimCategory WHERE SourceSystem = %s AND SourceCategoryId = %s",
            ("NORTHWIND", doc.get("CategoryID"))
        )
        supplier_key = fetch_key(
            cursor,
            "SELECT SupplierKey FROM DimSupplier WHERE SourceSystem = %s AND SourceSupplierId = %s",
            ("NORTHWIND", doc.get("SupplierID"))
        )

        cursor.execute("""
            INSERT INTO DimProduct (
                SourceSystem, SourceProductId,
                ProductName, CategoryKey, SupplierKey,
                UnitPrice, Discontinued
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            "NORTHWIND",
            doc.get("ProductID"),
            doc.get("ProductName"),
            category_key,
            supplier_key,
            doc.get("UnitPrice"),
            1 if doc.get("Discontinued") else 0
        ))

    print("DimProduct loaded.")
    cursor.close()

def load_nw_employees(conn):
    cursor = conn.cursor()
    print("Loading DimEmployee from Northwind...")

    cursor.execute("DELETE FROM DimEmployee WHERE SourceSystem = %s", ("NORTHWIND",))

    for doc in mongo_nw["Employees"].find({}):
        birth_dt = normalize_date(doc.get("BirthDate"))
        hire_dt = normalize_date(doc.get("HireDate"))

        cursor.execute("""
            INSERT INTO DimEmployee (
                SourceSystem, SourceEmployeeId, NWEmployeeId,
                FirstName, LastName, Title,
                BirthDate, HireDate,
                Phone, Email, OrgUnitKey,
                ValidFromDateKey, ValidToDateKey, IsCurrent
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            "NORTHWIND",
            str(doc.get("EmployeeID")),
            doc.get("EmployeeID"),
            doc.get("FirstName"),
            doc.get("LastName"),
            doc.get("Title"),
            birth_dt,
            hire_dt,
            doc.get("HomePhone"),
            doc.get("PhotoPath"),
            None,
            None,
            None,
            1
        ))
    print("DimEmployee loaded.")
    cursor.close()

def load_nw_customers(conn):
    cursor = conn.cursor()
    print("Loading DimCustomer from Northwind...")

    cursor.execute("DELETE FROM DimCustomer WHERE SourceSystem = %s", ("NORTHWIND",))

    for doc in mongo_nw["Customers"].find({}):
        contact_name = doc.get("ContactName") or ""
        parts = contact_name.split(" ", 1)
        first_name = parts[0] if parts else None
        last_name = parts[1] if len(parts) > 1 else None

        cursor.execute("""
            INSERT INTO DimCustomer (
                SourceSystem, SourceCustomerId, CustomerType,
                CompanyName, FirstName, LastName,
                ContactTitle, Email, AddressKey
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            "NORTHWIND",
            doc.get("CustomerID"),
            "B2B",
            doc.get("CompanyName"),
            first_name,
            last_name,
            doc.get("ContactTitle"),
            None,
            None
        ))

    print("DimCustomer loaded.")
    cursor.close()

def load_nw_factsales(conn):
    cursor = conn.cursor()
    print("Loading FactSales from Northwind (Orders + Order Details)...")

    cursor.execute("DELETE FROM FactSales")

    orders_by_id = {}
    for o in mongo_nw["Orders"].find({}):
        orders_by_id[o["OrderID"]] = o

    for od in mongo_nw["Order Details"].find({}):
        order_id = od.get("OrderID")
        order = orders_by_id.get(order_id)
        if not order:
            continue

        order_dt = normalize_date(order.get("OrderDate"))
        order_date_key = ensure_date(cursor, order_dt) if order_dt else None

        customer_key = fetch_key(
            cursor,
            """
            SELECT CustomerKey
            FROM DimCustomer
            WHERE SourceSystem = %s AND SourceCustomerId = %s
            """,
            ("NORTHWIND", order.get("CustomerID"))
        )

        employee_key = fetch_key(
            cursor,
            """
            SELECT EmployeeKey
            FROM DimEmployee
            WHERE SourceSystem = %s AND SourceEmployeeId = %s
            """,
            ("NORTHWIND", str(order.get("EmployeeID")))
        )

        product_key = fetch_key(
            cursor,
            """
            SELECT ProductKey
            FROM DimProduct
            WHERE SourceSystem = %s AND SourceProductId = %s
            """,
            ("NORTHWIND", od.get("ProductID"))
        )

        cursor.execute("""
            INSERT INTO FactSales (
                CustomerKey, EmployeeKey, ProductKey,
                OrderDateKey, UnitPrice, Quantity, Discount
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            customer_key,
            employee_key,
            product_key,
            order_date_key,
            Decimal(str(od.get("UnitPrice"))) if od.get("UnitPrice") is not None else None,
            od.get("Quantity"),
            od.get("Discount")
        ))

    print("FactSales loaded.")
    cursor.close()

# ==================== MAIN RUNNER ====================

def run_northwind_etl():
    conn = sql_conn()
    try:
        print("Connected to SQL Server WarehouseDB")

        load_nw_categories(conn)
        load_nw_suppliers(conn)
        load_nw_products(conn)
        load_nw_employees(conn)
        load_nw_customers(conn)
        load_nw_factsales(conn)

        conn.commit()
        print("Northwind ETL completed successfully!")
    except Exception as e:
        print("ERROR during Northwind ETL:", e)
        conn.rollback()
    finally:
        conn.close()
        print("SQL connection closed")


run_northwind_etl()

