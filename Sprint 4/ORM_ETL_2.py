import pyodbc
from sqlalchemy import create_engine, Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base, sessionmaker

# =======================================
# 1) SQLAlchemy ORM CONFIGURATION
# =======================================

server = "cis444.campus-quest.com,24000"
username = "sa"
password = "Academic2025U04!"
dw_database = "WarehouseDB"
orm_database = "ORM"

engine_orm = create_engine(
    f"mssql+pyodbc://{username}:{password}@{server}/{orm_database}?driver=ODBC+Driver+17+for+SQL+Server",
    fast_executemany=True
)

Base = declarative_base()

# =======================================
# 2) ORM TABLE MODELS
# =======================================

class FactSalesMonthly(Base):
    __tablename__ = "FactSalesMonthly"

    SalesMonthlyKey = Column(Integer, primary_key=True, autoincrement=True)
    ProductKey = Column(Integer)
    CategoryKey = Column(Integer)
    Year = Column(Integer)
    Month = Column(Integer)
    MonthName = Column(String(20))
    TotalRevenue = Column(Numeric(18, 2))
    TotalUnitsSold = Column(Integer)
    AvgPrice = Column(Numeric(18, 2))
    TotalDiscountAmount = Column(Numeric(18, 2))


class FactFinanceMonthly(Base):
    __tablename__ = "FactFinanceMonthly"

    FinanceMonthlyKey = Column(Integer, primary_key=True, autoincrement=True)
    Year = Column(Integer)
    Month = Column(Integer)
    MonthName = Column(String(20))
    TotalRevenue = Column(Numeric(18, 2))
    TotalPayroll = Column(Numeric(18, 2))
    Profit = Column(Numeric(18, 2))



Base.metadata.create_all(engine_orm)

Session = sessionmaker(bind=engine_orm)
session = Session()

print("\n========== STARTING ORM ETL  ==========\n")


conn_dw_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={dw_database};"
    f"UID={username};"
    f"PWD={password}"
)
dw_conn = pyodbc.connect(conn_dw_str)
dw_cursor = dw_conn.cursor()



def robust_delete(table_class):
    before = session.query(table_class).count()
    deleted = session.query(table_class).delete()
    session.commit()
    print(f"🧹 {table_class.__tablename__}: Deleted {deleted} rows (previously {before})")

robust_delete(FactSalesMonthly)
robust_delete(FactFinanceMonthly)

print("\n--- Tables cleaned successfully. Starting ETL load... ---\n")


query_sales = """
SELECT
    dp.ProductKey,
    dp.CategoryKey,
    dd.Year,
    dd.Month,
    dd.MonthName,
    SUM(fs.UnitPrice * fs.Quantity) AS TotalRevenue,
    SUM(fs.Quantity) AS TotalUnitsSold,
    AVG(fs.UnitPrice) AS AvgPrice,
    SUM(fs.Discount) AS TotalDiscountAmount
FROM FactSales fs
JOIN DimDate dd ON fs.OrderDateKey = dd.DateKey
JOIN DimProduct dp ON fs.ProductKey = dp.ProductKey
GROUP BY dp.ProductKey, dp.CategoryKey, dd.Year, dd.Month, dd.MonthName
ORDER BY dd.Year, dd.Month;
"""

dw_cursor.execute(query_sales)
sales_rows = dw_cursor.fetchall()

print(f"Inserting {len(sales_rows)} rows into FactSalesMonthly...")

for r in sales_rows:
    session.add(FactSalesMonthly(
        ProductKey=r.ProductKey,
        CategoryKey=r.CategoryKey,
        Year=r.Year,
        Month=r.Month,
        MonthName=r.MonthName,
        TotalRevenue=r.TotalRevenue,
        TotalUnitsSold=r.TotalUnitsSold,
        AvgPrice=r.AvgPrice,
        TotalDiscountAmount=r.TotalDiscountAmount
    ))

session.commit()
print("FactSalesMonthly loaded.\n")

# =======================================
# 7) FINANCE MONTHLY QUERY (FULL CALENDAR)
# =======================================

query_finance = """
SELECT
    dd.Year,
    dd.Month,
    dd.MonthName,
    ISNULL(SUM(fs.UnitPrice * fs.Quantity), 0) AS TotalRevenue,
    ISNULL(SUM(sal.SalaryAmount), 0) AS TotalPayroll,
    ISNULL(SUM(fs.UnitPrice * fs.Quantity), 0)
        - ISNULL(SUM(sal.SalaryAmount), 0) AS Profit
FROM DimDate dd
LEFT JOIN FactSales fs ON fs.OrderDateKey = dd.DateKey
LEFT JOIN FactSalary sal ON sal.FromDateKey = dd.DateKey
GROUP BY dd.Year, dd.Month, dd.MonthName
ORDER BY dd.Year, dd.Month;
"""

dw_cursor.execute(query_finance)
finance_rows = dw_cursor.fetchall()

print(f"Inserting {len(finance_rows)} rows into FactFinanceMonthly...")

for r in finance_rows:
    session.add(FactFinanceMonthly(
        Year=r.Year,
        Month=r.Month,
        MonthName=r.MonthName,
        TotalRevenue=r.TotalRevenue,
        TotalPayroll=r.TotalPayroll,
        Profit=r.Profit
    ))

session.commit()
print("FactFinanceMonthly loaded.\n")

# =======================================
# 8) FINAL CONFIRMATION
# =======================================

count_sales = session.query(FactSalesMonthly).count()
count_finance = session.query(FactFinanceMonthly).count()

print("========== ORM ETL COMPLETED  ==========")
print(f"FactSalesMonthly → {count_sales}")
print(f"FactFinanceMonthly → {count_finance}")
print("====================================================\n")
