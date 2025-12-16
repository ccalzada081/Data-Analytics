import pyodbc
from pymongo import MongoClient
from decimal import Decimal
from datetime import datetime, date

# =================== SQL SERVER CONNECTION ===================
sql_server = "cis444.campus-quest.com,24000"
sql_database = "Employees"
sql_username = "sa"
sql_password = "Academic2025U04!"

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={sql_server};"
    f"DATABASE={sql_database};"
    f"UID={sql_username};"
    f"PWD={sql_password}"
)

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
    tables = [row[0] for row in cursor.fetchall()]
    print("Connected to SQL Server! Tables found:", tables)
except Exception as e:
    print("Error connecting to SQL Server:", e)
    exit(1)

# =================== MONGODB CONNECTION ===================
mongo_uri = "mongodb://admin:Academic2025U04!@cis444.campus-quest.com:24010/?tls=true&tlsInsecure=true"
mongo_client = MongoClient(mongo_uri)
mongo_db = mongo_client["employees_mongo"]
print("Connected to MongoDB!")

# =================== TRANSFER TABLES ===================
for table in tables:
    if table.lower() == "sysdiagrams":
        print(f"\nSkipping table: {table} (not needed)")
        continue

    try:
        print(f"\nTransferring table: {table} ...")

        cursor.execute(f"SELECT * FROM [employee].[{table}]")

        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()

        documents = []
        for row in rows:
            doc = {}
            for col, val in zip(columns, row):
                if isinstance(val, bytes):
                    val = val.decode(errors='ignore')
                elif isinstance(val, Decimal):
                    val = float(val)
                elif isinstance(val, date):
                    val = datetime.combine(val, datetime.min.time())
                doc[col] = val
            documents.append(doc)

        mongo_collection = mongo_db[table]
        mongo_collection.delete_many({})
        if documents:
            mongo_collection.insert_many(documents)
            print(f"Inserted {len(documents)} rows into {table}")
        else:
            print(f"No data in {table}, skipped insertion")

    except Exception as e:
        print(f"Error transferring table {table}: {e}")

print("\nMigration completed")
