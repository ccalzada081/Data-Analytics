import pyodbc
from pymongo import MongoClient
from datetime import datetime
import time

# ================================================================
#  SQL RECONNECT
# ================================================================
def reconnect_sql():
    global sql_conn, sql_cursor
    print("Trying to reconnect to SQL...")
    try:
        sql_conn.close()
    except:
        pass
    sql_conn = pyodbc.connect(conn_str, autocommit=False)
    sql_cursor = sql_conn.cursor()
    print("Reconnection")

def safe_execute(query, params=None, retries=3):
    for _ in range(retries):
        try:
            if params:
                sql_cursor.execute(query, params)
            else:
                sql_cursor.execute(query)
            return
        except pyodbc.OperationalError as e:
            if "10054" in str(e):
                reconnect_sql()
                time.sleep(0.1)
                continue
            raise
    raise Exception("Error in SQL")


# ================================================================
#  CCONNECTION
# ================================================================
sql_server = "cis444.campus-quest.com,24000"
sql_database = "Sprint3New_ERD"
sql_username = "sa"
sql_password = "Academic2025U04!"

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={sql_server};DATABASE={sql_database};"
    f"UID={sql_username};PWD={sql_password}"
)

sql_conn = pyodbc.connect(conn_str, autocommit=False)
sql_cursor = sql_conn.cursor()
print(f"SQL conectado a base: {sql_database}")

mongo_client = MongoClient(
    "mongodb://admin:Academic2025U04!@cis444.campus-quest.com:24010/"
    "?tls=true&tlsAllowInvalidCertificates=true"
)
mongo_db = mongo_client["sakila_mongo"]
print("Mongo connected")

# ================================================================
#  DELETE + RESEED
# ================================================================
tables_identity = [
    "FactStoreRevenue","FactFilmRentals","FactActorCategory",
    "DimFilm_Actor","DimCategory","DimActor","DimInventory",
    "DimFilm","DimCustomer","DimStore_Rentals","DimStore"
]
tables_no_identity = ["DimDate"]

for t in tables_identity + tables_no_identity:
    safe_execute(f"DELETE FROM {t}")

for t in tables_identity:
    safe_execute(f"DBCC CHECKIDENT ('{t}', RESEED, 0)")

sql_conn.commit()
print("All tables cleaned")


# ================================================================
#  Loadiong
# ================================================================
print("Loading...")

actors = list(mongo_db["actor"].find())
addresses = list(mongo_db["address"].find())
cities = list(mongo_db["city"].find())
countries = list(mongo_db["country"].find())
customers = list(mongo_db["customer"].find())
films = list(mongo_db["film"].find())
film_actor = list(mongo_db["film_actor"].find())
film_category = list(mongo_db["film_category"].find())
inventory = list(mongo_db["inventory"].find())
payments = list(mongo_db["payment"].find())
rentals = list(mongo_db["rental"].find())
staff = list(mongo_db["staff"].find())
stores = list(mongo_db["store"].find())

payments_by_id = {p["payment_id"]: p for p in payments}
rentals_by_id = {r["rental_id"]: r for r in rentals}
staff_by_id = {s["staff_id"]: s for s in staff}
inventory_by_id = {i["inventory_id"]: i for i in inventory}
films_by_id = {f["film_id"]: f for f in films}
film_actor_by_film = {}

for fa in film_actor:
    film_actor_by_film.setdefault(fa["film_id"], []).append(fa)

film_category_by_film = {}

for fc in film_category:
    film_category_by_film.setdefault(fc["film_id"], []).append(fc)

address_by_id = {a["address_id"]: a for a in addresses}
city_by_id = {c["city_id"]: c for c in cities}
country_by_id = {c["country_id"]: c for c in countries}

print("Everything load")


# ================================================================
#  DIMDATE
# ================================================================
date_cache = set()
def insert_dim_date(date_obj):
    date_key = int(date_obj.strftime("%Y%m%d"))
    if date_key in date_cache:
        return date_key

    safe_execute("""
        INSERT INTO DimDate (DateKey, Year, Quarter, Month, MonthName, Day)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        date_key,
        date_obj.year,
        (date_obj.month - 1)//3 + 1,
        date_obj.month,
        date_obj.strftime("%B"),
        date_obj.day
    ))
    date_cache.add(date_key)
    return date_key


# ================================================================
#  DIMSTORE + DIMSTORE_RENTALS
# ================================================================
for s in stores:
    store_id = s["store_id"]
    addr = address_by_id.get(s["address_id"])
    city = city_by_id.get(addr["city_id"]) if addr else None
    country = country_by_id.get(city["country_id"]) if city else None

    safe_execute("""
        INSERT INTO DimStore (StoreID, Address, City, Country)
        VALUES (?, ?, ?, ?)
    """, (
        store_id,
        addr["address"] if addr else None,
        city["city"] if city else None,
        country["country"] if country else None
    ))

    safe_execute("""
        INSERT INTO DimStore_Rentals (StoreID, City, Country)
        VALUES (?, ?, ?)
    """, (
        store_id,
        city["city"] if city else None,
        country["country"] if country else None
    ))

sql_conn.commit()

# build lookups
store_lookup = {}
sql_cursor.execute("SELECT StoreKey, StoreID FROM DimStore")
for k, sid in sql_cursor.fetchall():
    store_lookup[sid] = k

store_rent_lookup = {}
sql_cursor.execute("SELECT StoreKey, StoreID FROM DimStore_Rentals")
for k, sid in sql_cursor.fetchall():
    store_rent_lookup[sid] = k

print("DimStore y DimStore_Rentals loaded")


# ================================================================
#  DIMCUSTOMER
# ================================================================
for c in customers:
    safe_execute("""
        INSERT INTO DimCustomer (CustomerID, FirstName, LastName, Email)
        VALUES (?, ?, ?, ?)
    """, (c["customer_id"], c["first_name"], c["last_name"], c["email"]))

sql_conn.commit()
customer_lookup = {}
sql_cursor.execute("SELECT CustomerKey, CustomerID FROM DimCustomer")
for key, cid in sql_cursor.fetchall():
    customer_lookup[cid] = key

print("DimCustomer loaded")


# ================================================================
#  DIMFILM
# ================================================================
for f in films:
    safe_execute("""
        INSERT INTO DimFilm (FilmID, Title, ReleaseYear, Rating)
        VALUES (?, ?, ?, ?)
    """, (f["film_id"], f["title"], f["release_year"], f["rating"]))

sql_conn.commit()
film_lookup = {}
sql_cursor.execute("SELECT FilmKey, FilmID FROM DimFilm")
for k, fid in sql_cursor.fetchall():
    film_lookup[fid] = k

print("DimFilm loaded")


# ================================================================
#  DIMINVENTORY
# ================================================================
for inv in inventory:
    safe_execute("""
        INSERT INTO DimInventory (InventoryID, FilmID, StoreID)
        VALUES (?, ?, ?)
    """, (inv["inventory_id"], inv["film_id"], inv["store_id"]))

sql_conn.commit()
inventory_lookup = {}
sql_cursor.execute("SELECT InventoryKey, InventoryID FROM DimInventory")
for k, iid in sql_cursor.fetchall():
    inventory_lookup[iid] = k

print("DimInventory loaded")


# ================================================================
#  DIMACTOR
# ================================================================
for a in actors:
    safe_execute("""
        INSERT INTO DimActor (ActorID, FirstName, LastName)
        VALUES (?, ?, ?)
    """, (a["actor_id"], a["first_name"], a["last_name"]))

sql_conn.commit()
actor_lookup = {}
sql_cursor.execute("SELECT ActorKey, ActorID FROM DimActor")
for k, aid in sql_cursor.fetchall():
    actor_lookup[aid] = k

print("DimActor loaded")


# ================================================================
#  DIMCATEGORY
# ================================================================
for c in mongo_db["category"].find():
    safe_execute("""
        INSERT INTO DimCategory (CategoryID, CategoryName)
        VALUES (?, ?)
    """, (c["category_id"], c["name"]))

sql_conn.commit()
category_lookup = {}
sql_cursor.execute("SELECT CategoryKey, CategoryID FROM DimCategory")
for k, cid in sql_cursor.fetchall():
    category_lookup[cid] = k

print("DimCategory loaded")


# ================================================================
#  DIMFILM_ACTOR
# ================================================================
for f in films:
    safe_execute("""
        INSERT INTO DimFilm_Actor (FilmID, Title, ReleaseYear)
        VALUES (?, ?, ?)
    """, (f["film_id"], f["title"], f["release_year"]))

sql_conn.commit()
film_actor_lookup = {}
sql_cursor.execute("SELECT FilmKey, FilmID FROM DimFilm_Actor")
for k, fid in sql_cursor.fetchall():
    film_actor_lookup[fid] = k

print("DimFilm_Actor loaded")


# ================================================================
#  FACTSTORE_REVENUE
# ================================================================
fact_rows = []

for p in payments:
    date_key = insert_dim_date(p["payment_date"])
    staff_doc = staff_by_id[p["staff_id"]]
    store_key = store_lookup[staff_doc["store_id"]]
    customer_key = customer_lookup[p["customer_id"]]
    fact_rows.append((date_key, store_key, customer_key, p["amount"]))

sql_cursor.executemany("""
    INSERT INTO FactStoreRevenue (DateKey, StoreKey, CustomerKey, Amount)
    VALUES (?, ?, ?, ?)
""", fact_rows)
sql_conn.commit()

print("FactStoreRevenue loaded")


# ================================================================
#  FACTFILMRENTALS
# ================================================================
fact_rows = []

for p in payments:
    rental = rentals_by_id.get(p["rental_id"])
    if not rental:
        continue

    inv = inventory_by_id.get(rental["inventory_id"])
    if not inv:
        continue

    film_key = film_lookup.get(inv["film_id"])
    if not film_key:
        continue

    store_key = store_rent_lookup[inv["store_id"]]
    inv_key = inventory_lookup[inv["inventory_id"]]
    date_key = insert_dim_date(p["payment_date"])

    fact_rows.append(
        (date_key, film_key, store_key, inv_key, 120)
    )

sql_cursor.executemany("""
    INSERT INTO FactFilmRentals (DateKey, FilmKey, StoreKey, InventoryKey, RentalDurationMinutes)
    VALUES (?, ?, ?, ?, ?)
""", fact_rows)
sql_conn.commit()

print("FactFilmRentals loaded")


# ================================================================
#  FACTACTORCATEGORY
# ================================================================
fact_rows = []
fixed_date = insert_dim_date(datetime(2005,1,1))

for f in films:
    fid = f["film_id"]
    film_key = film_actor_lookup[fid]

    categories = film_category_by_film.get(fid, [])
    actors = film_actor_by_film.get(fid, [])

    for cat in categories:
        category_key = category_lookup[cat["category_id"]]

        for ac in actors:
            actor_key = actor_lookup[ac["actor_id"]]

            fact_rows.append((actor_key, category_key, film_key, fixed_date))

sql_cursor.executemany("""
    INSERT INTO FactActorCategory (ActorKey, CategoryKey, FilmKey, DateKey)
    VALUES (?, ?, ?, ?)
""", fact_rows)
sql_conn.commit()

print("FactActorCategory loaded")

print("\n===== ETL COMPLETE =====")
