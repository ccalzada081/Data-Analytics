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
mongo = mongo_client["sakila_mongo"]
print("Connected to MongoDB sakila_mongo!")


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

    cursor.execute("""
        INSERT INTO DimDate (
            DateKey, FullDate, Year, Quarter, Month, Day,
            DayName, MonthName, WeekOfYear, IsWeekend
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        date_key, dt, dt.year,
        (dt.month - 1)//3 + 1,
        dt.month, dt.day,
        dt.strftime("%A"), dt.strftime("%B"),
        int(dt.strftime("%U")),
        1 if dt.weekday() >= 5 else 0
    ))
    return date_key

def fetch_key(cursor, query, params):
    cursor.execute(query, params)
    row = cursor.fetchone()
    return row[0] if row else None

# ==================== LOAD DIMADDRESS ====================
def load_addresses(conn):
    cursor = conn.cursor()
    print("Loading DimAddress...")

    # cache countries
    countries = {c["country_id"]: c["country"] for c in mongo.country.find({})}

    # cache cities
    cities = {}
    for c in mongo.city.find({}):
        cities[c["city_id"]] = {
            "city": c["city"],
            "country_id": c["country_id"]
        }

    addr_map = {}

    for a in mongo.address.find({}):
        aid = a["address_id"]
        city_id = a.get("city_id")
        city = cities[city_id]["city"] if city_id in cities else None
        country = countries.get(cities[city_id]["country_id"]) if city_id in cities else None

        cursor.execute("""
            INSERT INTO DimAddress (
                AddressLine1, AddressLine2, City, Region, PostalCode, Country,
                SourceSystem, SourceAddressId
            )
            VALUES (%s,%s,%s,%s,%s,%s,'SAKILA',%s)
        """, (
            a.get("address"),
            a.get("address2"),
            city,
            a.get("district"),
            a.get("postal_code"),
            country,
            str(aid)
        ))

        key = fetch_key(
            cursor,
            "SELECT AddressKey FROM DimAddress WHERE SourceSystem='SAKILA' AND SourceAddressId=%s",
            (str(aid),)
        )

        addr_map[aid] = {"AddressKey": key, "Phone": a.get("phone")}

    print("DimAddress loaded.")
    return addr_map

# ==================== LOAD DIMSTOREDEPT ====================
def load_stores(conn, addr_map):
    cursor = conn.cursor()
    print("Loading DimStoreDept (stores)...")

    store_map = {}

    for s in mongo.store.find({}):
        sid = s["store_id"]
        addr_id = s.get("address_id")
        addr_key = addr_map.get(addr_id, {}).get("AddressKey")

        cursor.execute("""
            INSERT INTO DimStoreDept (
                SourceSystem, SourceOrgId, OrgType, OrgName, AddressKey
            )
            VALUES ('SAKILA', %s, 'STORE', %s, %s)
        """, (str(sid), f"Store {sid}", addr_key))

        key = fetch_key(
            cursor,
            "SELECT StoreDeptKey FROM DimStoreDept WHERE SourceSystem='SAKILA' AND SourceOrgId=%s",
            (str(sid),)
        )

        store_map[sid] = key

    print("DimStoreDept loaded.")
    return store_map


# ==================== LOAD DIMEEMPLOYEE (SAKILA STAFF) ====================
def load_staff(conn, addr_map, store_map):
    cursor = conn.cursor()
    print("Loading DimEmployee...")

    staff_map = {}

    for s in mongo.staff.find({}):
        sid = s["staff_id"]
        addr_key = addr_map.get(s.get("address_id"), {}).get("AddressKey")
        store_key = store_map.get(s.get("store_id"))

        cursor.execute("""
            INSERT INTO DimEmployee (
                SourceSystem, SourceEmployeeId,
                SakilaStaffId, FirstName, LastName, Title, Gender,
                BirthDate, HireDate, Email, Phone, Extension,
                AddressKey, OrgUnitKey,
                ValidFromDateKey, ValidToDateKey, IsCurrent
            )
            VALUES ('SAKILA', %s, %s, %s, %s, NULL, NULL,
                    NULL, NULL, %s, %s, NULL,
                    %s, %s,
                    NULL, NULL, 1)
        """, (
            f"STAFF:{sid}", sid,
            s.get("first_name"), s.get("last_name"),
            s.get("email"), None,
            addr_key, store_key
        ))

        key = fetch_key(
            cursor,
            "SELECT EmployeeKey FROM DimEmployee WHERE SourceSystem='SAKILA' AND SourceEmployeeId=%s",
            (f"STAFF:{sid}",)
        )
        staff_map[sid] = key

    print("DimEmployee loaded.")
    return staff_map

# ==================== LOAD DIMPRODUCT (FILMS) ====================
def load_films(conn):
    cursor = conn.cursor()
    print("Loading DimProduct...")

    film_map = {}

    for f in mongo.film.find({}):
        fid = f["film_id"]

        cursor.execute("""
            INSERT INTO DimProduct (
                SourceSystem, SourceProductId, ProductType,
                FilmTitle, FilmDescription, ReleaseYear, Rating
            )
            VALUES ('SAKILA', %s, 'FILM', %s, %s, %s, %s)
        """, (
            str(fid),
            f.get("title"),
            f.get("description"),
            f.get("release_year"),
            f.get("rating")
        ))

        key = fetch_key(
            cursor,
            "SELECT ProductKey FROM DimProduct WHERE SourceSystem='SAKILA' AND SourceProductId=%s",
            (str(fid),)
        )
        film_map[fid] = key

    print("DimProduct loaded.")
    return film_map

# ==================== LOAD DIMCUSTOMER (B2C) ====================
def load_customers(conn, addr_map):
    cursor = conn.cursor()
    print("Loading DimCustomer (Sakila customers)...")

    cust_map = {}

    for c in mongo.customer.find({}):
        cid = c["customer_id"]
        addr_key = addr_map.get(c.get("address_id"), {}).get("AddressKey")
        phone = addr_map.get(c.get("address_id"), {}).get("Phone")

        active = c.get("active")
        if isinstance(active, str):
            active = 1 if active == "1" else 0

        cursor.execute("""
            INSERT INTO DimCustomer (
                SourceSystem, SourceCustomerId,
                CustomerType, CompanyName,
                FirstName, LastName, ContactName, ContactTitle,
                AddressKey, Email, Phone, Fax,
                IsActive,
                ValidFromDateKey, ValidToDateKey, IsCurrent
            )
            VALUES ('SAKILA', %s, 'B2C', NULL,
                    %s, %s, %s, NULL,
                    %s, %s, %s, NULL,
                    %s,
                    NULL, NULL, 1)
        """, (
            f"CUST:{cid}",
            c.get("first_name"),
            c.get("last_name"),
            f"{c.get('first_name')} {c.get('last_name')}",
            addr_key,
            c.get("email"),
            phone,
            active
        ))

        key = fetch_key(
            cursor,
            "SELECT CustomerKey FROM DimCustomer WHERE SourceSystem='SAKILA' AND SourceCustomerId=%s",
            (f"CUST:{cid}",)
        )
        cust_map[cid] = key

    print("DimCustomer loaded.")
    return cust_map

# ==================== LOAD FACTRENTAL ====================
def load_rentals(conn, cust_map, staff_map, store_map, film_map):
    cursor = conn.cursor()
    print("Loading FactRental...")

    # payment totals
    payments = {}
    for p in mongo.payment.find({}):
        rid = p["rental_id"]
        amt = Decimal(str(p.get("amount", 0)))
        payments[rid] = payments.get(rid, Decimal("0")) + amt

    count = 0
    for r in mongo.rental.find({}):
        rid = r["rental_id"]

        rental_dt = normalize_date(r.get("rental_date"))
        return_dt = normalize_date(r.get("return_date"))

        rental_key = ensure_date(cursor, rental_dt)
        return_key = ensure_date(cursor, return_dt)

        cursor.execute("""
            INSERT INTO FactRental (
                SourceRentalId, RentalDateKey, ReturnDateKey,
                CustomerKey, EmployeeKey, StoreDeptKey, ProductKey,
                InventoryId, Amount
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            rid,
            rental_key,
            return_key,
            cust_map.get(r.get("customer_id")),
            staff_map.get(r.get("staff_id")),
            store_map.get(r.get("store_id")),
            film_map.get(r.get("inventory_id")),
            r.get("inventory_id"),
            payments.get(rid, Decimal("0"))
        ))

        count += 1

    print(f"Inserted {count} rental rows.")

# ==================== RUN FULL PIPELINE ====================
def run_sakila_etl():
    conn = sql_conn()
    try:
        print("Connected to SQL Server!")

        addr_map = load_addresses(conn)
        store_map = load_stores(conn, addr_map)
        staff_map = load_staff(conn, addr_map, store_map)
        film_map = load_films(conn)
        cust_map = load_customers(conn, addr_map)
        load_rentals(conn, cust_map, staff_map, store_map, film_map)

        conn.commit()
        print("Sakila in WarehouseDB")

    except Exception as e:
        conn.rollback()
        print("ERROR:", e)

    finally:
        conn.close()


run_sakila_etl()
