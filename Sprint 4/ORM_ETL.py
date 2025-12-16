from sqlalchemy import create_engine, Column, Integer, String, DECIMAL
from sqlalchemy.orm import sessionmaker, declarative_base

# CONFIGURATION

sql_server = "cis444.campus-quest.com,24000"
sql_username = "sa"
sql_password = "Academic2025U04!"
sql_database = "ORM"

DATABASE_URL = (
    "mssql+pyodbc://"
    f"{sql_username}:{sql_password}@{sql_server}/{sql_database}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

engine = create_engine(DATABASE_URL)

Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()


# ORM

class FactSalesMonthly(Base):
    __tablename__ = "FactSalesMonthly"

    SalesMonthlyKey = Column(Integer, primary_key=True, autoincrement=True)
    ProductKey = Column(Integer, nullable=False)
    CategoryKey = Column(Integer, nullable=True)
    Year = Column(Integer, nullable=False)
    Month = Column(Integer, nullable=False)
    MonthName = Column(String(20), nullable=False)
    TotalRevenue = Column(DECIMAL(18, 2), nullable=False)
    TotalUnitsSold = Column(Integer, nullable=False)
    AvgPrice = Column(DECIMAL(18, 2), nullable=False)
    TotalDiscountAmount = Column(DECIMAL(18, 2), nullable=False)

    def __repr__(self):
        return f"<FactSalesMonthly(ProductKey={self.ProductKey}, Year={self.Year}, Month={self.Month})>"


class FactFinanceMonthly(Base):
    __tablename__ = "FactFinanceMonthly"

    FinanceMonthlyKey = Column(Integer, primary_key=True, autoincrement=True)
    Year = Column(Integer, nullable=False)
    Month = Column(Integer, nullable=False)
    MonthName = Column(String(20), nullable=False)
    TotalRevenue = Column(DECIMAL(18, 2), nullable=False)
    TotalPayroll = Column(DECIMAL(18, 2), nullable=False)
    Profit = Column(DECIMAL(18, 2), nullable=False)

    def __repr__(self):
        return f"<FactFinanceMonthly(Year={self.Year}, Month={self.Month}, Profit={self.Profit})>"


# CREATING TABLES

def create_tables():
    print("Creando tablas en la base ORM...")
    Base.metadata.create_all(engine)
    print("Tablas creadas correctamente.")


# RUNING CODE

if __name__ == "__main__":
    create_tables()
