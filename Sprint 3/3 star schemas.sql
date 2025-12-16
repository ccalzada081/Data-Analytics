CREATE DATABASE Sprint3New_ERD;
GO
 
USE Sprint3_ERD;
GO CREATE TABLE DimDate (
DateKey INT PRIMARY KEY,
Year INT,
Quarter TINYINT,
Month TINYINT,
MonthName VARCHAR(20),
Day TINYINT
);

GO
CREATE TABLE DimStore (
StoreKey INT IDENTITY(1,1) PRIMARY KEY,
StoreID INT,
Address VARCHAR(100),
City VARCHAR(50),
Country VARCHAR(50)
);

GO
CREATE TABLE DimCustomer (
CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
CustomerID INT,
FirstName VARCHAR(50),
LastName VARCHAR(50),
Email VARCHAR(100)
);

GO
CREATE TABLE FactStoreRevenue (
StoreRevenueKey BIGINT IDENTITY(1,1) PRIMARY KEY,
DateKey INT NOT NULL,
StoreKey INT NOT NULL,
CustomerKey INT NOT NULL,
Amount DECIMAL(10,2),
FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
FOREIGN KEY (StoreKey) REFERENCES DimStore(StoreKey),
FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey)
);

GO
CREATE TABLE DimFilm (
FilmKey INT IDENTITY(1,1) PRIMARY KEY,
FilmID INT,
Title VARCHAR(100),
ReleaseYear INT,
Rating VARCHAR(10)
);

GO
CREATE TABLE DimStore_Rentals (
StoreKey INT IDENTITY(1,1) PRIMARY KEY,
StoreID INT,
City VARCHAR(50),
Country VARCHAR(50)
);

GO
CREATE TABLE DimInventory (
InventoryKey INT IDENTITY(1,1) PRIMARY KEY,
InventoryID INT,
FilmID INT,
StoreID INT
);

GO
CREATE TABLE FactFilmRentals (
FilmRentalKey BIGINT IDENTITY(1,1) PRIMARY KEY,
DateKey INT NOT NULL,
FilmKey INT NOT NULL,
StoreKey INT NOT NULL,
InventoryKey INT NOT NULL,
RentalDurationMinutes INT,
FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
FOREIGN KEY (FilmKey) REFERENCES DimFilm(FilmKey),
FOREIGN KEY (StoreKey) REFERENCES DimStore_Rentals(StoreKey),
FOREIGN KEY (InventoryKey) REFERENCES DimInventory(InventoryKey)
);

GO
CREATE TABLE DimActor (
ActorKey INT IDENTITY(1,1) PRIMARY KEY,
ActorID INT,
FirstName VARCHAR(50),
LastName VARCHAR(50)
);

GO
 
CREATE TABLE DimCategory (
CategoryKey INT IDENTITY(1,1) PRIMARY KEY,
CategoryID INT,
CategoryName VARCHAR(50)
);

GO
 
CREATE TABLE DimFilm_Actor (
FilmKey INT IDENTITY(1,1) PRIMARY KEY,
FilmID INT,
Title VARCHAR(100),
ReleaseYear INT
);

GO
CREATE TABLE FactActorCategory (
ActorCategoryKey BIGINT IDENTITY(1,1) PRIMARY KEY,
ActorKey INT NOT NULL,
CategoryKey INT NOT NULL,
FilmKey INT NOT NULL,
DateKey INT NOT NULL,          
FOREIGN KEY (ActorKey) REFERENCES DimActor(ActorKey),
FOREIGN KEY (CategoryKey) REFERENCES DimCategory(CategoryKey),
FOREIGN KEY (FilmKey) REFERENCES DimFilm_Actor(FilmKey),
FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey)
);
GO
