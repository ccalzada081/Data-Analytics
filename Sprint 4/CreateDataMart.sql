-- Data Mart
CREATE DATABASE DataMart;
GO

USE DataMart;
GO

-- Data Mart 1: Sales 
CREATE TABLE dbo.FactSalesMonthly (
SalesMonthlyKey INT IDENTITY(1,1) PRIMARY KEY,
ProductKey INT NOT NULL,
CategoryKey INT NULL,
Year INT NOT NULL,
Month INT NOT NULL,
MonthName VARCHAR(20) NOT NULL,
TotalRevenue DECIMAL(18,2) NOT NULL,
TotalUnitsSold INT NOT NULL,
AvgPrice DECIMAL(18,2) NOT NULL,
TotalDiscountAmount DECIMAL(18,2) NOT NULL
);
GO

-- Data Mart 2: Finance
CREATE TABLE dbo.FactFinanceMonthly (
FinanceMonthlyKey INT IDENTITY(1,1) PRIMARY KEY,
Year INT NOT NULL,
Month INT NOT NULL,
MonthName VARCHAR(20)  NOT NULL,
TotalRevenue DECIMAL(18,2) NOT NULL,
TotalPayroll DECIMAL(18,2) NOT NULL,
Profit DECIMAL(18,2) NOT NULL
);
GO
