-- DELETE FactSalesMonthly
DELETE FROM dbo.FactSalesMonthly;

INSERT INTO dbo.FactSalesMonthly (
ProductKey,
CategoryKey,
Year,
Month,
MonthName,
TotalRevenue,
TotalUnitsSold,
AvgPrice,
TotalDiscountAmount
)
SELECT
p.ProductKey,
p.CategoryKey,
d.Year,
d.Month,
d.MonthName,
SUM(fs.UnitPrice * fs.Quantity * (1 - fs.Discount)) AS TotalRevenue,
SUM(fs.Quantity) AS TotalUnitsSold,
AVG(fs.UnitPrice) AS AvgPrice,
SUM(fs.UnitPrice * fs.Quantity * fs.Discount) AS TotalDiscountAmount
FROM WarehouseDB.dbo.FactSales fs
JOIN WarehouseDB.dbo.DimDate d ON fs.OrderDateKey = d.DateKey
JOIN WarehouseDB.dbo.DimProduct p ON fs.ProductKey = p.ProductKey
LEFT JOIN WarehouseDB.dbo.DimCategory c ON p.CategoryKey = c.CategoryKey
GROUP BY
p.ProductKey,
p.CategoryKey,
d.Year,
d.Month,
d.MonthName;



-- DELETE FactFinanceMonthly
DELETE FROM dbo.FactFinanceMonthly;

WITH SalesByMonth AS (
SELECT
d.Year,
d.Month,
d.MonthName,
SUM(fs.UnitPrice * fs.Quantity * (1 - fs.Discount)) AS TotalRevenue
FROM WarehouseDB.dbo.FactSales fs
JOIN WarehouseDB.dbo.DimDate d ON fs.OrderDateKey = d.DateKey
GROUP BY
d.Year,
d.Month,
d.MonthName
),
PayrollByMonth AS (
SELECT
d.Year,
d.Month,
d.MonthName,
SUM(sal.SalaryAmount) AS TotalPayroll
FROM WarehouseDB.dbo.FactSalary sal
JOIN WarehouseDB.dbo.DimDate d ON sal.ValidFromDateKey = d.DateKey
GROUP BY
d.Year,
d.Month,
d.MonthName
)
INSERT INTO dbo.FactFinanceMonthly (
Year,
Month,
MonthName,
TotalRevenue,
TotalPayroll,
Profit
)
SELECT
COALESCE(s.Year, p.Year) AS Year,
COALESCE(s.Month, p.Month) AS Month,
COALESCE(s.MonthName, p.MonthName) AS MonthName,
ISNULL(s.TotalRevenue, 0) AS TotalRevenue,
ISNULL(p.TotalPayroll, 0) AS TotalPayroll,
ISNULL(s.TotalRevenue, 0) - ISNULL(p.TotalPayroll, 0) AS Profit
FROM SalesByMonth s
FULL OUTER JOIN PayrollByMonth p ON s.Year = p.Year AND s.Month = p.Month;


-- TEST
SELECT COUNT(*) AS RowsSalesMart FROM DataMart.dbo.FactSalesMonthly;
SELECT COUNT(*) AS RowsFinanceMart FROM DataMart.dbo.FactFinanceMonthly;

SELECT TOP 10 *
FROM DataMart.dbo.FactSalesMonthly
ORDER BY Year, Month, ProductKey;

SELECT TOP 10 *
FROM DataMart.dbo.FactFinanceMonthly
ORDER BY Year, Month;
