INSERT INTO DGDB.dbo.Validation_Rules
    (Rule_Name, Rule_Description, Rule_Category, Severity_Level, Rule_Logic)
VALUES
(
    'Customer_Address_Exists',
    'Ensures every customer has a valid address reference in DimAddress.',
    'Referential Integrity',
    'Critical',
    'SELECT COUNT(*) 
     FROM WarehouseDB.dbo.DimCustomer c
     LEFT JOIN WarehouseDB.dbo.DimAddress a ON c.AddressKey = a.AddressKey
     WHERE a.AddressKey IS NULL;'
);

INSERT INTO DGDB.dbo.Validation_Rules
    (Rule_Name, Rule_Description, Rule_Category, Severity_Level, Rule_Logic)
VALUES
(
    'Product_Supplier_Exists',
    'Ensures each product references an existing supplier.',
    'Referential Integrity',
    'Critical',
    'SELECT COUNT(*)
     FROM WarehouseDB.dbo.DimProduct p
     LEFT JOIN WarehouseDB.dbo.DimSupplier s ON p.SupplierKey = s.SupplierKey
     WHERE s.SupplierKey IS NULL;'
);

INSERT INTO DGDB.dbo.Validation_Rules
    (Rule_Name, Rule_Description, Rule_Category, Severity_Level, Rule_Logic)
VALUES
(
    'Product_Category_Exists',
    'Ensures each product has a valid category in DimCategory.',
    'Referential Integrity',
    'Critical',
    'SELECT COUNT(*)
     FROM WarehouseDB.dbo.DimProduct p
     LEFT JOIN WarehouseDB.dbo.DimCategory c ON p.CategoryKey = c.CategoryKey
     WHERE c.CategoryKey IS NULL;'
);

INSERT INTO DGDB.dbo.Validation_Rules
    (Rule_Name, Rule_Description, Rule_Category, Severity_Level, Rule_Logic)
VALUES
(
    'FactSales_Dimension_Integrity',
    'Ensures all FactSales rows reference existing Product, Customer, Employee, and Date dimensions.',
    'Referential Integrity',
    'Critical',
    'SELECT COUNT(*)
     FROM WarehouseDB.dbo.FactSales fs
     LEFT JOIN WarehouseDB.dbo.DimProduct p ON fs.ProductKey = p.ProductKey
     LEFT JOIN WarehouseDB.dbo.DimCustomer c ON fs.CustomerKey = c.CustomerKey
     LEFT JOIN WarehouseDB.dbo.DimEmployee e ON fs.EmployeeKey = e.EmployeeKey
     LEFT JOIN WarehouseDB.dbo.DimDate d ON fs.OrderDateKey = d.DateKey
     WHERE p.ProductKey IS NULL 
        OR c.CustomerKey IS NULL
        OR e.EmployeeKey IS NULL
        OR d.DateKey IS NULL;'
);

INSERT INTO DGDB.dbo.Validation_Rules
    (Rule_Name, Rule_Description, Rule_Category, Severity_Level, Rule_Logic)
VALUES
(
    'FactSales_Positive_Values',
    'Ensures quantities, unit prices, and discounts in FactSales are non-negative.',
    'Business Logic',
    'Critical',
    'SELECT COUNT(*)
     FROM WarehouseDB.dbo.FactSales
     WHERE Quantity < 0 OR UnitPrice < 0 OR Discount < 0;'
);

INSERT INTO DGDB.dbo.Validation_Rules
    (Rule_Name, Rule_Description, Rule_Category, Severity_Level, Rule_Logic)
VALUES
(
    'FactSalary_Valid_DateRange',
    'Ensures FromDateKey is earlier than or equal to ToDateKey in FactSalary.',
    'Business Logic',
    'Critical',
    'SELECT COUNT(*)
     FROM WarehouseDB.dbo.FactSalary
     WHERE FromDateKey > ToDateKey;'
);

INSERT INTO DGDB.dbo.Validation_Rules
    (Rule_Name, Rule_Description, Rule_Category, Severity_Level, Rule_Logic)
VALUES
(
    'Employee_Address_Exists',
    'Ensures every employee has a valid address reference in DimAddress.',
    'Referential Integrity',
    'Critical',
    'SELECT COUNT(*)
     FROM WarehouseDB.dbo.DimEmployee e
     LEFT JOIN WarehouseDB.dbo.DimAddress a ON e.AddressKey = a.AddressKey
     WHERE a.AddressKey IS NULL;'
);

INSERT INTO DGDB.dbo.Validation_Rules
    (Rule_Name, Rule_Description, Rule_Category, Severity_Level, Rule_Logic)
VALUES
(
    'Customer_Email_Format',
    'Ensures customer email addresses follow a valid email pattern.',
    'Format Validation',
    'Warning',
    'SELECT COUNT(*)
     FROM WarehouseDB.dbo.DimCustomer
     WHERE Email NOT LIKE ''%@%.%'';'
);

INSERT INTO DGDB.dbo.Validation_Rules
    (Rule_Name, Rule_Description, Rule_Category, Severity_Level, Rule_Logic)
VALUES
(
    'EmployeeKey_No_Duplicates',
    'Ensures EmployeeKey values in DimEmployee are unique.',
    'Uniqueness',
    'Critical',
    'SELECT COUNT(*)
     FROM (
         SELECT EmployeeKey
         FROM WarehouseDB.dbo.DimEmployee
         GROUP BY EmployeeKey
         HAVING COUNT(*) > 1
     ) x;'
);

INSERT INTO DGDB.dbo.Validation_Rules
    (Rule_Name, Rule_Description, Rule_Category, Severity_Level, Rule_Logic)
VALUES
(
    'Customer_SourceSystem_Valid',
    'Ensures SourceSystem for each customer is one of the expected values.',
    'Merger Logic',
    'Critical',
    'SELECT COUNT(*)
     FROM WarehouseDB.dbo.DimCustomer
     WHERE SourceSystem NOT IN (''Sakila'', ''Northwind'', ''Employees'');'
);

INSERT INTO DGDB.dbo.Validation_Rules
    (Rule_Name, Rule_Description, Rule_Category, Severity_Level, Rule_Logic)
VALUES
(
    'Product_Supplier_Source_Match',
    'Ensures Product and Supplier share the same SourceSystem.',
    'Merger Logic',
    'Warning',
    'SELECT COUNT(*)
     FROM WarehouseDB.dbo.DimProduct p
     JOIN WarehouseDB.dbo.DimSupplier s ON p.SupplierKey = s.SupplierKey
     WHERE p.SourceSystem <> s.SourceSystem;'
);

INSERT INTO DGDB.dbo.Validation_Rules
    (Rule_Name, Rule_Description, Rule_Category, Severity_Level, Rule_Logic)
VALUES
(
    'FactRental_Valid_Dates',
    'Ensures rental dates are earlier than return dates.',
    'Business Logic',
    'Warning',
    'SELECT COUNT(*)
     FROM WarehouseDB.dbo.FactRental
     WHERE ReturnDateKey IS NOT NULL 
       AND RentalDateKey > ReturnDateKey;'
);
 