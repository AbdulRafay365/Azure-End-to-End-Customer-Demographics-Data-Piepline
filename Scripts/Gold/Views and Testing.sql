DROP VIEW IF EXISTS gold.vw_Sales_Fact;


--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Sales and Returns
--------------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE VIEW gold.vw_Sales_Fact AS
WITH salesandreturns AS (
    -- Sales data
    SELECT 
        OrderDate AS TransactionDate,
        OrderNumber,
        ProductKey,
        CustomerKey,
        TerritoryKey AS SalesTerritoryKey,
        OrderLineItem,
        OrderQuantity AS Quantity,
        'Purchase' AS TransactionType,
        CAST(NULL AS DATE) AS ReturnDate,
        0 AS ReturnQuantity,
        StockDate
    FROM
        OPENROWSET (
            BULK 'https://abdulrafayawstorage.dfs.core.windows.net/silver/Sales/',
            FORMAT = 'PARQUET'
        ) AS sales_ingest
    
    UNION ALL
    
    -- Returns data
    SELECT 
        ReturnDate AS TransactionDate,
        CAST(NULL AS VARCHAR(20)) AS OrderNumber,
        ProductKey,
        CAST(NULL AS INT) AS CustomerKey,
        Territorykey AS SalesTerritoryKey,
        CAST(NULL AS INT) AS OrderLineItem,
        0 AS Quantity,
        'Return' AS TransactionType,
        ReturnDate,
        ReturnQuantity,
        CAST(NULL AS DATE) AS StockDate
    FROM
        OPENROWSET (
            BULK 'https://abdulrafayawstorage.dfs.core.windows.net/silver/Returns/',
            FORMAT = 'PARQUET'
        ) AS returns_ingest
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY TransactionDate, ProductKey) AS SalesFactID,
    TransactionDate,
    OrderNumber,
    ProductKey,
    CustomerKey,
    SalesTerritoryKey,
    OrderLineItem,
    Quantity,
    TransactionType,
    ReturnDate,
    ReturnQuantity,
    StockDate,
    CASE 
        WHEN TransactionType = 'Purchase' THEN Quantity
        WHEN TransactionType = 'Return' THEN -ReturnQuantity
    END AS NetQuantity
FROM salesandreturns;

SELECT * FROM gold.vw_Sales_Fact;


--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Products with Category and Subcategory
--------------------------------------------------------------------------------------------------------------------------------------------------------------------


DROP VIEW IF EXISTS gold.vw_products_dim;

CREATE VIEW gold.vw_products_dim AS 
WITH products AS (
    SELECT 
    ProductKey,	
    ProductSubcategoryKey,
    ProductSKU,	
    ProductName,	
    ModelName,	
    ProductDescription,	
    ProductColor,
    ProductSize,	
    ProductStyle,	
    ProductCost,
    ProductPrice
    FROM 
        OPENROWSET (
                BULK 'https://abdulrafayawstorage.dfs.core.windows.net/silver/Products/',
                FORMAT = 'PARQUET'
            ) AS products_ingest
),

productcategories AS (
    SELECT
    productcategorykey,
    categoryname
    FROM
        OPENROWSET (
                    BULK 'https://abdulrafayawstorage.dfs.core.windows.net/silver/Product Categories/',
                    FORMAT = 'PARQUET'
                ) AS product_categories_ingest

),

productsubcategories AS (
    SELECT
    ProductSubcategoryKey,
    SubcategoryName,
    productCategoryKey
    FROM
        OPENROWSET (
                    BULK 'https://abdulrafayawstorage.dfs.core.windows.net/silver/Product Subcategories/',
                    FORMAT = 'PARQUET'
                ) AS product_subcategories_ingest
)

SELECT
    products.productkey,
    products.productname,
    SUBSTRING(
            products.ProductName, 
            1, 
            CASE 
                WHEN CHARINDEX(' ', products.ProductName) > 0 
                THEN CHARINDEX(' ', products.ProductName) - 1 
                ELSE LEN(products.ProductName) 
            END
        ) AS baseproductname, -- FROM DATABRICKS -- SUBSTRING_INDEX(products_vw.ProductName, ' ', 1) AS BaseProduct
    productcategories.categoryname,
    productsubcategories.subcategoryname
FROM
    products
JOIN productsubcategories ON products.productsubcategorykey = productsubcategories.productsubcategorykey
JOIN productcategories ON productsubcategories.productcategorykey = productcategories.productcategorykey;


SELECT * FROM gold.vw_products_dim;


--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Customers
--------------------------------------------------------------------------------------------------------------------------------------------------------------------

DROP VIEW IF EXISTS gold.vw_customers_dim;

CREATE VIEW gold.vw_customers_dim AS
SELECT 
    CustomerKey, 
    Prefix, 
    FirstName, 
    LastName, 
    BirthDate, 
    MaritalStatus, 
    Gender, 
    EmailAddress, 
    AnnualIncome, 
    TotalChildren, 
    EducationLevel, 
    Occupation, 
    HomeOwner, 
    fullname
FROM
    OPENROWSET (
        BULK 'https://abdulrafayawstorage.dfs.core.windows.net/silver/Customers/',
        FORMAT = 'PARQUET'
    ) AS customers_ingest;

SELECT * FROM gold.vw_customers_dim;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Territories
--------------------------------------------------------------------------------------------------------------------------------------------------------------------

DROP VIEW IF EXISTS gold.vw_territories_dim;

CREATE VIEW gold.vw_territories_dim AS
SELECT
    SalesTerritoryKey, 
    Region, 
    Country, 
    Continent
FROM
    OPENROWSET (
        BULK 'https://abdulrafayawstorage.dfs.core.windows.net/silver/Territories/',
        FORMAT = 'PARQUET'
    ) AS territories_ingest;

SELECT * FROM gold.vw_territories_dim;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Calendar
--------------------------------------------------------------------------------------------------------------------------------------------------------------------

DROP VIEW IF EXISTS gold.vw_calendar_dim;

CREATE VIEW gold.vw_calendar_dim AS
SELECT
    Date,
    DATEPART(WEEKDAY, Date) AS DayOfWeekNumber,         -- Returns 1 (Sunday) to 7 (Saturday)
    DATENAME(WEEKDAY, Date) AS DayOfWeekName,           -- Returns name like 'Monday'
    MONTH(Date) AS Month,
    DATENAME(MONTH, Date) AS MonthName,
    YEAR(Date) AS Year
FROM
    OPENROWSET (
        BULK 'https://abdulrafayawstorage.dfs.core.windows.net/silver/Calendar/',
        FORMAT = 'PARQUET'
    ) AS calendar_ingest;

SELECT * FROM gold.vw_calendar_dim;
