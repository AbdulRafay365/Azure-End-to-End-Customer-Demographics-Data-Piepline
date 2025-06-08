CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'mYhumber365'

CREATE DATABASE SCOPED CREDENTIAL cred_abdul 
WITH 
    IDENTITY = 'Managed Identity' 


CREATE EXTERNAL DATA SOURCE source_silver

WITH
    (
        LOCATION = 'https://abdulrafayawstorage.blob.core.windows.net/silver',
        CREDENTIAL = cred_abdul
    )

CREATE EXTERNAL DATA SOURCE source_gold

WITH
    (
        LOCATION = 'https://abdulrafayawstorage.blob.core.windows.net/gold',
        CREDENTIAL = cred_abdul
    )

CREATE EXTERNAL FILE FORMAT format_parquet
WITH
    (
        FORMAT_TYPE = PARQUET,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    )

CREATE EXTERNAL TABLE gold.extsales_fact
WITH (
    LOCATION = 'extsales_fact',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.vw_Sales_Fact;

select * FROM gold.extsales_fact;

CREATE EXTERNAL TABLE gold.extproducts_dim
WITH (
    LOCATION = 'extproducts_dim',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.vw_products_dim;

SELECT * FROM gold.vw_products_dim;

CREATE EXTERNAL TABLE gold.extcustomers_dim
WITH (
    LOCATION = 'extcustomers_dim',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.vw_customers_dim;

SELECT * FROM gold.vw_customers_dim;

CREATE EXTERNAL TABLE gold.extterritories_dim
WITH (
    LOCATION = 'extterritories_dim',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.vw_territories_dim;

SELECT * FROM gold.vw_territories_dim;

CREATE EXTERNAL TABLE gold.extcalendar_dim
WITH (
    LOCATION = 'extcalendar_dim',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.vw_calendar_dim;

SELECT * FROM gold.vw_calendar_dim;