# Databricks notebook source
# MAGIC %md
# MAGIC ## **Silver Layer Transformations**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###**Connect to Azure Data Lake Storage**

# COMMAND ----------

# Set the authentication type to OAuth for the specified Azure Data Lake Storage Gen2 account
spark.conf.set("fs.azure.account.auth.type.abdulrafayawstorage.dfs.core.windows.net", "OAuth")

# Specify the OAuth provider class responsible for getting the access token using client credentials
spark.conf.set("fs.azure.account.oauth.provider.type.abdulrafayawstorage.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

# Set the Azure AD Application (Client) ID registered in Azure Active Directory
spark.conf.set("fs.azure.account.oauth2.client.id.abdulrafayawstorage.dfs.core.windows.net", 
               "87492583-f214-4fa6-84ee-4331fdb1ac1b")

# Set the Client Secret (password) generated for the Azure AD Application
# ⚠️ IMPORTANT: Keep this value secure and never expose it in shared code or version control
spark.conf.set("fs.azure.account.oauth2.client.secret.abdulrafayawstorage.dfs.core.windows.net", 
               "Hva8Q~rWQ~gFDoNA0EeLV8tdEU~D9P5nNHBiIcVa")

# Set the Azure AD OAuth2 token endpoint using your Tenant ID
spark.conf.set("fs.azure.account.oauth2.client.endpoint.abdulrafayawstorage.dfs.core.windows.net", 
               "https://login.microsoftonline.com/0b6489a9-ebe8-4b5d-bf1d-8eb5ca5ef945/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ###**Phase 1: Data Ingestion**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Data

# COMMAND ----------

df_calendar = spark.read.format("csv")\
  .option("header", "true")\
    .option("inferSchema", "true")\
      .load("abfss://rawdata@abdulrafayawstorage.dfs.core.windows.net/Calendar")

df_calendar.display()

# COMMAND ----------

df_customers = spark.read.format("csv")\
    .option("header", "true")\
        .option("InferSchema", "true")\
            .load("abfss://rawdata@abdulrafayawstorage.dfs.core.windows.net/Customers")
df_customers.display()

# COMMAND ----------

df_product_categories = spark.read.format("csv")\
    .option("header", "true")\
        .option("InferSchema", "true")\
            .load("abfss://rawdata@abdulrafayawstorage.dfs.core.windows.net/Product Categories")
df_product_categories.display()

# COMMAND ----------

df_product_subcategories = spark.read.format("csv")\
    .option("header", "true")\
        .option("InferSchema", "true")\
            .load("abfss://rawdata@abdulrafayawstorage.dfs.core.windows.net/Product Subcategories")
df_product_subcategories.display()

# COMMAND ----------

df_products = spark.read.format("csv")\
    .option("header", "true")\
        .option("InferSchema", "true")\
            .load("abfss://rawdata@abdulrafayawstorage.dfs.core.windows.net/Products")
df_products.display()

# COMMAND ----------

df_returns = spark.read.format("csv")\
    .option("header", "true")\
        .option("InferSchema", "true")\
            .load("abfss://rawdata@abdulrafayawstorage.dfs.core.windows.net/Returns")
df_returns.display()

# COMMAND ----------

df_sales_2015 = spark.read.format("csv")\
    .option("header", "true")\
        .option("InferSchema", "true")\
            .load("abfss://rawdata@abdulrafayawstorage.dfs.core.windows.net/Sales 2015")
df_sales_2015.display()

# COMMAND ----------

df_sales_2016 = spark.read.format("csv")\
    .option("header", "true")\
        .option("InferSchema", "true")\
            .load("abfss://rawdata@abdulrafayawstorage.dfs.core.windows.net/Sales 2016")
df_sales_2016.display()

# COMMAND ----------

df_sales_2017 = spark.read.format("csv")\
    .option("header", "true")\
        .option("InferSchema", "true")\
            .load("abfss://rawdata@abdulrafayawstorage.dfs.core.windows.net/Sales 2017")
df_sales_2017.display()

# COMMAND ----------

df_sales = spark.read.format("csv")\
    .option("header", "true")\
        .option("inferschema", "true")\
            .load('abfss://rawdata@abdulrafayawstorage.dfs.core.windows.net/Sales *')
df_sales.display()

# COMMAND ----------

df_territories = spark.read.format("csv")\
    .option("header", "true")\
        .option("InferSchema", "true")\
            .load("abfss://rawdata@abdulrafayawstorage.dfs.core.windows.net/Territories")
df_territories.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##**Phase 2: Data Transformation**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calender
# MAGIC ##### Extracting month from date column

# COMMAND ----------

df_calendar.display()

# COMMAND ----------

df_calendar = df_calendar\
    .withColumn('Month', month(col("Date")))\
    .withColumn('Year', year(col("Date")))
df_calendar.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writing to Silver Container

# COMMAND ----------

path = "abfss://silver@abdulrafayawstorage.dfs.core.windows.net/Calendar"

# Write the DataFrame to the specified path
df_calendar.write.format("parquet").mode('ignore').option("path", path).save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers
# MAGIC
# MAGIC ##### Concatonating prefix, firstname and lastname columns into a new fullname column for easier interpretability.

# COMMAND ----------

df_customers = df_customers.withColumn("fullname", concat_ws(" ", col('prefix'), col("firstname"), col('lastname')))

# COMMAND ----------

df_customers.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writing to Silver Container

# COMMAND ----------

path = "abfss://silver@abdulrafayawstorage.dfs.core.windows.net/Customers"

# Write the DataFrame to the specified path
df_customers.write.format("parquet").mode('ignore').option("path", path).save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Categories

# COMMAND ----------

df_product_categories.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writing to Silver Container

# COMMAND ----------

path = "abfss://silver@abdulrafayawstorage.dfs.core.windows.net/Product Categories"

df_product_categories.write.format("Parquet")\
    .mode("Ignore")\
        .option("path", path)\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Subcategories

# COMMAND ----------

df_product_subcategories.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writing to Silver Container

# COMMAND ----------

path = "abfss://silver@abdulrafayawstorage.dfs.core.windows.net/Product Subcategories"

# Write the DataFrame to the specified path
df_product_subcategories.write.format("parquet").mode('ignore').option("path", path).save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products
# MAGIC ##### Clarifying Product Information

# COMMAND ----------

df_products.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writing to Silver Container

# COMMAND ----------

path = "abfss://silver@abdulrafayawstorage.dfs.core.windows.net/Products"

# Write the DataFrame to the specified path
df_products.write.format("parquet").mode('ignore').option("path", path).save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Returns

# COMMAND ----------

df_returns.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writing to Silver Container

# COMMAND ----------

path = "abfss://silver@abdulrafayawstorage.dfs.core.windows.net/Returns"

df_returns.write.format("Parquet")\
    .option("path", path)\
        .mode("ignore")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Territories

# COMMAND ----------

df_territories.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writing to Silver Container

# COMMAND ----------

path = "abfss://silver@abdulrafayawstorage.dfs.core.windows.net/Territories"

df_territories.write.format("parquet")\
.option("path", path)\
    .mode("ignore")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales

# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writing to Silver Container

# COMMAND ----------

path = "abfss://silver@abdulrafayawstorage.dfs.core.windows.net/Sales"

df_sales.coalesce(1)\
    .write.format("parquet")\
    .mode("ignore")\
    .option("path", path)\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adhoc Analysis

# COMMAND ----------

df_customers.createOrReplaceTempView("Customers_vw")
df_sales.createOrReplaceTempView("Sales_vw")
df_products.createOrReplaceTempView("Products_vw")
df_product_categories.createOrReplaceTempView("ProductCategories_vw")
df_product_subcategories.createOrReplaceTempView("ProductSubcategories_vw")
df_returns.createOrReplaceTempView("Returns_vw")


# COMMAND ----------

print(
    (df_sales_2015.count(), len(df_sales_2015.columns)),
    (df_sales_2016.count(), len(df_sales_2016.columns)),
    (df_sales_2017.count(), len(df_sales_2017.columns))
)

# COMMAND ----------

df_sales.describe().display()

# COMMAND ----------

df_sales.groupBy('OrderDate')\
    .agg(count('OrderNumber').alias('TotalOrderQuantity')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ``` sql
# MAGIC
# MAGIC SELECT YEAR(orderdate) AS year, 
# MAGIC        SUM(orderquantity) AS totalquantity
# MAGIC FROM sales
# MAGIC GROUP BY YEAR(orderdate)
# MAGIC ORDER BY year;
# MAGIC
# MAGIC ```
# MAGIC

# COMMAND ----------

df_product_categories.display()

# COMMAND ----------

df_product_subcategories.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM productsubcategories_vw;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM sales_vw
# MAGIC WHERE productkey = 397;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TOP Categories by Transactions and Revenue
# MAGIC -- SALES_vw —— productkey —— PRODUCTS_vw —— productsubcategorykey ——— PRODUCTSUBCATEGORIES_vw ——— productcategorykey —— PRODUCTCATEGORIES_vw
# MAGIC SELECT productcategories_vw.categoryname, 
# MAGIC        COUNT(sales_vw.ordernumber) AS TotalTransactions, 
# MAGIC        ROUND(CAST(SUM(sales_vw.orderquantity * products_vw.productprice) AS DECIMAL(10, 2))) AS Revenue
# MAGIC FROM productcategories_vw
# MAGIC LEFT JOIN productsubcategories_vw 
# MAGIC   ON productcategories_vw.productcategorykey = productsubcategories_vw.productcategorykey
# MAGIC LEFT JOIN products_vw 
# MAGIC   ON productsubcategories_vw.productsubcategorykey = products_vw.productsubcategorykey
# MAGIC LEFT JOIN sales_vw 
# MAGIC   ON products_vw.productkey = sales_vw.productkey
# MAGIC GROUP BY productcategories_vw.categoryname
# MAGIC ORDER BY Revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TOP SubCategories by Transactions and Revenue
# MAGIC -- SALES_vw —— productkey —— PRODUCTS_vw —— productsubcategorykey ——— PRODUCTSUBCATEGORIES_vw ——— productcategorykey —— PRODUCTCATEGORIES_vw
# MAGIC SELECT productsubcategories_vw.subcategoryname, 
# MAGIC        COUNT(sales_vw.ordernumber) AS TotalTransactions, 
# MAGIC        ROUND(CAST(SUM(sales_vw.orderquantity * products_vw.productprice) AS DECIMAL(10, 2))) AS Revenue
# MAGIC FROM productcategories_vw
# MAGIC LEFT JOIN productsubcategories_vw 
# MAGIC   ON productcategories_vw.productcategorykey = productsubcategories_vw.productcategorykey
# MAGIC LEFT JOIN products_vw 
# MAGIC   ON productsubcategories_vw.productsubcategorykey = products_vw.productsubcategorykey
# MAGIC LEFT JOIN sales_vw 
# MAGIC   ON products_vw.productkey = sales_vw.productkey
# MAGIC GROUP BY productsubcategories_vw.subcategoryname
# MAGIC HAVING TotalTransactions > 0
# MAGIC ORDER BY Revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   SUBSTRING_INDEX(ProductName, ' ', 1) AS BaseProduct,  -- captures e.g., "Mountain-100"
# MAGIC   ROUND(SUM(ProductPrice - ProductCost), 2) AS TotalProfit
# MAGIC FROM products_vw
# MAGIC GROUP BY BaseProduct
# MAGIC ORDER BY TotalProfit DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUBSTRING_INDEX(products_vw.ProductName, ' ', 1) AS BaseProduct, COUNT(*) AS TotalSales
# MAGIC FROM Sales_vw 
# MAGIC JOIN Products_vw
# MAGIC   ON Sales_vw.ProductKey = Products_vw.ProductKey
# MAGIC GROUP BY Products_vw.ProductKey, Products_vw.ProductName
# MAGIC ORDER BY TotalSales DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   SUBSTRING_INDEX(products_vw.productname, ' ', 1) AS BaseProduct, 
# MAGIC   COUNT(*) AS TotalReturns
# MAGIC FROM returns_vw
# MAGIC JOIN products_vw
# MAGIC   ON returns_vw.productkey = products_vw.productkey
# MAGIC GROUP BY BaseProduct
# MAGIC ORDER BY TotalReturns DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS TotalCustomers
# MAGIC FROM customers_vw;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   sales_vw.customerkey, customers_vw.fullname, COUNT(sales_vw.ordernumber) AS TotalTransactions,
# MAGIC   ROUND(CAST(SUM(sales_vw.orderquantity * products_vw.productprice) AS DECIMAL(10, 2))) AS Revenue
# MAGIC FROM sales_vw
# MAGIC LEFT JOIN customers_vw
# MAGIC   ON sales_vw.customerkey = customers_vw.customerkey
# MAGIC   LEFT JOIN products_vw
# MAGIC     ON sales_vw.productkey = products_vw.productkey
# MAGIC GROUP BY sales_vw.customerkey, customers_vw.fullname
# MAGIC ORDER BY TotalTransactions DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC