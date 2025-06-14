#Big Data Residency Weekend Group Project
#GROUP-03: Team members(Satyanarayana Reddy Muttana, Sushila Sipai, Yanjie Liu)
#2025 Summer - Big Data (MSDS-632-M51) - Full Term
#Dr. Moody Amakobe
#06/15/2025

import org.apache.spark.sql.functions._

#Load CSV
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("C:/Users/satya/Documents/UC/Summer/Retail.csv")

#Step 1: preprocessing: Filter out bad descriptions
val cleanedDF = df.filter(
  $"Description".isNotNull &&
  $"CustomerID".isNotNull &&
  !$"Description".isin("?", "check", "damaged", "damages", "mailout", "") &&
  !lower($"Description").contains("check") &&
  !lower($"Description").contains("damaged") &&
  !lower($"Description").contains("mailout")
)

#Trim whitespaces
val cleanedDFTrimmed = cleanedDF.withColumn("Description", trim($"Description"))

#Step 2: Create views
cleanedDFTrimmed.select("StockCode", "Description").distinct().createOrReplaceTempView("products")
cleanedDFTrimmed.select("CustomerID", "Country").distinct().createOrReplaceTempView("customers")
cleanedDFTrimmed.select("InvoiceNo", "StockCode", "CustomerID", "InvoiceDate", "Quantity", "UnitPrice", "Country").createOrReplaceTempView("transactions")


#Top 10 Selling Products
spark.sql("""
  SELECT p.Description, SUM(t.Quantity) AS TotalQuantity
  FROM transactions t
  JOIN products p ON t.StockCode = p.StockCode
  GROUP BY p.Description
  ORDER BY TotalQuantity DESC
  LIMIT 10
""").show()


#Revenue per Country:
spark.sql("""
  SELECT Country, SUM(Quantity * UnitPrice) AS Revenue
  FROM transactions
  GROUP BY Country
  ORDER BY Revenue DESC
""").show()

#3. Sales Over Time (By Month)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
val transactionsWithDate = spark.sql("SELECT * FROM transactions").withColumn("InvoiceTimestamp", to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm"))
transactionsWithDate.groupBy(month(col("InvoiceTimestamp")).alias("Month")).agg(sum("Quantity").alias("TotalQuantity")).orderBy("Month").show()

#4. Top Customers by Revenue
val revenueDF = spark.sql("SELECT * FROM transactions").withColumn("Revenue", col("Quantity") * col("UnitPrice"))
revenueDF.groupBy("CustomerID").agg(round(sum("Revenue"), 2).alias("TotalSpent")).orderBy(desc("TotalSpent")).show(10)



#Visualization 
from google.colab import files
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import matplotlib.dates as mdates

# Upload the CSV
uploaded = files.upload()

# Load with encoding handling
df = pd.read_csv("Retail.csv", encoding='latin1')

# Basic inspection
print("Before cleaning:", df.shape)

# Drop rows with missing essential fields
df = df.dropna(subset=['InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country'])

# Convert data types
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
df['Quantity'] = df['Quantity'].astype(int)
df['UnitPrice'] = df['UnitPrice'].astype(float)
df['CustomerID'] = df['CustomerID'].astype(str)

# Add Revenue column
df['Revenue'] = df['Quantity'] * df['UnitPrice']

print("After cleaning:", df.shape)
df.head()


#Step 3: Create Logical Tables
# Products Table
products = df[['StockCode', 'Description']].drop_duplicates().reset_index(drop=True)

# Customers Table
customers = df[['CustomerID', 'Country']].drop_duplicates().reset_index(drop=True)

# Transactions Table
transactions = df[['InvoiceNo', 'StockCode', 'CustomerID', 'Quantity', 'UnitPrice', 'InvoiceDate', 'Revenue']].reset_index(drop=True)

# Create SQLite DB
conn = sqlite3.connect("retail.db")

# Write tables
products.to_sql("products", conn, if_exists="replace", index=False)
customers.to_sql("customers", conn, if_exists="replace", index=False)
transactions.to_sql("transactions", conn, if_exists="replace", index=False)


#Daily Quantity sold:
query = """
SELECT DATE(InvoiceDate) AS InvoiceDate, SUM(Quantity) AS TotalQuantity
FROM transactions
GROUP BY DATE(InvoiceDate)
ORDER BY DATE(InvoiceDate)
"""
daily_sales = pd.read_sql(query, conn)
daily_sales['InvoiceDate'] = pd.to_datetime(daily_sales['InvoiceDate'])

plt.figure(figsize=(14,6))
plt.plot(daily_sales['InvoiceDate'], daily_sales['TotalQuantity'])
plt.title("Daily Quantity Sold")
plt.xlabel("Date")
plt.ylabel("Total Quantity")
plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()
plt.show()


#Top 10 selling notebooks
query = """
SELECT p.Description, SUM(t.Quantity) AS TotalQuantity
FROM transactions t
JOIN products p ON t.StockCode = p.StockCode
GROUP BY p.Description
ORDER BY TotalQuantity DESC
LIMIT 10
"""
top_products = pd.read_sql(query, conn)
top_products = top_products.dropna(subset=['Description'])

plt.figure(figsize=(12,6))
plt.bar(top_products['Description'], top_products['TotalQuantity'])
plt.title("Top 10 Best-Selling Products")
plt.ylabel("Quantity Sold")
plt.xticks(rotation=45, ha='right')
plt.grid(True)
plt.tight_layout()
plt.show()


#revenue by country:
query = """
SELECT c.Country, SUM(t.Revenue) AS TotalRevenue
FROM transactions t
JOIN customers c ON t.CustomerID = c.CustomerID
GROUP BY c.Country
ORDER BY TotalRevenue DESC
LIMIT 10
"""
country_sales = pd.read_sql(query, conn)

plt.figure(figsize=(12,6))
plt.bar(country_sales['Country'], country_sales['TotalRevenue'])
plt.title("Top 10 Countries by Revenue")
plt.ylabel("Total Revenue (GBP)")
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()
plt.show()


#revenue per customer distribution:
query = """
SELECT CustomerID, SUM(Revenue) AS TotalRevenue
FROM transactions
GROUP BY CustomerID
"""
customer_revenue = pd.read_sql(query, conn)

plt.figure(figsize=(10,5))
customer_revenue['TotalRevenue'].hist(bins=50)
plt.title("Distribution of Revenue per Customer")
plt.xlabel("Total Revenue")
plt.ylabel("Number of Customers")
plt.grid(True)
plt.tight_layout()
plt.show()



