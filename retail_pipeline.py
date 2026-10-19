
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum,round


# 1. Start Spark
spark = (
    SparkSession.builder
    .appName("RetailSalesDataPipeline")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# 2. Read sample retail sales data



df = spark.read.format("csv").options(header=True,inferschema=True).load("online_retail_II.csv")

print("\n====Raw Retail sales data=====")

df.show(5)
print("Total rows:", df.count())
print("Missing Customer IDs:", df.filter(col("Customer ID").isNull()).count())
print("Missing Descriptions:", df.filter(col("Description").isNull()).count())
print("Rows with missing Customer IDs:", df.filter(col("Customer ID").isNull()).count())
print("Valid Customer IDs:", df.filter(col("Customer ID").isNotNull()).count())
df.printSchema()

# 3. Transform: calculate total sales
sales_df = df.withColumn(
    "total_sales",
    col("Quantity") * col("Price")
)

print("\n=== TRANSFORMED SALES DATA ===")
sales_df.select("Invoice",
    "Description",
    "Quantity",
    "Price",
    "total_sales"
).show(5)
sales_df.write.mode("overwrite").option(
    "header", True
).csv("output/retail_sales")

# 4. SQL analysis
sales_df.createOrReplaceTempView("retail_sales")
customer_sales = spark.sql("""
    SELECT
        `Customer ID`,
        ROUND(SUM(total_sales), 2) AS total_spent,
        COUNT(DISTINCT Invoice) AS total_orders
    FROM retail_sales
    WHERE `Customer ID` IS NOT NULL
    GROUP BY `Customer ID`
    ORDER BY total_spent DESC
""")

print("=== TOP 10 CUSTOMERS BY SPENDING ===")
customer_sales.show(10)
customer_sales.write.mode("overwrite").option(
    "header", True
).csv("output/customer_sales")

country_sales = spark.sql("""
    SELECT
        Country,
        ROUND(SUM(total_sales), 2) AS revenue,
        SUM(Quantity) AS units_sold
    FROM retail_sales
    GROUP BY Country
    ORDER BY revenue DESC
""")

print("\n=== REVENUE BY COUNTRY (SQL) ===")
country_sales.show(10)
country_sales.write.mode("overwrite").option(
    "header", True
).csv("output/country_sales")

print("\nPipeline completed successfully!")




