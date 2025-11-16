# ------------------------------
# Data Cleaning Script
# Healthcare Provider Analysis
# ------------------------------

import findspark
findspark.init("C:/spark")

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Start Spark Session
spark = SparkSession.builder \
    .appName("Healthcare Provider Cleaning") \
    .getOrCreate()

# ---------------------------------------------
# 1. Load raw datasets
# ---------------------------------------------
df1 = spark.read.csv("../Data_Resources/healthcare_provider_data_part1.csv",
                     header=True, inferSchema=True)

df2 = spark.read.csv("../Data_Resources/healthcare_provider_data_part2.csv",
                     header=True, inferSchema=True)

# Combine both datasets
df = df1.unionByName(df2)

# ---------------------------------------------
# 2. Clean Numeric Columns (remove $, commas)
# ---------------------------------------------
numeric_cols = [
    "Average Covered Charges",
    "Average Total Payments",
    "Average Medicare Payments"
]

for col in numeric_cols:
    df = df.withColumn(
        col,
        F.regexp_replace(F.col(col), "[$,]", "").cast("double")
    )

# ---------------------------------------------
# 3. Extract DRG Code & Description
# ---------------------------------------------
df = df.withColumn(
    "DRG_Code",
    F.regexp_extract(F.col("DRG Definition"), r"(\d+)", 1).cast("int")
)

df = df.withColumn(
    "DRG_Description",
    F.regexp_replace(F.col("DRG Definition"), r"^\d+\s*-\s*", "")
)

# ---------------------------------------------
# 4. Remove duplicates
# ---------------------------------------------
df = df.dropDuplicates()

# ---------------------------------------------
# 5. Remove rows missing required fields
# ---------------------------------------------
df = df.dropna(subset=["Provider Id", "DRG_Code"])

# ---------------------------------------------
# 6. Save cleaned dataset using Pandas (Windows safe)
# ---------------------------------------------
pdf = df.toPandas()   # Convert from Spark → Pandas

pdf.to_csv("../Cleaned_Data/cleaned_healthcare_data.csv", index=False)

print("✔ Data cleaning completed successfully!")
print("✔ Cleaned CSV saved to: ../Cleaned_Data/cleaned_healthcare_data.csv")
