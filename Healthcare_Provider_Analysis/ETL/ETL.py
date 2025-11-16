# ------------------------------
# ETL Pipeline
# Healthcare Provider Analysis
# ------------------------------

import findspark
findspark.init("C:/spark")

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Start Spark
spark = SparkSession.builder \
    .appName("Healthcare Provider ETL") \
    .getOrCreate()

# ---------------------------------------------
# 1. EXTRACT: Load Cleaned Data
# ---------------------------------------------
df = spark.read.csv(
    "../Cleaned_Data/cleaned_healthcare_data.csv",
    header=True, inferSchema=True
)

print("Data Loaded Successfully")
df.show(5)

# ---------------------------------------------
# 2. TRANSFORMATIONS
# ---------------------------------------------

# A. Average Total Payments by State
avg_payments_state = df.groupBy("Provider State") \
    .agg(F.avg("Average Total Payments").alias("Avg_Total_Payments")) \
    .orderBy("Avg_Total_Payments", ascending=False)

# B. Average Medicare Payments by DRG
avg_medicare_drg = df.groupBy("DRG_Description") \
    .agg(F.avg("Average Medicare Payments").alias("Avg_Medicare_Payments")) \
    .orderBy("Avg_Medicare_Payments", ascending=False)

# C. Total Discharges by Hospital
total_discharges_hosp = df.groupBy("Provider Id", "Provider Name") \
    .agg(F.sum("Total Discharges").alias("Total_Discharges")) \
    .orderBy("Total_Discharges", ascending=False)

# D. Top 10 Expensive DRGs
top_expensive_drgs = df.groupBy("DRG_Description") \
    .agg(F.avg("Average Total Payments").alias("Avg_Total_Payments")) \
    .orderBy("Avg_Total_Payments", ascending=False) \
    .limit(10)
print(type(top_expensive_drgs))
# ---------------------------------------------
# 3. LOAD: Save outputs (CSV for Windows)
# ---------------------------------------------

avg_payments_state.toPandas().to_csv("../Analytics/avg_payments_by_state.csv", index=False)
avg_medicare_drg.toPandas().to_csv("../Analytics/avg_medicare_by_drg.csv", index=False)
total_discharges_hosp.toPandas().to_csv("../Analytics/total_discharges_by_hospital.csv", index=False)
top_expensive_drgs.toPandas().to_csv("../Analytics/top_10_expensive_drgs.csv", index=False)

print("\nETL Completed Successfully!")
print("All analytics files saved to ../Analytics/")
