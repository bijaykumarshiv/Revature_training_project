# -----------------------------------------
# Visualization Script
# Healthcare Provider Analysis
# -----------------------------------------

import pandas as pd
import matplotlib.pyplot as plt


# Make plots look better
plt.style.use("seaborn-v0_8")

# -----------------------------------------
# Load ETL Output Files
# -----------------------------------------

path = "../Analytics/"

avg_payments_state = pd.read_csv(path + "avg_payments_by_state.csv")
avg_medicare_drg = pd.read_csv(path + "avg_medicare_by_drg.csv")
total_discharges_hosp = pd.read_csv(path + "total_discharges_by_hospital.csv")
top_expensive_drgs = pd.read_csv(path + "top_10_expensive_drgs.csv")

print("Analytics data loaded successfully!")

# -----------------------------------------
# Visualization 1: Avg Total Payments by State
# -----------------------------------------

plt.figure(figsize=(12, 6))
plt.bar(avg_payments_state["Provider State"], avg_payments_state["Avg_Total_Payments"])
plt.title("Average Total Payments by State")
plt.xlabel("State")
plt.ylabel("Avg Total Payments")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# -----------------------------------------
# Visualization 2: Avg Medicare Payments by DRG
# -----------------------------------------

top_drg = avg_medicare_drg.head(15)

plt.figure(figsize=(12, 6))
plt.barh(top_drg["DRG_Description"], top_drg["Avg_Medicare_Payments"])
plt.title("Top 15 DRGs with Highest Medicare Payments")
plt.xlabel("Medicare Payment Amount")
plt.tight_layout()
plt.show()

# -----------------------------------------
# Visualization 3: Total Discharges by Hospital (Top 20)
# -----------------------------------------

top_hosp = total_discharges_hosp.head(20)

plt.figure(figsize=(12, 6))
plt.barh(top_hosp["Provider Name"], top_hosp["Total_Discharges"])
plt.title("Top 20 Hospitals by Total Discharges")
plt.xlabel("Total Discharges")
plt.tight_layout()
plt.show()

# -----------------------------------------
# Visualization 4: Top 10 Most Expensive DRGs
# -----------------------------------------

plt.figure(figsize=(12, 6))
plt.barh(top_expensive_drgs["DRG_Description"], top_expensive_drgs["Avg_Total_Payments"])
plt.title("Top 10 Most Expensive DRGs")
plt.xlabel("Avg Total Payments")
plt.tight_layout()
plt.show()

print("All visualizations generated successfully!")
