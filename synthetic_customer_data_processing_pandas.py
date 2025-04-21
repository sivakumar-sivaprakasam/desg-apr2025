import pandas as pd
import time
import sys

start_time = time.time()

# Load dataset into a DataFrame
df = pd.read_csv(sys.argv[1], sep=",")

# Filter unnecessary data
excluded_countries = ["IT", "RU"]
df = df[~df["Country"].isin(excluded_countries)]

# Data Standardization
df["Country"] = df["Country"].replace({
    "US": "United States",
    "UK": "United Kingdom"
})

# Aggregate salary data
df_avg_salary = df.groupby("Country").agg(
    Avg_Salary=("Salary", "mean"),
    Count=("Salary", "size")
).reset_index().sort_values(by="Country")
print(df_avg_salary)

print("Execution Time: %.2f seconds" % (time.time() - start_time))