import polars as pl
import time
import sys

start_time = time.time()
pl.Config.set_tbl_hide_dataframe_shape(True)
pl.Config.set_tbl_hide_column_data_types(True)
pl.Config.set_tbl_rows(100)
pl.Config.set_tbl_cols(100)
pl.Config.set_fmt_float("full")
pl.Config.set_thousands_separator(",")

# Load dataset into a DataFrame
df = pl.read_csv(sys.argv[1], separator=",")

# Filter unnecessary data
excluded_countries = ["IT", "RU"]
df = df.filter(~pl.col("Country").is_in(excluded_countries))

# Data Standardization
df = df.with_columns(Country = pl
                     .when(pl.col("Country") == "US").then(pl.lit("United States"))
                     .when(pl.col("Country") == "UK").then(pl.lit("United Kingdom"))
                     .otherwise(pl.col("Country")))

df_avg_salary = df.group_by(pl.col("Country")).agg(
    pl.col("Salary").mean().alias("Avg_Salary"),
    pl.len().alias("Count")
).sort(pl.col("Country"))
print(df_avg_salary)

print("Execution Time: %.2f seconds" % (time.time() - start_time))