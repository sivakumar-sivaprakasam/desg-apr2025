import polars as pl
from datetime import datetime
import time

start_time = time.time()
pl.Config.set_tbl_hide_dataframe_shape(True)
pl.Config.set_tbl_hide_column_data_types(True)
pl.Config.set_tbl_rows(1000)
pl.Config.set_tbl_cols(100)
pl.Config.set_fmt_float("full")
pl.Config.set_thousands_separator(",")

# Load dataset into a DataFrame
df = pl.read_csv("data/SG_HDB_Resale_Flat_Prices.csv", separator=",", infer_schema_length=1_000_000)

# Filter un-necessary data
excluded_flat_types = ["null", "1 ROOM", "2 ROOM"]
df = df.filter(~pl.col("flat_type").is_in(excluded_flat_types))

# Data Standardization
df = df.with_columns(flat_type = pl.when(pl.col("flat_type") == "MULTI-GENERATION").then(pl.lit("MULTI GENERATION"))
                     .when(pl.col("flat_type") == "3 ROOM").then(pl.lit("3RM"))
                     .when(pl.col("flat_type") == "4 ROOM").then(pl.lit("4RM"))
                     .when(pl.col("flat_type") == "5 ROOM").then(pl.lit("5RM"))
                     .when(pl.col("flat_type") == "EXECUTIVE").then(pl.lit("EXEC"))
                     .when(pl.col("flat_type") == "MULTI GENERATION").then(pl.lit("MULTI-GEN")),
                     decade = pl.col("month").str.to_date("%Y-%m").dt.year() // 10 * 10)

print("Min & Max Resale Values for each flat type for the period 1990 - Till date")
# For each flat type, find Minimum & Maximum resale valued transaction from data scope
df_agg_flat_types = df.group_by(pl.col("flat_type")).agg(
        pl.col("resale_price").min().alias("min_resale_value"),
        pl.col("resale_price").max().alias("max_resale_value")
        ).sort(pl.col("flat_type"))
print(df_agg_flat_types)

print("Min & Max Resale Values for each flat type from each town & decade for the period 1990 - Till date")
# For each town & flat type, find Minimum & Maximum resale valued transaction from data scope
df_min = df.pivot("flat_type", index=["town", "decade"], values="resale_price", aggregate_function="min")
df_max = df.pivot("flat_type", index=["town", "decade"], values="resale_price", aggregate_function="max")
df_joined = df_min.join(df_max, on=["town", "decade"]).select(
    pl.col("town"), 
    pl.format("{}", pl.col("decade")),
    pl.col("3RM").alias("3RM (MIN)"), pl.col("3RM_right").alias("3RM (MAX)"), 
    pl.col("4RM").alias("4RM (MIN)"), pl.col("4RM_right").alias("4RM (MAX)"), 
    pl.col("5RM").alias("5RM (MIN)"), pl.col("5RM_right").alias("5RM (MAX)"), 
    pl.col("EXEC").alias("EXEC (MIN)"), pl.col("EXEC_right").alias("EXEC (MAX)"), 
    pl.col("MULTI-GEN").alias("MULTI-GEN (MIN)"), pl.col("MULTI-GEN_right").alias("MULTI-GEN (MAX)")).sort("town", "decade")
print(df_joined)

print("Execution Time: %.2f seconds" % (time.time() - start_time))