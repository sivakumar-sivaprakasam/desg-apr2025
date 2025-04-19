import polars as pl
import datetime as dt
import time

start_time = time.time()
pl.Config.set_tbl_hide_dataframe_shape(True)
pl.Config.set_tbl_hide_column_data_types(True)
pl.Config.set_tbl_rows(100)
pl.Config.set_tbl_cols(100)
pl.Config.set_fmt_float("full")
pl.Config.set_thousands_separator(",")

# Load dataset into a DataFrame
df = pl.read_csv("SG_HDB_Resale_Flat_Prices.csv", separator=",", infer_schema_length=1_000_000)

# Get the facts 
#print(df.group_by(pl.col("flat_type")).len().sort(pl.col("flat_type")))

# Filter un-necessary data
excluded_flat_types = ["null", "1 ROOM", "2 ROOM"]
df = df.filter(~pl.col("flat_type").is_in(excluded_flat_types))

# Verify the data set post filter operation
#print(df.group_by(pl.col("flat_type")).len().sort(pl.col("flat_type")))

# Data Standardization
df = df.with_columns(flat_type = pl.when(pl.col("flat_type") == "MULTI-GENERATION")
                     .then(pl.lit("MULTI GENERATION"))
                     .otherwise(pl.col("flat_type")))

# Verify the data set post data standardization operation
#print(df.group_by(pl.col("flat_type")).len().sort(pl.col("flat_type")))

# For each flat type, find Minimum & Maximum resale valued transaction from data scope
df_agg_flat_types = df.group_by(pl.col("flat_type")).agg(
        pl.col("resale_price").min().alias("min_resale_value"),
        pl.col("resale_price").max().alias("max_resale_value")
        ).sort(pl.col("flat_type"))
print(df_agg_flat_types)

# For each town & flat type, find Minimum & Maximum resale valued transaction from data scope
#df_agg_town_flat_types = df.group_by(pl.col("town"), pl.col("flat_type")).agg(
#        pl.col("resale_price").min().alias("min_resale_value"),
#        pl.col("resale_price").max().alias("max_resale_value")
#        ).sort(pl.col("town"), pl.col("flat_type"))
#print(df_agg_town_flat_types)

# For each town & flat type, find Minimum & Maximum resale valued transaction from data scope
df_min = df.pivot("flat_type", index="town", values="resale_price", aggregate_function="min")
df_max = df.pivot("flat_type", index="town", values="resale_price", aggregate_function="max")
df_joined = df_min.join(df_max, on="town").select(
    pl.col("town"), 
    pl.col("3 ROOM").alias("3 ROOM (MIN)"), pl.col("3 ROOM_right").alias("3 ROOM (MAX)"), 
    pl.col("4 ROOM").alias("4 ROOM (MIN)"), pl.col("4 ROOM_right").alias("4 ROOM (MAX)"), 
    pl.col("5 ROOM").alias("5 ROOM (MIN)"), pl.col("5 ROOM_right").alias("5 ROOM (MAX)"), 
    pl.col("EXECUTIVE").alias("EXECUTIVE (MIN)"), pl.col("EXECUTIVE_right").alias("EXECUTIVE (MAX)"), 
    pl.col("MULTI GENERATION").alias("MULTI GENERATION (MIN)"), pl.col("MULTI GENERATION_right").alias("MULTI GENERATION (MAX)"))
print(df_joined)

print("Execution Time: %.2f seconds" % (time.time() - start_time))