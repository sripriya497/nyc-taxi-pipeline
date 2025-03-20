import pandas as pd

# Load Parquet file
df = pd.read_parquet("data/yellow_tripdata_2024-12.parquet")

# Display first few rows
print(df.head())

# Show column names and data types
print(df.info())
