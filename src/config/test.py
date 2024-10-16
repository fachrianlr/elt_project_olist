import os

import pandas as pd
from sqlalchemy import create_engine

from src.config.common import PARENT_FOLDER

# Step 1: Read the CSV file without headers
file_path = sql_folder = os.path.join(PARENT_FOLDER, "data", "load", "customers.csv")

# Read the CSV
df = pd.read_csv(file_path)

# Step 2: Connect to your database (PostgreSQL in this example)
# Replace with your actual database URL
engine = create_engine('postgresql://dev:adminroot@localhost:5434/olist-dwh')

# Step 3: Insert data into the table
table_name = 'dim_customer'

# Make sure that the number of columns in the DataFrame matches the table's structure in the database
df.to_sql(table_name, engine, if_exists='append', index=False)

print("Data inserted successfully.")
