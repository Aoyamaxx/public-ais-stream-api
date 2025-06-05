import os
import pandas as pd
import asyncio
import sys
from google.cloud.sql.connector import Connector
from sqlalchemy import create_engine, text, inspect

# Configure event loop policy for Windows
if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Create data directory if it doesn't exist
data_dir = "data"
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

# Set the environment variable for Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "north-sea-watch-d8ad3753e506.json"

# Database connection parameters
DB_NAME = os.getenv("DB_NAME", "ais_data_collection")
DB_USER = os.getenv("DB_USER", "aoyamaxx")
DB_PASSWORD = os.getenv("DB_PASSWORD", "aoyamaxx")
INSTANCE_CONNECTION_NAME = "north-sea-watch:europe-west4:ais-database"

# Create a connection object
connector = Connector()

# Define connection function
def getconn():
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME,
    )
    return conn

# Create database engine
engine = create_engine("postgresql+pg8000://", creator=getconn)

try:
    # Get all table names
    with engine.connect() as conn:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        print(f"Found {len(tables)} tables in the database.")
        
        # Download each table as CSV
        for table in tables:
            print(f"Downloading table: {table}")
            
            # Read the entire table
            query = f'SELECT * FROM "{table}"'
            df = pd.read_sql(text(query), conn)
            
            # Save to CSV
            csv_path = os.path.join(data_dir, f"{table}.csv")
            df.to_csv(csv_path, index=False)
            print(f"Saved {table} to {csv_path} ({len(df)} rows)")

finally:
    # Close the connector
    connector.close()

print("\nAll tables have been downloaded successfully!") 