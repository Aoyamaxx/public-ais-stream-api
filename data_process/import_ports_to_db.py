import os
import pandas as pd
import logging
from google.cloud.sql.connector import Connector
from sqlalchemy import create_engine, text, inspect
import platform
import asyncio
import sys
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set the correct event loop policy for Windows
if platform.system() == 'Windows':
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        logger.info("Successfully set the Windows-compatible event loop policy")
    except Exception as e:
        logger.error(f"Error setting the event loop policy: {str(e)}")

logger.info("Script started execution")

# Set the environment variable GOOGLE_APPLICATION_CREDENTIALS to the path of the JSON file
credentials_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "north-sea-watch-39a510f80808.json"))
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
logger.info(f"Using credentials file: {credentials_path}")

if not os.path.exists(credentials_path):
    logger.error(f"Credentials file does not exist: {credentials_path}")
    sys.exit(1)

# Database connection settings
DB_NAME = os.getenv("DB_NAME", "ais_data_collection")
DB_USER = os.getenv("DB_USER", "aoyamaxx")
DB_PASSWORD = os.getenv("DB_PASSWORD", "aoyamaxx")
INSTANCE_CONNECTION_NAME = "north-sea-watch:europe-west4:ais-database"

logger.info(f"Database connection information: DB_NAME={DB_NAME}, DB_USER={DB_USER}, INSTANCE={INSTANCE_CONNECTION_NAME}")

# Create the connection object
logger.info("Creating Connector object...")
connector = Connector()
logger.info("Connector object created successfully")

# Define the function to return the connection object
def getconn():
    try:
        logger.info("Attempting to connect to the database...")
        conn = connector.connect(
            INSTANCE_CONNECTION_NAME,
            "pg8000",
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME,
        )
        logger.info("Database connection successful")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to the database: {str(e)}")
        logger.error(traceback.format_exc())
        raise

# Create the engine object
logger.info("Creating SQLAlchemy engine...")
engine = create_engine("postgresql+pg8000://", creator=getconn)
logger.info("SQLAlchemy engine created successfully")

def check_table_exists(table_name):
    """
    Check if the table exists in the database
    
    Parameters:
        table_name (str): The name of the table to check
        
    Returns:
        bool: If the table exists, return True, otherwise return False
    """
    try:
        inspector = inspect(engine)
        return table_name in inspector.get_table_names()
    except Exception as e:
        logger.error(f"Error checking if the table exists: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def drop_table(table_name):
    """
    Drop the table from the database
    
    Parameters:
        table_name (str): The name of the table to drop
        
    Returns:
        bool: If the table is successfully deleted, return True, otherwise return False
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            conn.commit()
        logger.info(f"Table {table_name} has been successfully deleted")
        return True
    except Exception as e:
        logger.error(f"Error deleting table: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def import_ports_to_db():
    """
    Import the filtered_port.csv file into the ports table
    """
    table_name = "ports"
    
    try:
        # Get the CSV file path
        current_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(current_dir, 'filtered_port.csv')
        
        # Check if the file exists
        if not os.path.exists(csv_path):
            logger.error(f"File does not exist: {csv_path}")
            return False
            
        # Check if the table exists
        if check_table_exists(table_name):
            logger.info(f"Table {table_name} exists")
            user_input = input(f"Table {table_name} exists. Do you want to overwrite it? (Y/n): ")
            if user_input.lower() != 'y' and user_input != '':
                logger.info("User chose not to overwrite the table, the script will exit")
                return False
            
            logger.info("User chose to overwrite the table")
            # Delete the existing table
            if not drop_table(table_name):
                logger.error(f"Cannot delete table {table_name}, import aborted")
                return False
        
        # Read the CSV file
        logger.info(f"Reading CSV file: {csv_path}")
        df = pd.read_csv(csv_path)
        logger.info(f"CSV file read successfully, {len(df)} rows of data")
        
        # Load scrubber status from port_bans.csv if it exists
        port_bans_path = os.path.join(current_dir, 'port_bans.csv')
        if os.path.exists(port_bans_path):
            logger.info(f"Loading scrubber status from {port_bans_path}")
            port_bans_df = pd.read_csv(port_bans_path)
            
            # Create a dictionary mapping port names to their scrubber status
            port_status_dict = dict(zip(port_bans_df['port_name'], port_bans_df['scrubber_status']))
            
            # Add SCRUBBER_STATUS column to ports dataframe
            df['SCRUBBER_STATUS'] = df['PORT_NAME'].map(port_status_dict).fillna(0).astype(int)
            logger.info("Applied scrubber status from port_bans.csv")
            
            # Log the number of ports with different scrubber statuses
            status_counts = df['SCRUBBER_STATUS'].value_counts()
            logger.info(f"Scrubber status distribution: {status_counts.to_dict()}")
        else:
            logger.info("port_bans.csv not found, setting default scrubber status to 0")
            # Set default scrubber status to 0 for all ports
            df['SCRUBBER_STATUS'] = 0
        
        # Clean column names
        logger.info("Cleaning column names...")
        df.columns = [col.strip().replace(' ', '_').replace('(', '').replace(')', '').replace(',', '').lower() for col in df.columns]
        logger.info(f"Column names cleaned, column names: {', '.join(df.columns)}")
        
        # Check for NA values and handle them properly
        logger.info("Checking for NA values in the dataframe...")
        na_counts = df.isna().sum()
        columns_with_na = na_counts[na_counts > 0].index.tolist()
        if columns_with_na:
            logger.info(f"Found NA values in columns: {', '.join(columns_with_na)}")
            logger.info("NA values will be imported as NULL values in the database")
        
        # Connect to the database and import the data
        logger.info(f"Importing data into table {table_name}...")
        try:
            with engine.connect() as conn:
                # Write the DataFrame to the database
                # Using dtype=None to let PostgreSQL handle the data types
                df.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists='replace',
                    index=False,
                    chunksize=1000,  # Batch processing to avoid memory issues
                    dtype=None,  # Let PostgreSQL handle the data types
                    method='multi'  # Use the multi insert method which can better handle NULL values
                )
            logger.info(f"Successfully imported {len(df)} rows of data into table {table_name}")
            
            # Verify the import
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.fetchone()[0]
                logger.info(f"Table {table_name} has {count} rows of data")
                
                # Display the first few rows of data
                result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT 5"))
                rows = result.fetchall()
                for row in rows:
                    logger.info(f"Row data: {row}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error importing data into the database: {str(e)}")
            logger.error(traceback.format_exc())
            return False
        
    except Exception as e:
        logger.error(f"Error importing ports data: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def main():
    try:
        logger.info("Starting main function")
        success = import_ports_to_db()
        
        if success:
            logger.info("Successfully imported ports data to database")
        else:
            logger.error("Failed to import ports data to database")
        
        # Close the connection
        logger.info("Closing connection...")
        connector.close()
        logger.info("Connection closed")
        
    except Exception as e:
        logger.error(f"Error executing main function: {str(e)}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    try:
        main()
        logger.info("Script execution completed")
    except Exception as e:
        logger.error(f"Error executing script: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1) 