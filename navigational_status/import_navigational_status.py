import os
import pandas as pd
from google.cloud.sql.connector import Connector
from sqlalchemy import create_engine, text, inspect
import logging
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

try:
    # Set the environment variable GOOGLE_APPLICATION_CREDENTIALS to the path of the JSON file
    credentials_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "north-sea-watch-39a510f80808.json"))
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    logger.info(f"Using credentials file: {credentials_path}")
    
    if not os.path.exists(credentials_path):
        logger.error(f"Credentials file does not exist: {credentials_path}")
        sys.exit(1)

    # Set the environment variables
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

    def import_navigational_status_csv(csv_filename, table_name, if_exists='replace'):
        """
        Import the navigational status CSV file into the database
        
        Parameters:
            csv_filename (str): CSV file name (located in the navigational_status directory)
            table_name (str): Name of the table to create or update
            if_exists (str): Action to take if the table exists ('fail', 'replace', 'append')
        """
        try:
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
                
                # Set if_exists to 'replace' to ensure the table is replaced
                if_exists = 'replace'
            
            # Build the full path to the CSV file
            csv_path = os.path.join(os.path.dirname(__file__), csv_filename)
            
            # Check if the file exists
            if not os.path.exists(csv_path):
                logger.error(f"File does not exist: {csv_path}")
                return False
            
            # Read the CSV file
            logger.info(f"Reading CSV file: {csv_path}")
            df = pd.read_csv(csv_path)
            logger.info(f"CSV file read successfully, {len(df)} rows of data")
            
            # Clean the column names (remove spaces, replace special characters)
            logger.info("Cleaning column names...")
            df.columns = [col.strip().replace(' ', '_').replace('(', '').replace(')', '').replace(',', '').lower() for col in df.columns]
            logger.info(f"Column names cleaned, column names: {', '.join(df.columns)}")
            
            # Connect to the database and import the data
            logger.info(f"Importing data into table {table_name}...")
            try:
                with engine.connect() as conn:
                    # Write the DataFrame to the database
                    df.to_sql(
                        name=table_name,
                        con=conn,
                        if_exists=if_exists,
                        index=False,
                        chunksize=1000  # Batch processing to avoid memory issues
                    )
                logger.info(f"Successfully imported {len(df)} rows of data into table {table_name}")
                return True
            except Exception as e:
                logger.error(f"Error importing data into the database: {str(e)}")
                logger.error(traceback.format_exc())
                return False
            
        except Exception as e:
            logger.error(f"Error importing data: {str(e)}")
            logger.error(traceback.format_exc())
            return False
        
    def main():
        try:
            logger.info("Starting main function")
            # Define the CSV file name and target table name
            csv_filename = "navigational_status_code.csv"
            table_name = "navigational_status"
            
            logger.info(f"Importing file {csv_filename} to table {table_name}")
            
            # Import the CSV file to the database
            success = import_navigational_status_csv(csv_filename, table_name)
            
            if success:
                logger.info("Successfully imported, verifying data...")
                # Verify if the import was successful
                try:
                    with engine.connect() as conn:
                        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                        count = result.fetchone()[0]
                        logger.info(f"Table {table_name} has {count} rows of data")
                        
                        # Display the first few rows of data
                        result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT 5"))
                        rows = result.fetchall()
                        for row in rows:
                            logger.info(f"Row data: {row}")
                except Exception as e:
                    logger.error(f"Error verifying data: {str(e)}")
                    logger.error(traceback.format_exc())
            else:
                logger.error("Import failed")
            
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
except Exception as e:
    logger.error(f"Error initializing: {str(e)}")
    logger.error(traceback.format_exc())
    sys.exit(1) 