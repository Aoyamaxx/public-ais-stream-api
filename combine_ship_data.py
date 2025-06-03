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
    # Set the environment variable GOOGLE_APPLICATION_CREDENTIALS
    credentials_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "north-sea-watch-39a510f80808.json"))
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

    def get_table_data(table_name):
        """
        Retrieve all data from a table
        
        Parameters:
            table_name (str): The name of the table to retrieve data from
            
        Returns:
            pandas.DataFrame: DataFrame containing the table data
        """
        try:
            logger.info(f"Retrieving data from table: {table_name}")
            with engine.connect() as conn:
                # Get column information
                query = f"SELECT * FROM {table_name} LIMIT 0"
                df_empty = pd.read_sql_query(query, conn)
                columns = df_empty.columns.tolist()
                
                # Check if imo_number column exists
                if 'imo_number' not in columns:
                    logger.error(f"Table {table_name} does not have an imo_number column")
                    return None
                
                # Retrieve all data
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql_query(query, conn)
                logger.info(f"Retrieved {len(df)} rows from table {table_name}")
                return df
        except Exception as e:
            logger.error(f"Error retrieving data from table {table_name}: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def save_combined_data(combined_df, target_table):
        """
        Save combined DataFrame to a new table
        
        Parameters:
            combined_df (pandas.DataFrame): DataFrame containing the combined data
            target_table (str): Name of the target table
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Check if the target table exists
            if check_table_exists(target_table):
                logger.info(f"Table {target_table} exists")
                user_input = input(f"Table {target_table} exists. Do you want to overwrite it? (Y/n): ")
                if user_input.lower() != 'y' and user_input != '':
                    logger.info("User chose not to overwrite the table, the script will exit")
                    return False
                
                logger.info("User chose to overwrite the table")
                # Delete the existing table
                if not drop_table(target_table):
                    logger.error(f"Cannot delete table {target_table}, operation aborted")
                    return False
            
            # Save the combined DataFrame to the target table
            logger.info(f"Saving combined data to table {target_table}...")
            with engine.connect() as conn:
                combined_df.to_sql(
                    name=target_table,
                    con=conn,
                    if_exists='replace',
                    index=False,
                    chunksize=1000  # Batch processing to avoid memory issues
                )
            logger.info(f"Successfully saved {len(combined_df)} rows of data to table {target_table}")
            return True
        except Exception as e:
            logger.error(f"Error saving combined data: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def combine_tables():
        """
        Combine icct_scrubber_march_2025 and wfr_ship_list tables using imo_number as the key
        """
        try:
            # Table names
            icct_table = "icct_scrubber_march_2025"
            wfr_table = "wfr_ship_list"
            target_table = "icct_wfr_combined"
            
            # Get data from both tables
            icct_df = get_table_data(icct_table)
            wfr_df = get_table_data(wfr_table)
            
            if icct_df is None or wfr_df is None:
                logger.error("Failed to retrieve data from one or both tables")
                return False
            
            # Ensure imo_number columns are of the same type
            icct_df['imo_number'] = icct_df['imo_number'].astype(str)
            wfr_df['imo_number'] = wfr_df['imo_number'].astype(str)
            
            # Find common column names
            common_cols = set(icct_df.columns).intersection(set(wfr_df.columns))
            common_cols.remove('imo_number')  # Remove the key column from the common columns
            
            # Rename common columns in wfr_df to avoid conflicts during merge
            wfr_df_renamed = wfr_df.copy()
            for col in common_cols:
                wfr_df_renamed = wfr_df_renamed.rename(columns={col: f"{col}_wfr"})
            
            # Merge the dataframes on imo_number
            logger.info("Merging tables on imo_number...")
            merged_df = pd.merge(icct_df, wfr_df_renamed, on='imo_number', how='inner')
            logger.info(f"Merged data has {len(merged_df)} rows")
            
            # Reorder columns to have imo_number first, then icct data, then wfr data
            icct_cols = [col for col in icct_df.columns if col != 'imo_number']
            wfr_cols = [col for col in wfr_df_renamed.columns if col != 'imo_number']
            column_order = ['imo_number'] + icct_cols + wfr_cols
            merged_df = merged_df[column_order]
            
            # Save the combined data
            success = save_combined_data(merged_df, target_table)
            
            if success:
                # Verify the data
                with engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {target_table}"))
                    count = result.fetchone()[0]
                    logger.info(f"Table {target_table} has {count} rows of data")
                    
                    # Display the first few rows of data
                    result = conn.execute(text(f"SELECT * FROM {target_table} LIMIT 5"))
                    rows = result.fetchall()
                    for row in rows:
                        logger.info(f"Row data: {row}")
            
            return success
        except Exception as e:
            logger.error(f"Error combining tables: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def main():
        try:
            logger.info("Starting table combination process")
            success = combine_tables()
            
            if success:
                logger.info("Tables combined successfully")
            else:
                logger.error("Failed to combine tables")
                
            # Close the connection
            logger.info("Closing connection...")
            connector.close()
            logger.info("Connection closed")
        except Exception as e:
            logger.error(f"Error in main function: {str(e)}")
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
    logger.error(f"Error initializing script: {str(e)}")
    logger.error(traceback.format_exc())
    sys.exit(1) 