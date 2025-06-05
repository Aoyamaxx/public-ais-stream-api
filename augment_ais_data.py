import pandas as pd
import os
from sqlalchemy import create_engine, text
from google.cloud.sql.connector import Connector
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Database connection parameters
DB_NAME = os.getenv("DB_NAME", "ais_data_collection")
DB_USER = os.getenv("DB_USER", "aoyamaxx")
DB_PASSWORD = os.getenv("DB_PASSWORD", "aoyamaxx")
INSTANCE_CONNECTION_NAME = "north-sea-watch:europe-west4:ais-database"

# Navigational status to operation mode mapping
STATUS_TO_MODE = {
    0: 'Cruise',
    1: 'Anchor',
    2: 'Anchor',
    3: 'Maneuver',
    4: 'Maneuver',
    5: 'Berth',
    6: 'Berth',
    7: 'Cruise',
    8: 'Cruise',
    9: 'Cruise',
    10: 'Cruise',
    11: 'Cruise',
    12: 'Cruise',
    13: 'Cruise',
    14: 'Cruise',
    15: 'Cruise'
}

def get_db_connection():
    """Create and return a database connection."""
    try:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "north-sea-watch-39a510f80808.json"
        
        connector = Connector()
        
        def getconn():
            conn = connector.connect(
                INSTANCE_CONNECTION_NAME,
                "pg8000",
                user=DB_USER,
                password=DB_PASSWORD,
                db=DB_NAME,
            )
            return conn
        
        engine = create_engine("postgresql+pg8000://", creator=getconn)
        return engine, connector
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        raise

def get_emission_data(engine, imo_numbers):
    """Fetch emission data for the given IMO numbers."""
    try:
        query = """
        SELECT 
            imo_number::text as imo_number,
            emission_berth,
            emission_anchor,
            emission_maneuver,
            emission_cruise
        FROM ships
        WHERE imo_number::text = ANY(:imo_numbers)
        """
        return pd.read_sql(text(query), engine, params={'imo_numbers': imo_numbers})
    except Exception as e:
        logging.error(f"Error fetching emission data: {e}")
        raise

def main():
    # Read the parquet file
    logging.info("Reading parquet file...")
    df = pd.read_parquet('data/processed_ais_data_20250419_20250519_sampled_100.parquet')
    
    # Convert navigational status codes to operation modes
    logging.info("Converting navigational status codes to operation modes...")
    df['operation_mode'] = df['navigational_status_code'].map(STATUS_TO_MODE)
    
    # Convert IMO numbers to strings in the main dataframe
    df['imo_number'] = df['imo_number'].astype(str)
    
    # Get unique IMO numbers for scrubber ships only
    scrubber_ships = df[df['has_scrubber'] == True]
    unique_imo_numbers = scrubber_ships['imo_number'].unique().tolist()
    
    logging.info(f"Found {len(unique_imo_numbers)} unique scrubber ships")
    
    if unique_imo_numbers:
        # Fetch emission data
        logging.info("Fetching emission data from database...")
        engine, connector = get_db_connection()
        emission_df = get_emission_data(engine, unique_imo_numbers)
        connector.close()
        
        # Merge emission data with main dataframe
        logging.info("Merging emission data...")
        # First, drop any existing emission columns to avoid duplicates
        df = df.drop(columns=[col for col in df.columns if col.startswith('emission_')])
        # Then merge with the new emission data
        df = df.merge(emission_df, on='imo_number', how='left')
        
        # Count how many ships got emission data
        ships_with_emissions = df[df['emission_berth'].notna()]['imo_number'].nunique()
        logging.info(f"Successfully added emission data for {ships_with_emissions} ships")
    
    # Save augmented data
    output_file = 'data/augmented_ais_data.parquet'
    logging.info(f"Saving augmented data to {output_file}...")
    df.to_parquet(output_file)
    logging.info("Done!")

if __name__ == "__main__":
    main() 