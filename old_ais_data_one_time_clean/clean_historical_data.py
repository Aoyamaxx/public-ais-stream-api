#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
One-time script to clean historical AIS data in database.
This script filters entries in ship_data and unknown_ships tables,
keeping only ships that are within the North Sea region as defined by shapefile.
"""

import os
import sys
import time
import logging
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm
import argparse
import platform
import asyncio
import traceback
from google.cloud.sql.connector import Connector
from sqlalchemy import create_engine, text

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), "historical_data_clean.log"), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Set the correct event loop policy for Windows
if platform.system() == 'Windows':
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        logger.info("Successfully set the Windows-compatible event loop policy")
    except Exception as e:
        logger.error(f"Error setting the event loop policy: {str(e)}")

logger.info("Script started execution")

# Define path to project root (parent directory)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Path to the service account key for Cloud SQL
CREDENTIALS_PATH = os.path.abspath(os.path.join(PROJECT_ROOT, "north-sea-watch-39a510f80808.json"))
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
logger.info(f"Using credentials file: {CREDENTIALS_PATH}")

if not os.path.exists(CREDENTIALS_PATH):
    logger.error(f"Credentials file does not exist: {CREDENTIALS_PATH}")
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

# Path to the North Sea shapefile
NORTH_SEA_SHAPEFILE = os.path.join(
    PROJECT_ROOT,
    "north_sea_watch_region_merged_single_feature",
    "north_sea_watch_region_merged_single_feature.shp"
)

def load_north_sea_shapefile():
    """Load the North Sea region shapefile for filtering"""
    try:
        north_sea_shape = gpd.read_file(NORTH_SEA_SHAPEFILE)
        logger.info(f"Successfully loaded North Sea shapefile: {NORTH_SEA_SHAPEFILE}")
        logger.info(f"Shapefile contains {len(north_sea_shape)} feature")
        return north_sea_shape
    except Exception as e:
        logger.error(f"Failed to load North Sea shapefile: {e}")
        logger.error(traceback.format_exc())
        raise

def is_point_in_north_sea(latitude, longitude, north_sea_shape):
    """Check if a given latitude/longitude point is within the North Sea region"""
    if latitude is None or longitude is None:
        return False
    
    try:
        point = Point(longitude, latitude)  # GIS coordinates are (longitude, latitude)
        return any(north_sea_shape.contains(point))
    except Exception as e:
        logger.error(f"Error checking if point is in North Sea: {e}")
        return False

def get_record_counts():
    """Get counts of records in the tables before cleaning"""
    try:
        with engine.connect() as conn:
            ship_data_result = conn.execute(text("SELECT COUNT(*) FROM ship_data"))
            ship_data_count = ship_data_result.fetchone()[0]
            
            unknown_ships_result = conn.execute(text("SELECT COUNT(*) FROM unknown_ships"))
            unknown_ships_count = unknown_ships_result.fetchone()[0]
            
            return ship_data_count, unknown_ships_count
    except Exception as e:
        logger.error(f"Database error getting record counts: {e}")
        logger.error(traceback.format_exc())
        return 0, 0

def clean_ship_data(batch_size=5000, dry_run=False):
    """
    Clean the ship_data table, removing entries outside the North Sea region.
    
    Args:
        batch_size (int): Number of records to process in a batch
        dry_run (bool): If True, only simulate deletion without actually removing data
    """
    north_sea_shape = load_north_sea_shapefile()
    
    try:
        with engine.connect() as conn:
            # Get total count
            result = conn.execute(text("SELECT COUNT(*) FROM ship_data"))
            total_count = result.fetchone()[0]
            logger.info(f"Total records in ship_data: {total_count}")
            
            # Process in batches
            offset = 0
            deleted_count = 0
            kept_count = 0
            
            while offset < total_count:
                result = conn.execute(
                    text("SELECT id, latitude, longitude FROM ship_data ORDER BY id LIMIT :limit OFFSET :offset"),
                    {"limit": batch_size, "offset": offset}
                )
                records = result.fetchall()
                
                if not records:
                    break
                
                to_delete = []
                
                for record in tqdm(records, desc=f"Processing ship_data (batch {offset//batch_size + 1})", unit="records"):
                    record_id, latitude, longitude = record
                    if not is_point_in_north_sea(latitude, longitude, north_sea_shape):
                        to_delete.append(record_id)
                
                if to_delete and not dry_run:
                    # Delete in smaller sub-batches to avoid overloading the database
                    for i in range(0, len(to_delete), 1000):
                        sub_batch = to_delete[i:i+1000]
                        if len(sub_batch) == 1:
                            # Handle single item case
                            delete_query = text(f"DELETE FROM ship_data WHERE id = {sub_batch[0]}")
                        else:
                            # Handle multiple items
                            sub_batch_str = ','.join(str(id) for id in sub_batch)
                            delete_query = text(f"DELETE FROM ship_data WHERE id IN ({sub_batch_str})")
                        
                        conn.execute(delete_query)
                        conn.commit()
                
                deleted_count += len(to_delete)
                kept_count += len(records) - len(to_delete)
                
                logger.info(f"Batch {offset//batch_size + 1}: {len(to_delete)} records marked for deletion")
                if dry_run:
                    logger.info("DRY RUN - No actual deletions performed")
                    
                offset += batch_size
            
            # Final stats
            if total_count > 0:
                deleted_percent = (deleted_count / total_count) * 100
                kept_percent = (kept_count / total_count) * 100
            else:
                deleted_percent = kept_percent = 0
                
            logger.info(f"{'WOULD HAVE ' if dry_run else ''}Deleted {deleted_count} records from ship_data ({deleted_percent:.2f}%)")
            logger.info(f"Kept {kept_count} records in ship_data ({kept_percent:.2f}%)")
            
    except Exception as e:
        logger.error(f"Error cleaning ship_data: {e}")
        logger.error(traceback.format_exc())

def clean_unknown_ships(batch_size=5000, dry_run=False):
    """
    Clean the unknown_ships table, removing entries outside the North Sea region.
    
    Args:
        batch_size (int): Number of records to process in a batch
        dry_run (bool): If True, only simulate deletion without actually removing data
    """
    north_sea_shape = load_north_sea_shapefile()
    
    try:
        with engine.connect() as conn:
            # Get total count
            result = conn.execute(text("SELECT COUNT(*) FROM unknown_ships"))
            total_count = result.fetchone()[0]
            logger.info(f"Total records in unknown_ships: {total_count}")
            
            # Process in batches
            offset = 0
            deleted_count = 0
            kept_count = 0
            
            while offset < total_count:
                result = conn.execute(
                    text("SELECT id, latitude, longitude FROM unknown_ships ORDER BY id LIMIT :limit OFFSET :offset"),
                    {"limit": batch_size, "offset": offset}
                )
                records = result.fetchall()
                
                if not records:
                    break
                
                to_delete = []
                
                for record in tqdm(records, desc=f"Processing unknown_ships (batch {offset//batch_size + 1})", unit="records"):
                    record_id, latitude, longitude = record
                    if not is_point_in_north_sea(latitude, longitude, north_sea_shape):
                        to_delete.append(record_id)
                
                if to_delete and not dry_run:
                    # Delete in smaller sub-batches to avoid overloading the database
                    for i in range(0, len(to_delete), 1000):
                        sub_batch = to_delete[i:i+1000]
                        if len(sub_batch) == 1:
                            # Handle single item case
                            delete_query = text(f"DELETE FROM unknown_ships WHERE id = {sub_batch[0]}")
                        else:
                            # Handle multiple items
                            sub_batch_str = ','.join(str(id) for id in sub_batch)
                            delete_query = text(f"DELETE FROM unknown_ships WHERE id IN ({sub_batch_str})")
                        
                        conn.execute(delete_query)
                        conn.commit()
                
                deleted_count += len(to_delete)
                kept_count += len(records) - len(to_delete)
                
                logger.info(f"Batch {offset//batch_size + 1}: {len(to_delete)} records marked for deletion")
                if dry_run:
                    logger.info("DRY RUN - No actual deletions performed")
                    
                offset += batch_size
            
            # Final stats
            if total_count > 0:
                deleted_percent = (deleted_count / total_count) * 100
                kept_percent = (kept_count / total_count) * 100
            else:
                deleted_percent = kept_percent = 0
                
            logger.info(f"{'WOULD HAVE ' if dry_run else ''}Deleted {deleted_count} records from unknown_ships ({deleted_percent:.2f}%)")
            logger.info(f"Kept {kept_count} records in unknown_ships ({kept_percent:.2f}%)")
            
    except Exception as e:
        logger.error(f"Error cleaning unknown_ships: {e}")
        logger.error(traceback.format_exc())

def main():
    """Main function to clean historical AIS data"""
    parser = argparse.ArgumentParser(description="Clean historical AIS data based on North Sea shapefile")
    parser.add_argument("--batch-size", type=int, default=5000, help="Number of records to process in each batch")
    parser.add_argument("--dry-run", action="store_true", help="Simulate the cleaning without actually deleting data")
    parser.add_argument("--skip-ship-data", action="store_true", help="Skip processing the ship_data table")
    parser.add_argument("--skip-unknown-ships", action="store_true", help="Skip processing the unknown_ships table")
    
    args = parser.parse_args()
    
    # Verify shapefile exists
    if not os.path.exists(NORTH_SEA_SHAPEFILE):
        logger.error(f"North Sea shapefile not found at: {NORTH_SEA_SHAPEFILE}")
        sys.exit(1)
    
    start_time = time.time()
    logger.info("Starting historical AIS data cleaning process")
    
    if args.dry_run:
        logger.info("DRY RUN MODE - No actual deletions will be performed")
    
    try:
        # Get initial counts
        ship_data_count_before, unknown_ships_count_before = get_record_counts()
        logger.info(f"Initial counts - ship_data: {ship_data_count_before}, unknown_ships: {unknown_ships_count_before}")
        
        # Clean tables
        if not args.skip_ship_data:
            logger.info("Cleaning ship_data table...")
            clean_ship_data(batch_size=args.batch_size, dry_run=args.dry_run)
        else:
            logger.info("Skipping ship_data table as requested")
        
        if not args.skip_unknown_ships:
            logger.info("Cleaning unknown_ships table...")
            clean_unknown_ships(batch_size=args.batch_size, dry_run=args.dry_run)
        else:
            logger.info("Skipping unknown_ships table as requested")
        
        # Get final counts if not in dry run mode
        if not args.dry_run:
            ship_data_count_after, unknown_ships_count_after = get_record_counts()
            logger.info(f"Final counts - ship_data: {ship_data_count_after}, unknown_ships: {unknown_ships_count_after}")
            logger.info(f"Removed {ship_data_count_before - ship_data_count_after} records from ship_data")
            logger.info(f"Removed {unknown_ships_count_before - unknown_ships_count_after} records from unknown_ships")
    
    except Exception as e:
        logger.error(f"Error in cleaning process: {e}")
        logger.error(traceback.format_exc())
    
    finally:
        # Close the connection
        logger.info("Closing connection...")
        connector.close()
        logger.info("Connection closed")
        
        elapsed_time = time.time() - start_time
        logger.info(f"Cleaning process completed in {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    try:
        main()
        logger.info("Script execution completed")
    except Exception as e:
        logger.error(f"Error executing script: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1) 