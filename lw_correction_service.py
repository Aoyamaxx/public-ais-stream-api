import os
import time
import psycopg2
import asyncio
import websockets
import json
import datetime
import logging
import geopandas as gpd
from shapely.geometry import Point

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Get database credentials from environment variables
DB_NAME = os.getenv("DB_NAME", "ais_data_collection")
DB_USER = os.getenv("DB_USER", "aoyamaxx")
DB_PASSWORD = os.getenv("DB_PASSWORD", "aoyamaxx")
DB_HOST = os.getenv("DB_HOST", "cloud_sql_proxy")
DB_PORT = os.getenv("DB_PORT", "5432")

API_KEY = os.getenv("API_KEY", "ad0495b8eeb54a58bb5caff12f815828d94d148c")

# Define the bounding box for the North Sea (initial filter)
NORTH_SEA_BBOX = [[50.0, -5.0], [61.5, 13.0]]

# Path to the North Sea shapefile for secondary filtering
NORTH_SEA_SHAPEFILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                  "north_sea_watch_region_patched", 
                                  "north_sea_watch_region_patched.shp")

# AIS WebSocket subscription message for L&W correction service
SUBSCRIBE_MESSAGE = {
    "APIKey": API_KEY,
    "BoundingBoxes": [NORTH_SEA_BBOX],
    "FilterMessageTypes": ["ShipStaticData"]
}

# Maximum retry attempts for database operations
MAX_DB_RETRIES = 3
# Batch size for database operations
BATCH_SIZE = 50

# Load the North Sea shapefile
def load_north_sea_shapefile():
    try:
        north_sea_shape = gpd.read_file(NORTH_SEA_SHAPEFILE)
        logging.info(f"L&W Correction Service: North Sea shapefile loaded successfully")
        return north_sea_shape
    except Exception as e:
        logging.error(f"L&W Correction Service: Failed to load North Sea shapefile: {e}")
        raise

# Check if a point is within the North Sea region
def is_point_in_north_sea(latitude, longitude, north_sea_shape):
    if latitude is None or longitude is None:
        return False
    
    try:
        point = Point(longitude, latitude)
        return any(north_sea_shape.contains(point))
    except Exception as e:
        logging.error(f"L&W Correction Service: Error checking point in North Sea: {e}")
        return False

def ensure_correction_columns():
    """Ensure lw_correction and lw_correction_timestamp columns exist in ships table"""
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    cursor = conn.cursor()

    try:
        # Check if columns exist
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'ships' AND table_schema = 'public'
            AND column_name IN ('lw_correction', 'lw_correction_timestamp')
        """)
        existing_columns = [row[0] for row in cursor.fetchall()]
        
        # Add lw_correction column if it doesn't exist
        if 'lw_correction' not in existing_columns:
            cursor.execute("""
                ALTER TABLE ships 
                ADD COLUMN lw_correction BOOLEAN DEFAULT FALSE
            """)
            logging.info("L&W Correction Service: Added lw_correction column to ships table")
        
        # Add lw_correction_timestamp column if it doesn't exist
        if 'lw_correction_timestamp' not in existing_columns:
            cursor.execute("""
                ALTER TABLE ships 
                ADD COLUMN lw_correction_timestamp TIMESTAMP
            """)
            logging.info("L&W Correction Service: Added lw_correction_timestamp column to ships table")
        
        conn.commit()
    except Exception as e:
        logging.error(f"L&W Correction Service: Error ensuring correction columns: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def calculate_correct_dimensions(dimension_data):
    """Calculate correct length and width from dimension data according to AIS standard"""
    length = None
    width = None
    
    if dimension_data:
        a = dimension_data.get("A")  # Distance from bow to GPS antenna
        b = dimension_data.get("B")  # Distance from GPS antenna to stern
        c = dimension_data.get("C")  # Distance from port side to GPS antenna
        d = dimension_data.get("D")  # Distance from GPS antenna to starboard side
        
        # Calculate total length: A + B
        if a is not None and b is not None:
            length = a + b
        
        # Calculate total width/beam: C + D
        if c is not None and d is not None:
            width = c + d
    
    return length, width

def check_ship_exists_and_needs_correction(conn, imo_number):
    """Check if ship exists in database and needs L&W correction"""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT imo_number, lw_correction 
            FROM ships 
            WHERE imo_number = %s
        """, (imo_number,))
        result = cursor.fetchone()
        
        if result is None:
            return False, False  # Ship doesn't exist, let collector handle it
        
        # Ship exists, check if correction is needed
        lw_correction = result[1]
        needs_correction = lw_correction is None or lw_correction is False
        
        return True, needs_correction
    except Exception as e:
        logging.error(f"L&W Correction Service: Error checking ship {imo_number}: {e}")
        return False, False
    finally:
        cursor.close()

def update_ship_dimensions(conn, imo_number, length, width):
    """Update ship dimensions and mark as corrected"""
    cursor = conn.cursor()
    try:
        correction_timestamp = datetime.datetime.utcnow()
        
        cursor.execute("""
            UPDATE ships 
            SET length = %s, 
                width = %s, 
                lw_correction = TRUE, 
                lw_correction_timestamp = %s
            WHERE imo_number = %s
        """, (length, width, correction_timestamp, imo_number))
        
        conn.commit()
        logging.info(f"L&W Correction Service: Updated dimensions for IMO {imo_number}: Length={length}, Width={width}")
        return True
    except Exception as e:
        logging.error(f"L&W Correction Service: Error updating dimensions for IMO {imo_number}: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()

async def run_lw_correction_service():
    """Main L&W correction service that runs continuously"""
    # Create database connection
    conn = None
    for attempt in range(MAX_DB_RETRIES):
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
            )
            conn.autocommit = False
            break
        except psycopg2.Error as e:
            logging.error(f"L&W Correction Service: Database connection attempt {attempt+1} failed: {e}")
            if attempt == MAX_DB_RETRIES - 1:
                logging.error("L&W Correction Service: Maximum database connection attempts reached. Exiting.")
                return
            await asyncio.sleep(2 ** attempt)

    # Ensure correction columns exist
    ensure_correction_columns()

    # Load North Sea shapefile
    north_sea_shape = load_north_sea_shapefile()
    
    # Statistics
    processed_count = 0
    corrected_count = 0
    ignored_count = 0
    start_time = time.time()
    
    # Connection retry settings
    min_retry_delay = 1
    max_retry_delay = 60
    retry_attempts = 0

    while True:
        try:
            if retry_attempts > 0:
                retry_delay = min(max_retry_delay, min_retry_delay * (2 ** (retry_attempts - 1)))
                logging.info(f"L&W Correction Service: Waiting {retry_delay:.2f} seconds before reconnecting...")
                await asyncio.sleep(retry_delay)
            
            retry_attempts += 1
            logging.info(f"L&W Correction Service: Connecting to AIS WebSocket (attempt {retry_attempts})...")
            
            async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
                logging.info("L&W Correction Service: WebSocket connection established")
                await websocket.send(json.dumps(SUBSCRIBE_MESSAGE))
                logging.info("L&W Correction Service: Subscription sent for ShipStaticData in North Sea")
                
                retry_attempts = 0
                
                async for message_json in websocket:
                    try:
                        # Check database connection
                        if conn.closed:
                            logging.warning("L&W Correction Service: Database connection lost. Reconnecting...")
                            conn = psycopg2.connect(
                                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
                            )
                        
                        message = json.loads(message_json)
                        
                        if message.get("MessageType") != "ShipStaticData":
                            continue
                        
                        metadata = message["MetaData"]
                        latitude = metadata.get("latitude")
                        longitude = metadata.get("longitude")
                        
                        # Skip if position is not available
                        if latitude is None or longitude is None:
                            continue
                        
                        # Secondary filtering - check if point is within North Sea shapefile
                        if not is_point_in_north_sea(latitude, longitude, north_sea_shape):
                            continue
                        
                        ship_data = message["Message"]["ShipStaticData"]
                        raw_imo_number = ship_data.get("ImoNumber")
                        
                        # Only process ships with valid IMO numbers
                        if raw_imo_number is None or raw_imo_number == 0:
                            continue
                        
                        processed_count += 1
                        
                        # Check if ship exists and needs correction
                        exists, needs_correction = check_ship_exists_and_needs_correction(conn, raw_imo_number)
                        
                        if not exists:
                            # Ship doesn't exist in database, let collector handle it
                            ignored_count += 1
                            continue
                        
                        if not needs_correction:
                            # Ship already corrected
                            ignored_count += 1
                            continue
                        
                        # Ship exists and needs correction
                        dimension = ship_data.get("Dimension", {})
                        length, width = calculate_correct_dimensions(dimension)
                        
                        # Only update if we have valid dimension data
                        if length is not None or width is not None:
                            if update_ship_dimensions(conn, raw_imo_number, length, width):
                                corrected_count += 1
                        
                        # Log statistics every 5 minutes
                        current_time = time.time()
                        if current_time - start_time >= 300:
                            logging.info(f"L&W Correction Service: Processed {processed_count} ships, "
                                       f"Corrected {corrected_count}, Ignored {ignored_count}")
                            start_time = current_time
                            
                    except json.JSONDecodeError:
                        continue
                    except psycopg2.Error as e:
                        logging.error(f"L&W Correction Service: Database error: {e}")
                        conn.rollback()
                        
                        if isinstance(e, psycopg2.OperationalError):
                            try:
                                conn = psycopg2.connect(
                                    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
                                )
                            except psycopg2.Error as reconnect_err:
                                logging.error(f"L&W Correction Service: Failed to reconnect: {reconnect_err}")
                                await asyncio.sleep(5)
                    except Exception as e:
                        logging.error(f"L&W Correction Service: Unexpected error: {e}")
                        continue

        except websockets.exceptions.ConnectionClosedError as e:
            logging.error(f"L&W Correction Service: WebSocket connection closed: {e}")
        except websockets.exceptions.WebSocketException as e:
            logging.error(f"L&W Correction Service: WebSocket error: {e}")
        except Exception as e:
            logging.error(f"L&W Correction Service: Unexpected error: {e}")

if __name__ == "__main__":
    try:
        logging.info("L&W Correction Service: Starting L&W correction service...")
        
        # Verify shapefile exists
        if not os.path.exists(NORTH_SEA_SHAPEFILE):
            logging.error(f"L&W Correction Service: North Sea shapefile not found: {NORTH_SEA_SHAPEFILE}")
            exit(1)
        
        asyncio.run(run_lw_correction_service())
        
    except KeyboardInterrupt:
        logging.info("L&W Correction Service: Service stopped by user")
    except Exception as e:
        logging.error(f"L&W Correction Service: Failed to start service: {e}") 