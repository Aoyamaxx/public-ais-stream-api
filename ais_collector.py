import os
import time
import psycopg2
import asyncio
import websockets
import json
import datetime
import logging
import re
import geopandas as gpd
from shapely.geometry import Point
import random

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Flag to control whether to save vessels without IMO numbers to unknown_ships table
# Set to True to save ships without IMO, False to ignore them
SAVE_NO_IMO_VESSELS = False # Disabled no IMO vessels recording on 13/04/2025
logging.info(f"Running in {'SAVE_NO_IMO_VESSELS=True' if SAVE_NO_IMO_VESSELS else 'SAVE_NO_IMO_VESSELS=False'} mode")

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

# AIS WebSocket subscription message
SUBSCRIBE_MESSAGE = {
    "APIKey": API_KEY,
    "BoundingBoxes": [NORTH_SEA_BBOX],
    "FilterMessageTypes": ["ShipStaticData", "PositionReport"]
}

# Maximum retry attempts for database operations
MAX_DB_RETRIES = 3
# Maximum batch size for database operations
BATCH_SIZE = 100

# Load the North Sea shapefile
def load_north_sea_shapefile():
    try:
        north_sea_shape = gpd.read_file(NORTH_SEA_SHAPEFILE)
        logging.info(f"Successfully loaded North Sea shapefile: {NORTH_SEA_SHAPEFILE}")
        return north_sea_shape
    except Exception as e:
        logging.error(f"Failed to load North Sea shapefile: {e}")
        raise

# Check if a point is within the North Sea region
def is_point_in_north_sea(latitude, longitude, north_sea_shape):
    if latitude is None or longitude is None:
        return False
    
    try:
        point = Point(longitude, latitude)  # GIS coordinates are (longitude, latitude)
        return any(north_sea_shape.contains(point))
    except Exception as e:
        logging.error(f"Error checking if point is in North Sea: {e}")
        return False

def create_tables():
    """
    Create necessary database tables if they do not exist.
    1) ships (static data for ships with valid IMO)
    2) ship_data (dynamic data for ships with valid IMO)
    3) unknown_ships (for entries with IMO=0 or None; merged static+dynamic info)
    """
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    cursor = conn.cursor()

    # Table for valid IMO ships
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ships (
            imo_number BIGINT PRIMARY KEY,
            mmsi BIGINT,
            name TEXT,
            ship_type TEXT,
            length INTEGER,
            width INTEGER,
            max_draught NUMERIC(5,2)
        );
    """)

    # Table for dynamic data
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ship_data (
            id SERIAL PRIMARY KEY,
            imo_number BIGINT REFERENCES ships(imo_number),
            timestamp_collected TIMESTAMP DEFAULT NOW(),
            timestamp_ais TIMESTAMP,
            latitude NUMERIC(10,6),
            longitude NUMERIC(10,6),
            destination TEXT,
            sog NUMERIC(5,2),
            cog NUMERIC(5,2),
            navigational_status_code INTEGER,
            rate_of_turn NUMERIC(5,2),
            true_heading INTEGER
        );
    """)

    # Table for unknown or 0-IMO ships (merged static + dynamic fields)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS unknown_ships (
            id SERIAL PRIMARY KEY,
            imo_number BIGINT,  -- -1 if originally None, or 0 if IMO=0
            mmsi BIGINT,
            name TEXT,
            ship_type TEXT,
            length INTEGER,
            width INTEGER,
            max_draught NUMERIC(5,2),
            destination TEXT,
            timestamp_collected TIMESTAMP DEFAULT NOW(),
            timestamp_ais TIMESTAMP,
            latitude NUMERIC(10,6),
            longitude NUMERIC(10,6),
            sog NUMERIC(5,2),
            cog NUMERIC(5,2),
            navigational_status_code INTEGER,
            rate_of_turn NUMERIC(5,2),
            true_heading INTEGER
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Tables created successfully.")

def migrate_database():
    """
    Run a comprehensive database migration to ensure all tables and columns exist.
    This function:
    1. Checks if all required tables exist and creates missing ones
    2. Verifies all required columns exist in each table
    3. Adds any missing columns with appropriate data types
    
    This ensures the database is always in sync with the code requirements.
    """
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    cursor = conn.cursor()
    
    # Define the required schema for verification
    table_schemas = {
        "ships": {
            "imo_number": "BIGINT PRIMARY KEY",
            "mmsi": "BIGINT",
            "name": "TEXT",
            "ship_type": "TEXT",
            "length": "INTEGER",
            "width": "INTEGER",
            "max_draught": "NUMERIC(5,2)"
        },
        "ship_data": {
            "id": "SERIAL PRIMARY KEY",
            "imo_number": "BIGINT REFERENCES ships(imo_number)",
            "timestamp_collected": "TIMESTAMP DEFAULT NOW()",
            "timestamp_ais": "TIMESTAMP",
            "latitude": "NUMERIC(10,6)",
            "longitude": "NUMERIC(10,6)",
            "destination": "TEXT",
            "sog": "NUMERIC(5,2)",
            "cog": "NUMERIC(5,2)",
            "navigational_status_code": "INTEGER",
            "rate_of_turn": "NUMERIC(5,2)",
            "true_heading": "INTEGER"
        },
        "unknown_ships": {
            "id": "SERIAL PRIMARY KEY",
            "imo_number": "BIGINT",  
            "mmsi": "BIGINT",
            "name": "TEXT",
            "ship_type": "TEXT",
            "length": "INTEGER",
            "width": "INTEGER",
            "max_draught": "NUMERIC(5,2)",
            "destination": "TEXT",
            "timestamp_collected": "TIMESTAMP DEFAULT NOW()",
            "timestamp_ais": "TIMESTAMP",
            "latitude": "NUMERIC(10,6)",
            "longitude": "NUMERIC(10,6)",
            "sog": "NUMERIC(5,2)",
            "cog": "NUMERIC(5,2)",
            "navigational_status_code": "INTEGER",
            "rate_of_turn": "NUMERIC(5,2)",
            "true_heading": "INTEGER"
        },
        "ship_static_data_temp": {
            "id": "SERIAL PRIMARY KEY",
            "imo_number": "BIGINT",
            "mmsi": "BIGINT",
            "name": "TEXT",
            "ship_type": "TEXT",
            "length": "INTEGER",
            "width": "INTEGER",
            "max_draught": "NUMERIC(5,2)",
            "destination": "TEXT",
            "timestamp_collected": "TIMESTAMP DEFAULT NOW()",
            "timestamp_ais": "TIMESTAMP",
            "latitude": "NUMERIC(10,6)",
            "longitude": "NUMERIC(10,6)"
        }
    }
    
    try:
        # Check if tables exist
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public';
        """)
        existing_tables = [row[0] for row in cursor.fetchall()]
        
        # Create missing tables
        for table_name in table_schemas:
            if table_name not in existing_tables:
                logging.info(f"Table {table_name} does not exist, creating it...")
                columns = []
                for col_name, col_type in table_schemas[table_name].items():
                    columns.append(f"{col_name} {col_type}")
                
                create_query = f"""
                    CREATE TABLE {table_name} (
                        {', '.join(columns)}
                    );
                """
                cursor.execute(create_query)
                logging.info(f"Created table {table_name}")
                
                # Create indexes for the new table if necessary
                if table_name == "ship_static_data_temp":
                    # Create index on imo_number for faster lookup
                    cursor.execute("""
                        CREATE INDEX idx_ship_static_data_temp_imo_number
                        ON ship_static_data_temp (imo_number);
                    """)
                    
                    # Create index on timestamp_ais for faster temporal queries
                    cursor.execute("""
                        CREATE INDEX idx_ship_static_data_temp_timestamp_ais
                        ON ship_static_data_temp (timestamp_ais DESC);
                    """)
                    
                    # Create compound index on imo_number and timestamp_ais for optimized lookups
                    cursor.execute("""
                        CREATE INDEX idx_ship_static_data_temp_imo_ts
                        ON ship_static_data_temp (imo_number, timestamp_ais DESC);
                    """)
                    
                    logging.info("Created indexes on ship_static_data_temp table")
        
        # For each existing table, check and add missing columns
        for table_name in table_schemas:
            if table_name in existing_tables:
                # Get existing columns
                cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}';
                """)
                existing_columns = [row[0] for row in cursor.fetchall()]
                
                # Add missing columns
                for col_name, col_type in table_schemas[table_name].items():
                    if col_name not in existing_columns:
                        # Remove any default values or constraints for the ALTER TABLE command
                        clean_type = col_type.split("DEFAULT")[0].strip()
                        if "PRIMARY KEY" in clean_type:
                            clean_type = clean_type.replace("PRIMARY KEY", "").strip()
                        if "REFERENCES" in clean_type:
                            clean_type = clean_type.split("REFERENCES")[0].strip()
                        if "SERIAL" in clean_type:
                            # For serial types, use integer for alter table
                            clean_type = "INTEGER"
                            
                        logging.info(f"Adding missing column {col_name} to table {table_name}")
                        alter_query = f"""
                            ALTER TABLE {table_name}
                            ADD COLUMN {col_name} {clean_type};
                        """
                        cursor.execute(alter_query)
                        logging.info(f"Added column {col_name} to {table_name}")
                
                # Check if we need to add indexes to the ship_static_data_temp table
                if table_name == "ship_static_data_temp":
                    # Check if indexes exist
                    cursor.execute("""
                        SELECT indexname
                        FROM pg_indexes
                        WHERE tablename = 'ship_static_data_temp';
                    """)
                    existing_indexes = [row[0] for row in cursor.fetchall()]
                    
                    # Create indexes if they don't exist
                    if "idx_ship_static_data_temp_imo_number" not in existing_indexes:
                        cursor.execute("""
                            CREATE INDEX idx_ship_static_data_temp_imo_number
                            ON ship_static_data_temp (imo_number);
                        """)
                        logging.info("Created index on imo_number for ship_static_data_temp")
                    
                    if "idx_ship_static_data_temp_timestamp_ais" not in existing_indexes:
                        cursor.execute("""
                            CREATE INDEX idx_ship_static_data_temp_timestamp_ais
                            ON ship_static_data_temp (timestamp_ais DESC);
                        """)
                        logging.info("Created index on timestamp_ais for ship_static_data_temp")
                    
                    if "idx_ship_static_data_temp_imo_ts" not in existing_indexes:
                        cursor.execute("""
                            CREATE INDEX idx_ship_static_data_temp_imo_ts
                            ON ship_static_data_temp (imo_number, timestamp_ais DESC);
                        """)
                        logging.info("Created compound index on imo_number and timestamp_ais for ship_static_data_temp")
        
        conn.commit()
        logging.info("Database migration completed successfully")
    except Exception as e:
        conn.rollback()
        logging.error(f"Database migration failed: {e}")
    finally:
        cursor.close()
        conn.close()

def parse_timestamp(timestamp_str):
    """
    A custom parser that can handle strings in the form:
    'YYYY-MM-DD HH:MM:SS.fffffffff +0000 UTC'
    
    1. Remove ' UTC'.
    2. Truncate fractional part to 6 digits.
    3. Preserve timezone info '+0000'.
    4. Parse with Python's strptime.
    """
    ts = timestamp_str.replace(" UTC", "")

    pattern = r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\.(\d+)\s(\+\d{4})$"
    match = re.match(pattern, ts)
    if not match:
        raise ValueError(f"Timestamp doesn't match expected pattern: {timestamp_str}")

    date_time_part = match.group(1)
    fraction_part = match.group(2)[:6]  # truncate microseconds to 6 digits
    tz_part = match.group(3)

    new_timestamp_str = f"{date_time_part}.{fraction_part}{tz_part}"
    parsed_dt = datetime.datetime.strptime(new_timestamp_str, "%Y-%m-%d %H:%M:%S.%f%z")

    return parsed_dt

def load_mmsi_to_imo_mapping(conn):
    """
    Load existing MMSI to IMO mappings from the database.
    Returns a dictionary mapping MMSI to IMO.
    """
    cursor = conn.cursor()
    mmsi_to_imo = {}
    
    try:
        # Query all ships with both MMSI and IMO
        cursor.execute("SELECT mmsi, imo_number FROM ships WHERE mmsi IS NOT NULL")
        for mmsi, imo in cursor.fetchall():
            if mmsi and imo:
                mmsi_to_imo[mmsi] = imo
        
        logging.info(f"Loaded {len(mmsi_to_imo)} MMSI-to-IMO mappings from database")
    except Exception as e:
        logging.error(f"Error loading MMSI-to-IMO mappings: {e}")
    finally:
        cursor.close()
        
    return mmsi_to_imo

def find_imo_by_mmsi(conn, mmsi):
    """
    Query the database to find IMO number by MMSI.
    Returns IMO if found, None otherwise.
    """
    if not mmsi:
        return None
        
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT imo_number FROM ships WHERE mmsi = %s LIMIT 1", (mmsi,))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        logging.error(f"Error querying IMO by MMSI: {e}")
        return None
    finally:
        cursor.close()

def get_recent_destination(conn, imo_number):
    """
    Query the most recent destination for a ship from the static data table
    within the last 5 hours. Returns None if no recent destination is found.
    
    Args:
        conn: Database connection
        imo_number: IMO number of the ship
        
    Returns:
        destination: The most recent destination or None
    """
    if not imo_number:
        return None
        
    cursor = conn.cursor()
    try:
        # Query the most recent destination within the last 5 hours
        cursor.execute("""
            SELECT destination 
            FROM ship_static_data_temp 
            WHERE imo_number = %s 
            AND timestamp_ais > NOW() - INTERVAL '5 hours'
            ORDER BY timestamp_ais DESC 
            LIMIT 1
        """, (imo_number,))
        
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        logging.error(f"Error querying recent destination: {e}")
        return None
    finally:
        cursor.close()

async def connect_ais_stream():
    """
    Connect to AIS WebSocket API and store ship data in Cloud SQL (PostgreSQL).
    - Filter ships based on both bounding box (API level) and North Sea shapefile (code level)
    - Store ShipStaticData messages in the ship_static_data_temp table
    - Store PositionReport messages in the ship_data table, with destination looked up from recent static data
    - If IMO is None or 0, store in unknown_ships table
    - Implements exponential backoff for connection retries
    - Loads MMSI-to-IMO mappings from database for efficient lookups
    """
    # Create initial database connection
    conn = None
    for attempt in range(MAX_DB_RETRIES):
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
            )
            conn.autocommit = False  # We'll handle transactions explicitly
            break
        except psycopg2.Error as e:
            logging.error(f"Database connection attempt {attempt+1} failed: {e}")
            if attempt == MAX_DB_RETRIES - 1:
                logging.error("Maximum database connection attempts reached. Exiting.")
                return
            await asyncio.sleep(2 ** attempt)  # Exponential backoff

    cursor = conn.cursor()

    # Load the North Sea shapefile for secondary filtering
    north_sea_shape = load_north_sea_shapefile()
    
    # Load existing MMSI to IMO mappings from database
    mmsi_to_imo = load_mmsi_to_imo_mapping(conn)
    
    # Data collection statistics
    collected_count = 0
    filtered_count = 0
    last_minute_count = 0
    last_minute_filtered = 0
    static_data_count = 0
    position_data_count = 0
    no_imo_filtered_count = 0  # Count of no-IMO vessels filtered due to flag setting
    last_minute_no_imo_filtered = 0  # Count of no-IMO vessels filtered in the last minute
    start_time = time.time()
    
    # Track unique vessels by MMSI to calculate real coverage
    unique_vessels = set()  # Set of MMSI values seen
    unique_vessels_with_imo = set()  # Set of MMSI values with known IMO
    
    # Batch processing variables
    ships_batch = []
    static_data_batch = []
    position_data_batch = []
    unknown_data_batch = []
    last_commit_time = time.time()

    # Connection retry settings
    min_retry_delay = 1  # Initial delay in seconds
    max_retry_delay = 60  # Maximum delay in seconds
    retry_attempts = 0
    connected = False

    while True:
        try:
            # Calculate retry delay with exponential backoff and jitter
            if retry_attempts > 0:
                retry_delay = min(max_retry_delay, min_retry_delay * (2 ** (retry_attempts - 1)))
                # Add jitter (±20%)
                jitter = random.uniform(0.8, 1.2)
                retry_delay *= jitter
                logging.info(f"Waiting {retry_delay:.2f} seconds before reconnecting (attempt {retry_attempts})...")
                await asyncio.sleep(retry_delay)
            
            retry_attempts += 1
            logging.info(f"Connecting to AIS WebSocket (attempt {retry_attempts})...")
            
            async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
                logging.info("WebSocket connection established successfully")
                await websocket.send(json.dumps(SUBSCRIBE_MESSAGE))
                logging.info("Subscription message sent to AIS stream")
                
                # Reset retry counter on successful connection
                connected = True
                retry_attempts = 0
                
                async for message_json in websocket:
                    try:
                        # Check database connection and reconnect if needed
                        if conn.closed:
                            logging.warning("Database connection lost. Reconnecting...")
                            conn = psycopg2.connect(
                                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
                            )
                            cursor = conn.cursor()
                            
                        message = json.loads(message_json)

                        if "MessageType" not in message:
                            logging.warning(f"Received message without 'MessageType': {message}")
                            continue

                        metadata = message["MetaData"]
                        mmsi = metadata.get("MMSI")
                        latitude = metadata.get("latitude")
                        longitude = metadata.get("longitude")
                        
                        # Skip if we can't determine the position
                        if latitude is None or longitude is None:
                            continue
                            
                        # Secondary filtering - check if the point is within the North Sea shapefile
                        if not is_point_in_north_sea(latitude, longitude, north_sea_shape):
                            filtered_count += 1
                            last_minute_filtered += 1
                            continue

                        timestamp_ais_raw = metadata.get("time_utc", "")
                        try:
                            timestamp_ais_dt = parse_timestamp(timestamp_ais_raw)
                            timestamp_ais = timestamp_ais_dt.strftime("%Y-%m-%d %H:%M:%S.%f")
                        except ValueError:
                            logging.error(f"Invalid timestamp format: {timestamp_ais_raw}. Skipping entry.")
                            continue

                        if message["MessageType"] == "ShipStaticData":
                            ship_data = message["Message"]["ShipStaticData"]
                            
                            raw_imo_number = ship_data.get("ImoNumber")  # Could be None or an integer
                            name = ship_data.get("Name", "Unknown")
                            ship_type = ship_data.get("Type")
                            
                            # Calculate length and width according to AIS standard
                            dimension = ship_data.get("Dimension", {})
                            length = None
                            width = None
                            if dimension:
                                a = dimension.get("A")  # Distance from bow to GPS antenna
                                b = dimension.get("B")  # Distance from GPS antenna to stern
                                c = dimension.get("C")  # Distance from port side to GPS antenna
                                d = dimension.get("D")  # Distance from GPS antenna to starboard side
                                
                                # Calculate total length: A + B
                                if a is not None and b is not None:
                                    length = a + b
                                # Calculate total width/beam: C + D
                                if c is not None and d is not None:
                                    width = c + d
                            
                            max_draught = ship_data.get("MaximumStaticDraught")
                            destination = ship_data.get("Destination", "Unknown")

                            # Track unique vessels
                            if mmsi:
                                unique_vessels.add(mmsi)
                                if raw_imo_number is not None and raw_imo_number != 0:
                                    unique_vessels_with_imo.add(mmsi)
                            
                            # Store MMSI to IMO mapping for position reports
                            if raw_imo_number is not None and raw_imo_number != 0 and mmsi is not None:
                                mmsi_to_imo[mmsi] = raw_imo_number

                            # Process ShipStaticData differently now
                            if raw_imo_number is not None and raw_imo_number != 0:
                                # Valid IMO > 0
                                imo_number = raw_imo_number

                                # Add/update ships table entry
                                ships_batch.append((imo_number, mmsi, name, ship_type, length, width, max_draught))
                                
                                # Store static data in the new ship_static_data_temp table
                                static_data_batch.append((imo_number, mmsi, name, ship_type, length, width, 
                                                         max_draught, destination, timestamp_ais, latitude, longitude))
                                static_data_count += 1
                            else:
                                # IMO is None or 0 => store in unknown_ships
                                if raw_imo_number is None:
                                    imo_number = -1
                                else:
                                    # raw_imo_number == 0
                                    imo_number = 0

                                # For ships with unknown IMO, check if we should store them
                                if SAVE_NO_IMO_VESSELS:
                                    # Set dynamic fields to None
                                    sog = None
                                    cog = None
                                    navigational_status_code = None
                                    rate_of_turn = None
                                    true_heading = None
                                    
                                    unknown_data_batch.append((imo_number, mmsi, name, ship_type, length, width, 
                                                             max_draught, destination, timestamp_ais, latitude, 
                                                             longitude, sog, cog, navigational_status_code, rate_of_turn,
                                                             true_heading))
                                else:
                                    # Count filtered no-IMO vessels
                                    no_imo_filtered_count += 1
                                    last_minute_no_imo_filtered += 1

                            collected_count += 1
                            
                        elif message["MessageType"] == "PositionReport":
                            position_data = message["Message"]["PositionReport"]
                            
                            # Extract dynamic data from position report
                            sog = position_data.get("Sog")
                            cog = position_data.get("Cog")
                            navigational_status_code = position_data.get("NavigationalStatus")
                            rate_of_turn = position_data.get("RateOfTurn")
                            true_heading = position_data.get("TrueHeading")
                            
                            # Track unique vessels by MMSI
                            if mmsi:
                                unique_vessels.add(mmsi)
                            
                            # Try to find IMO using MMSI from memory first
                            imo_number = mmsi_to_imo.get(mmsi)
                            
                            # If not found in memory, try to query from database
                            if imo_number is None and mmsi is not None:
                                imo_number = find_imo_by_mmsi(conn, mmsi)
                                # Update our in-memory mapping if found
                                if imo_number is not None:
                                    mmsi_to_imo[mmsi] = imo_number
                                    unique_vessels_with_imo.add(mmsi)
                            elif imo_number is not None and mmsi is not None:
                                # We found IMO in memory, update tracking
                                unique_vessels_with_imo.add(mmsi)
                            
                            if imo_number is not None:
                                # Get the recent destination for this ship from static data
                                destination = get_recent_destination(conn, imo_number)
                                
                                # Add to ship_data batch with the destination (could be None)
                                position_data_batch.append((imo_number, timestamp_ais, latitude, longitude, destination, 
                                                         sog, cog, navigational_status_code, rate_of_turn, true_heading))
                                position_data_count += 1
                            else:
                                # Unknown IMO, check if we should store it
                                if SAVE_NO_IMO_VESSELS:
                                    # Add to unknown_ships batch
                                    unknown_data_batch.append((-1, mmsi, None, None, None, None, None, None,
                                                              timestamp_ais, latitude, longitude, sog, cog, 
                                                              navigational_status_code, rate_of_turn, true_heading))
                                else:
                                    # Count filtered no-IMO vessels
                                    no_imo_filtered_count += 1
                                    last_minute_no_imo_filtered += 1
                            
                            collected_count += 1
                        
                        # Process batches if they're full or it's been a while since the last commit
                        current_time = time.time()
                        if (len(ships_batch) + len(static_data_batch) + len(position_data_batch) + len(unknown_data_batch) >= BATCH_SIZE or
                            current_time - last_commit_time >= 10):  # Commit at least every 10 seconds
                            
                            # Begin transaction
                            try:
                                # Process ships table updates
                                if ships_batch:
                                    cursor.executemany("""
                                        INSERT INTO ships (
                                            imo_number, mmsi, name, ship_type, length, width, max_draught
                                        )
                                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                                        ON CONFLICT (imo_number) DO UPDATE
                                        SET 
                                            name = EXCLUDED.name,
                                            ship_type = EXCLUDED.ship_type,
                                            length = EXCLUDED.length,
                                            width = EXCLUDED.width,
                                            max_draught = EXCLUDED.max_draught;
                                    """, ships_batch)
                                    logging.debug(f"Inserted/updated {len(ships_batch)} ship records")
                                    ships_batch = []
                                
                                # Process static data batches
                                if static_data_batch:
                                    cursor.executemany("""
                                        INSERT INTO ship_static_data_temp (
                                            imo_number, mmsi, name, ship_type, length, width, 
                                            max_draught, destination, timestamp_ais, latitude, longitude
                                        )
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                    """, static_data_batch)
                                    logging.debug(f"Inserted {len(static_data_batch)} ship static data records")
                                    static_data_batch = []
                                
                                # Process position data batches
                                if position_data_batch:
                                    cursor.executemany("""
                                        INSERT INTO ship_data (
                                            imo_number, timestamp_ais, latitude, longitude, destination, sog, cog, 
                                            navigational_status_code, rate_of_turn, true_heading
                                        )
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                    """, position_data_batch)
                                    logging.debug(f"Inserted {len(position_data_batch)} ship position records")
                                    position_data_batch = []
                                
                                # Process unknown ship batches
                                if unknown_data_batch:
                                    cursor.executemany("""
                                        INSERT INTO unknown_ships (
                                            imo_number, mmsi, name, ship_type, length, width, max_draught, destination,
                                            timestamp_ais, latitude, longitude, sog, cog, navigational_status_code, rate_of_turn,
                                            true_heading
                                        )
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                                                %s, %s, %s, %s, %s, %s, %s, %s);
                                    """, unknown_data_batch)
                                    logging.debug(f"Inserted {len(unknown_data_batch)} unknown ship records")
                                    unknown_data_batch = []
                                
                                conn.commit()
                                last_commit_time = current_time
                                
                            except psycopg2.Error as e:
                                logging.error(f"Database error during batch processing: {e}")
                                conn.rollback()
                                
                                # If it's a connection error, reconnect
                                if isinstance(e, psycopg2.OperationalError):
                                    conn = psycopg2.connect(
                                        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
                                    )
                                    cursor = conn.cursor()
                                
                        last_minute_count += 1

                        # Log statistics every minute
                        if current_time - start_time >= 60:
                            # Calculate coverage based on unique vessels seen
                            # Coverage = percentage of unique vessels for which we have IMO numbers
                            unique_vessel_count = len(unique_vessels)
                            unique_vessel_with_imo_count = len(unique_vessels_with_imo)
                            
                            vessel_coverage = (unique_vessel_with_imo_count / unique_vessel_count 
                                              if unique_vessel_count > 0 else 0)
                            
                            # Add no-IMO vessel filtering info to the log
                            no_imo_info = ""
                            if SAVE_NO_IMO_VESSELS:
                                no_imo_info = "No-IMO vessels: all saved to unknown_ships table."
                            else:
                                no_imo_info = f"No-IMO vessels: filtered out {no_imo_filtered_count} in total, {last_minute_no_imo_filtered} in past minute."
                            
                            logging.info(
                                f"Total vessels collected: {collected_count}, filtered out: {filtered_count}. "
                                f"Past minute collected: {last_minute_count}, filtered out {last_minute_filtered}. "
                                f"Static data: {static_data_count}, Position data: {position_data_count}, "
                                f"Unique vessels: {unique_vessel_count}, with IMO: {unique_vessel_with_imo_count} "
                                f"(vessel coverage: {vessel_coverage:.1%}). {no_imo_info}"
                            )
                            last_minute_count = 0
                            last_minute_filtered = 0
                            last_minute_no_imo_filtered = 0
                            start_time = current_time

                    except json.JSONDecodeError:
                        logging.error("Failed to decode JSON. Skipping message.")
                    except psycopg2.Error as e:
                        logging.error(f"Database error: {e}")
                        conn.rollback()
                        
                        # If it's a connection error, reconnect
                        if isinstance(e, psycopg2.OperationalError):
                            try:
                                conn = psycopg2.connect(
                                    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
                                )
                                cursor = conn.cursor()
                            except psycopg2.Error as reconnect_err:
                                logging.error(f"Failed to reconnect to database: {reconnect_err}")
                                await asyncio.sleep(5)  # Wait before retry

        except websockets.exceptions.ConnectionClosedError as e:
            logging.error(f"WebSocket connection closed unexpectedly: {e}")
            # Only reset the connected flag if we were connected before
            if connected:
                connected = False
                # Reset batches to avoid data loss
                if ships_batch or static_data_batch or position_data_batch or unknown_data_batch:
                    logging.warning(f"Connection lost with uncommitted data: {len(ships_batch)} ship records, "
                                   f"{len(static_data_batch)} static records, "
                                   f"{len(position_data_batch)} position records, "
                                   f"{len(unknown_data_batch)} unknown records")
                    # Try to commit any pending data before reconnecting
                    try:
                        if not conn.closed:
                            process_pending_batches(conn, cursor, ships_batch, static_data_batch, 
                                                   position_data_batch, unknown_data_batch)
                            ships_batch = []
                            static_data_batch = []
                            position_data_batch = []
                            unknown_data_batch = []
                    except Exception as commit_err:
                        logging.error(f"Failed to commit pending data: {commit_err}")
        except websockets.exceptions.WebSocketException as e:
            logging.error(f"WebSocket error: {e}")
            connected = False
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            connected = False

def process_pending_batches(conn, cursor, ships_batch, static_data_batch, position_data_batch, unknown_data_batch):
    """Helper function to process any pending batches before reconnection"""
    try:
        # Process ships table updates
        if ships_batch:
            cursor.executemany("""
                INSERT INTO ships (
                    imo_number, mmsi, name, ship_type, length, width, max_draught
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (imo_number) DO UPDATE
                SET 
                    name = EXCLUDED.name,
                    ship_type = EXCLUDED.ship_type,
                    length = EXCLUDED.length,
                    width = EXCLUDED.width,
                    max_draught = EXCLUDED.max_draught;
            """, ships_batch)
        
        # Process static data batches
        if static_data_batch:
            cursor.executemany("""
                INSERT INTO ship_static_data_temp (
                    imo_number, mmsi, name, ship_type, length, width, 
                    max_draught, destination, timestamp_ais, latitude, longitude
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, static_data_batch)
        
        # Process position data batches
        if position_data_batch:
            cursor.executemany("""
                INSERT INTO ship_data (
                    imo_number, timestamp_ais, latitude, longitude, destination, sog, cog, 
                    navigational_status_code, rate_of_turn, true_heading
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, position_data_batch)
        
        # Process unknown ship batches
        if unknown_data_batch:
            cursor.executemany("""
                INSERT INTO unknown_ships (
                    imo_number, mmsi, name, ship_type, length, width, max_draught, destination,
                    timestamp_ais, latitude, longitude, sog, cog, navigational_status_code, rate_of_turn,
                    true_heading
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s);
            """, unknown_data_batch)
        
        conn.commit()
        logging.info("Successfully committed pending data before reconnection")
    except Exception as e:
        logging.error(f"Error processing pending batches: {e}")
        conn.rollback()

if __name__ == "__main__":
    try:
        # Always run migration first to ensure database schema is up to date
        migrate_database()
        logging.info("Starting AIS Data Collector...")
        # Verify that the shapefile exists and can be loaded before starting
        if os.path.exists(NORTH_SEA_SHAPEFILE):
            logging.info(f"North Sea shapefile found at: {NORTH_SEA_SHAPEFILE}")
            # Try loading the shapefile once to catch any issues early
            test_load = load_north_sea_shapefile()
            logging.info(f"North Sea shapefile loaded successfully with {len(test_load)} feature")
            asyncio.run(connect_ais_stream())
        else:
            logging.error(f"North Sea shapefile not found at: {NORTH_SEA_SHAPEFILE}")
            logging.error("Cannot start AIS Data Collector without the North Sea shapefile")
    except Exception as e:
        logging.error(f"Failed to start AIS Data Collector: {e}")
