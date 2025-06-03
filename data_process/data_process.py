import os
import time
import pandas as pd
import psycopg2
import logging
from psycopg2 import pool
from psycopg2 import sql
from psycopg2.extras import execute_batch
from datetime import datetime
from contextlib import contextmanager

# Setup logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "ais_data_collection"),
    "user": os.getenv("DB_USER", "aoyamaxx"),
    "password": os.getenv("DB_PASSWORD", "aoyamaxx"),
    "host": os.getenv("DB_HOST", "cloud_sql_proxy"),
    "port": os.getenv("DB_PORT", "5432"),
}

# Connection pool configuration
MIN_CONNECTIONS = 1
MAX_CONNECTIONS = 10
connection_pool = None

def init_connection_pool():
    """Initialize the database connection pool"""
    global connection_pool
    try:
        connection_pool = pool.SimpleConnectionPool(
            MIN_CONNECTIONS,
            MAX_CONNECTIONS,
            **DB_CONFIG,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
        logger.info("Database connection pool initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize connection pool: {e}")
        raise

@contextmanager
def get_db_connection():
    """Context manager for database connections from the pool"""
    conn = None
    try:
        conn = connection_pool.getconn()
        yield conn
    except Exception as e:
        logger.error(f"Error getting connection from pool: {e}")
        raise
    finally:
        if conn:
            connection_pool.putconn(conn)

@contextmanager
def get_db_cursor(commit=True):
    """Context manager for database cursors"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            yield cursor
            if commit:
                conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database operation failed: {e}")
            raise
        finally:
            cursor.close()

# Configuration switch for recreating tables
RECREATE_TABLES = os.getenv("RECREATE_TABLES", "false").lower() == "true"

def setup_database():
    """Setup database tables and load reference data"""
    try:
        with get_db_cursor() as cursor:
            # Check if tables exist
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('ship_type_codes', 'ports', 'navigational_status')
            """)
            existing_tables = [row[0] for row in cursor.fetchall()]
            ship_type_exists = 'ship_type_codes' in existing_tables
            ports_exists = 'ports' in existing_tables
            nav_status_exists = 'navigational_status' in existing_tables
            
            if RECREATE_TABLES:
                logger.info("RECREATE mode: Starting full database reset...")
                
                # Drop and recreate reference tables
                logger.info("RECREATE mode: Dropping and recreating reference tables...")
                cursor.execute("""
                    DROP TABLE IF EXISTS ship_type_codes CASCADE;
                    DROP TABLE IF EXISTS ports CASCADE;
                    DROP TABLE IF EXISTS navigational_status CASCADE;
                    
                    CREATE TABLE ship_type_codes (
                        type_code INTEGER PRIMARY KEY,
                        type TEXT,
                        remark TEXT
                    );
                    
                    CREATE TABLE ports (
                        port_id SERIAL PRIMARY KEY,
                        port_name TEXT,
                        country TEXT,
                        latitude NUMERIC(10,6),
                        longitude NUMERIC(10,6),
                        scrubber_status INTEGER DEFAULT 0
                    );
                    
                    CREATE TABLE navigational_status (
                        navigational_status_code INTEGER PRIMARY KEY,
                        navigational_status TEXT
                    );
                """)
                
                # Reset processing columns in ships table
                logger.info("RECREATE mode: Resetting all processing data in ships table...")
                cursor.execute("""
                    ALTER TABLE ships DROP COLUMN IF EXISTS type_name;
                    ALTER TABLE ships DROP COLUMN IF EXISTS type_remark;
                    ALTER TABLE ships ADD COLUMN type_name TEXT;
                    ALTER TABLE ships ADD COLUMN type_remark TEXT;
                """)
                logger.info("RECREATE mode: All processing data in ships table has been reset")
                
                # Reset navigational_status in ship_data table if it exists
                cursor.execute("""
                    SELECT to_regclass('public.ship_data') IS NOT NULL;
                """)
                ship_data_exists = cursor.fetchone()[0]
                
                if ship_data_exists:
                    logger.info("RECREATE mode: Resetting navigational_status in ship_data table...")
                    cursor.execute("""
                        ALTER TABLE ship_data DROP COLUMN IF EXISTS navigational_status;
                        ALTER TABLE ship_data ADD COLUMN navigational_status TEXT;
                    """)
                    logger.info("RECREATE mode: navigational_status column in ship_data has been reset")
                
                # Set flags to load data
                ship_type_exists = False
                ports_exists = False
                nav_status_exists = False
            else:
                logger.info("UPDATE mode: Checking and creating tables if needed...")
                # Create tables if they don't exist
                if not ship_type_exists:
                    logger.info("Creating ship_type_codes table as it doesn't exist")
                    cursor.execute("""
                        CREATE TABLE ship_type_codes (
                            type_code INTEGER PRIMARY KEY,
                            type TEXT,
                            remark TEXT
                        );
                    """)
                
                if not ports_exists:
                    logger.info("Creating ports table as it doesn't exist")
                    cursor.execute("""
                        CREATE TABLE ports (
                            port_id SERIAL PRIMARY KEY,
                            port_name TEXT,
                            country TEXT,
                            latitude NUMERIC(10,6),
                            longitude NUMERIC(10,6),
                            scrubber_status INTEGER DEFAULT 0
                        );
                    """)
                    
                if not nav_status_exists:
                    logger.info("Creating navigational_status table as it doesn't exist")
                    cursor.execute("""
                        CREATE TABLE navigational_status (
                            navigational_status_code INTEGER PRIMARY KEY,
                            navigational_status TEXT
                        );
                    """)
                
                # Add columns if they don't exist
                cursor.execute("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                     WHERE table_name='ships' AND column_name='type_name') THEN
                            ALTER TABLE ships ADD COLUMN type_name TEXT;
                        END IF;
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                     WHERE table_name='ships' AND column_name='type_remark') THEN
                            ALTER TABLE ships ADD COLUMN type_remark TEXT;
                        END IF;
                    END $$;
                """)
                
                # Check if ship_data table exists and add navigational_status column if needed
                cursor.execute("""
                    SELECT to_regclass('public.ship_data') IS NOT NULL;
                """)
                ship_data_exists = cursor.fetchone()[0]
                
                if ship_data_exists:
                    cursor.execute("""
                        DO $$
                        BEGIN
                            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                         WHERE table_name='ship_data' AND column_name='navigational_status') THEN
                                ALTER TABLE ship_data ADD COLUMN navigational_status TEXT;
                            END IF;
                        END $$;
                    """)
                    logger.info("ship_data table exists, ensured navigational_status column exists")
                else:
                    logger.info("ship_data table doesn't exist yet, will be checked on next run")
                
                logger.info("UPDATE mode: Database structure check completed")
            
            # Load ship_type_codes data if needed
            if not ship_type_exists or RECREATE_TABLES:
                logger.info("Loading ship type codes...")
                ship_types_df = pd.read_csv('ship_type_codes_normalized.csv')
                ship_types_df['type_code'] = pd.to_numeric(ship_types_df['type_code'], errors='coerce')
                ship_types_df = ship_types_df.dropna(subset=['type_code'])
                ship_types_df['type_code'] = ship_types_df['type_code'].astype(int)

                # If table exists but we need to reload, truncate it first
                if ship_type_exists:
                    cursor.execute("TRUNCATE TABLE ship_type_codes;")
                
                # Insert data using parameterized queries to properly handle NULL values
                for _, row in ship_types_df.iterrows():
                    # Convert pandas NA/NaN to Python None for database NULL
                    type_code = None if pd.isna(row['type_code']) else int(row['type_code'])
                    type_name = None if pd.isna(row['type']) else row['type']
                    remark = None if pd.isna(row['remark']) else row['remark']
                    
                    cursor.execute("""
                        INSERT INTO ship_type_codes (type_code, type, remark)
                        VALUES (%s, %s, %s);
                    """, (type_code, type_name, remark))
                logger.info(f"Loaded {len(ship_types_df)} ship type codes")
            else:
                logger.info("Ship type codes table exists, skipping data import")

            # Load ports data if needed
            if not ports_exists or RECREATE_TABLES:
                logger.info("Loading ports data...")
                ports_df = pd.read_csv('filtered_port.csv')
                
                # Check for NA values and log their presence
                na_counts = ports_df.isna().sum()
                columns_with_na = na_counts[na_counts > 0].index.tolist()
                if columns_with_na:
                    logger.info(f"Found NA values in port data columns: {', '.join(columns_with_na)}")
                    logger.info("NA values will be imported as NULL values in the database")
                
                # Load scrubber status from port_bans.csv if it exists
                port_bans_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'port_bans.csv')
                if os.path.exists(port_bans_path):
                    logger.info(f"Loading scrubber status from {port_bans_path}")
                    port_bans_df = pd.read_csv(port_bans_path)
                    
                    # Create a dictionary mapping port names to their scrubber status
                    port_status_dict = dict(zip(port_bans_df['port_name'], port_bans_df['scrubber_status']))
                    
                    # Add SCRUBBER_STATUS column to ports dataframe
                    ports_df['SCRUBBER_STATUS'] = ports_df['PORT_NAME'].map(port_status_dict).fillna(0).astype(int)
                    logger.info("Applied scrubber status from port_bans.csv")
                    
                    # Log the number of ports with different scrubber statuses
                    status_counts = ports_df['SCRUBBER_STATUS'].value_counts()
                    logger.info(f"Scrubber status distribution: {status_counts.to_dict()}")
                else:
                    logger.info("port_bans.csv not found, setting default scrubber status to 0")
                    # Add default SCRUBBER_STATUS column
                    ports_df['SCRUBBER_STATUS'] = 0
                
                # If table exists but we need to reload, truncate it first
                if ports_exists:
                    cursor.execute("TRUNCATE TABLE ports;")
                
                # Insert data using parameterized queries to properly handle NULL values
                for _, row in ports_df.iterrows():
                    # Convert pandas NA/NaN to Python None for database NULL
                    port_name = None if pd.isna(row['PORT_NAME']) else row['PORT_NAME']
                    country = None if pd.isna(row['COUNTRY']) else row['COUNTRY']
                    latitude = None if pd.isna(row['LATITUDE']) else row['LATITUDE']
                    longitude = None if pd.isna(row['LONGITUDE']) else row['LONGITUDE']
                    scrubber_status = None if pd.isna(row.get('SCRUBBER_STATUS', 0)) else row.get('SCRUBBER_STATUS', 0)
                    
                    cursor.execute("""
                        INSERT INTO ports (port_name, country, latitude, longitude, scrubber_status)
                        VALUES (%s, %s, %s, %s, %s);
                    """, (port_name, country, latitude, longitude, scrubber_status))
                
                logger.info(f"Loaded {len(ports_df)} ports")
            else:
                logger.info("Ports table exists, skipping data import")
                
            # Load navigational_status data if needed
            if not nav_status_exists or RECREATE_TABLES:
                logger.info("Loading navigational status data...")
                # The file is in a different directory (navigational_status folder)
                nav_status_path = os.path.join('..', 'navigational_status', 'navigational_status_code.csv')
                
                if os.path.exists(nav_status_path):
                    nav_status_df = pd.read_csv(nav_status_path)
                    
                    # Check for NA values and log their presence
                    na_counts = nav_status_df.isna().sum()
                    columns_with_na = na_counts[na_counts > 0].index.tolist()
                    if columns_with_na:
                        logger.info(f"Found NA values in navigational status columns: {', '.join(columns_with_na)}")
                        logger.info("NA values will be imported as NULL values in the database")
                    
                    # If table exists but we need to reload, truncate it first
                    if nav_status_exists:
                        cursor.execute("TRUNCATE TABLE navigational_status;")
                    
                    # Insert data using parameterized queries to properly handle NULL values
                    for _, row in nav_status_df.iterrows():
                        # Convert pandas NA/NaN to Python None for database NULL
                        status_code = None if pd.isna(row['navigational_status_code']) else int(row['navigational_status_code'])
                        status = None if pd.isna(row['navigational_status']) else row['navigational_status']
                        
                        cursor.execute("""
                            INSERT INTO navigational_status (navigational_status_code, navigational_status)
                            VALUES (%s, %s);
                        """, (status_code, status))
                    
                    logger.info(f"Loaded {len(nav_status_df)} navigational status codes")
                else:
                    logger.error(f"Navigational status CSV file not found at: {nav_status_path}")
            else:
                logger.info("Navigational status table exists, skipping data import")

            if RECREATE_TABLES:
                logger.info("RECREATE mode: Database reset and reload completed")
            else:
                logger.info("UPDATE mode: Database update completed")

            return True
    except Exception as e:
        logger.error(f"Failed to setup database: {e}")
        return False

def process_ships(batch_size=10000):
    """Process a batch of ships and return the number of processed records"""
    try:
        with get_db_cursor() as cursor:
            # Get unprocessed ships with a better query - ordered by imo_number to ensure we process from old to new
            cursor.execute("""
                WITH unprocessed_ships AS (
                    SELECT DISTINCT ON (imo_number) imo_number, ship_type 
                    FROM ships 
                    WHERE type_name IS NULL 
                    OR type_remark IS NULL
                    ORDER BY imo_number ASC  -- Ensure we process from old to new
                    LIMIT %s
                )
                SELECT s.imo_number, s.ship_type
                FROM unprocessed_ships s
                ORDER BY s.imo_number ASC;  -- Maintain the order
            """, (batch_size,))
            
            rows = cursor.fetchall()
            if not rows:
                return 0

            # Get ship type mappings
            cursor.execute("SELECT type_code, type, remark FROM ship_type_codes;")
            type_mappings = {str(row[0]): (row[1], row[2]) for row in cursor.fetchall()}

            # Prepare batch update data
            batch_data = []
            for imo_number, ship_type in rows:
                try:
                    if ship_type and str(ship_type).strip():
                        ship_type_parsed = int(float(str(ship_type).strip()))
                        type_info = type_mappings.get(str(ship_type_parsed), ('Unknown', 'Unknown'))
                        batch_data.append((str(ship_type_parsed), type_info[0], type_info[1], imo_number))
                    else:
                        batch_data.append((None, 'Unknown', 'No ship type provided', imo_number))
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid ship_type for imo_number {imo_number}: {ship_type}")
                    batch_data.append((None, 'Unknown', 'Invalid ship type', imo_number))
            
            # Perform batch update using execute_batch
            if batch_data:
                execute_batch(cursor, """
                    UPDATE ships 
                    SET 
                        ship_type = COALESCE(%s, ship_type),
                        type_name = %s, 
                        type_remark = %s
                    WHERE imo_number = %s
                    AND (type_name IS NULL OR type_remark IS NULL);
                """, batch_data, page_size=500)
                
                return len(batch_data)
            return 0

    except Exception as e:
        logger.error(f"Error processing ships: {e}")
        return 0

def process_ship_data(batch_size=10000):
    """Process a batch of ship_data and return the number of processed records"""
    try:
        with get_db_cursor() as cursor:
            # Check if ship_data table exists
            cursor.execute("""
                SELECT to_regclass('public.ship_data') IS NOT NULL;
            """)
            ship_data_exists = cursor.fetchone()[0]
            
            if not ship_data_exists:
                logger.info("ship_data table doesn't exist yet, skipping processing")
                return 0
            
            # Get unprocessed ship_data records - ordered by timestamp_ais to ensure we process from old to new
            cursor.execute("""
                SELECT id, navigational_status_code
                FROM ship_data
                WHERE navigational_status IS NULL
                ORDER BY timestamp_ais ASC
                LIMIT %s;
            """, (batch_size,))
            
            rows = cursor.fetchall()
            if not rows:
                return 0

            # Get navigational status mappings
            cursor.execute("SELECT navigational_status_code, navigational_status FROM navigational_status;")
            nav_status_mappings = {str(row[0]): row[1] for row in cursor.fetchall()}

            # Prepare batch update data
            batch_data = []
            for record_id, nav_status_code in rows:
                try:
                    if nav_status_code is not None and str(nav_status_code).strip():
                        nav_status_code_parsed = int(float(str(nav_status_code).strip()))
                        nav_status = nav_status_mappings.get(str(nav_status_code_parsed), 'Unknown')
                        batch_data.append((nav_status, record_id))
                    else:
                        batch_data.append(('Unknown', record_id))
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid navigational_status_code for record {record_id}: {nav_status_code}")
                    batch_data.append(('Unknown', record_id))
            
            # Perform batch update using execute_batch
            if batch_data:
                execute_batch(cursor, """
                    UPDATE ship_data 
                    SET navigational_status = %s
                    WHERE id = %s
                    AND navigational_status IS NULL;
                """, batch_data, page_size=500)
                
                return len(batch_data)
            return 0

    except Exception as e:
        logger.error(f"Error processing ship_data: {e}")
        return 0

def get_database_stats():
    """Get current database statistics"""
    with get_db_cursor(commit=False) as cursor:
        # First check if ship_data table exists
        cursor.execute("""
            SELECT to_regclass('public.ship_data') IS NOT NULL;
        """)
        ship_data_exists = cursor.fetchone()[0]
        
        # Get ships stats
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN type_name IS NULL OR type_remark IS NULL THEN 1 END) as unprocessed_records
            FROM ships;
        """)
        ships_stats = cursor.fetchone()
        
        # Get ship_data stats if the table exists
        ship_data_stats = (0, 0)  # Default if table doesn't exist
        if ship_data_exists:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN navigational_status IS NULL THEN 1 END) as unprocessed_records
                FROM ship_data;
            """)
            ship_data_stats = cursor.fetchone()
        
        return ships_stats, ship_data_stats

def main():
    """Main function to run the data processing pipeline"""
    logger.info(f"Starting data processing service in {'RECREATE' if RECREATE_TABLES else 'UPDATE'} mode")
    
    try:
        # Import threading module
        import threading
        from concurrent.futures import ThreadPoolExecutor
        
        # Initialize connection pool with increased max connections
        global MAX_CONNECTIONS
        MAX_CONNECTIONS = 20
        init_connection_pool()
        
        # Setup database
        if not setup_database():
            logger.error("Failed to setup database")
            return

        # Get initial database stats
        (ships_total, ships_unprocessed), (ship_data_total, ship_data_unprocessed) = get_database_stats()
        logger.info(
            f"Initial database state - "
            f"ships: Total records: {ships_total}, "
            f"{'All records marked for reprocessing' if RECREATE_TABLES else f'Unprocessed: {ships_unprocessed}'}"
        )
        if ship_data_total > 0:
            logger.info(
                f"ship_data: Total records: {ship_data_total}, "
                f"{'All records marked for reprocessing' if RECREATE_TABLES else f'Unprocessed: {ship_data_unprocessed}'}"
            )

        ships_total_processed = 0
        ships_interval_processed = 0
        ship_data_total_processed = 0
        ship_data_interval_processed = 0
        last_status_time = time.time()
        check_interval = 60  # seconds (changed from 5 to 60)
        initial_processing = True

        # Define processing function for threads
        def process_ships_thread():
            nonlocal ships_total_processed, ships_interval_processed
            ships_processed_count = process_ships(batch_size=10000)
            if ships_processed_count > 0:
                ships_total_processed += ships_processed_count
                ships_interval_processed += ships_processed_count
            return ships_processed_count
            
        def process_ship_data_thread():
            nonlocal ship_data_total_processed, ship_data_interval_processed
            ship_data_processed_count = process_ship_data(batch_size=10000)
            if ship_data_processed_count > 0:
                ship_data_total_processed += ship_data_processed_count
                ship_data_interval_processed += ship_data_processed_count
            return ship_data_processed_count

        while True:
            try:
                # Use ThreadPoolExecutor to process ships and ship_data in parallel
                with ThreadPoolExecutor(max_workers=2) as executor:
                    # Submit both processing tasks
                    ships_future = executor.submit(process_ships_thread)
                    ship_data_future = executor.submit(process_ship_data_thread)
                    
                    # Get results from each thread
                    ships_processed_count = ships_future.result()
                    ship_data_processed_count = ship_data_future.result()

                current_time = time.time()
                if current_time - last_status_time >= check_interval:
                    # Get current database stats
                    (current_ships_total, current_ships_unprocessed), (current_ship_data_total, current_ship_data_unprocessed) = get_database_stats()
                    
                    # Log status
                    logger.info(
                        f"Status Report - "
                        f"ships: Database Total: {current_ships_total}, "
                        f"Past minute: {ships_interval_processed}, "
                        f"Total Processed: {ships_total_processed}, "
                        f"Remaining Unprocessed: {current_ships_unprocessed}"
                    )
                    
                    if current_ship_data_total > 0:
                        logger.info(
                            f"ship_data: Database Total: {current_ship_data_total}, "
                            f"Past minute: {ship_data_interval_processed}, "
                            f"Total Processed: {ship_data_total_processed}, "
                            f"Remaining Unprocessed: {current_ship_data_unprocessed}"
                        )
                    
                    ships_interval_processed = 0
                    ship_data_interval_processed = 0
                    last_status_time = current_time

                    # Check if initial processing is complete
                    if initial_processing and current_ships_unprocessed == 0 and current_ship_data_unprocessed == 0:
                        initial_processing = False
                        logger.info("Initial data processing complete. Monitoring for new data...")

                # If no records processed, wait before next check - reduced wait time
                if ships_processed_count == 0 and ship_data_processed_count == 0:
                    if not initial_processing:
                        time.sleep(2)  # Reduced from 5 seconds to 2 seconds
                    else:
                        time.sleep(0.5)  # Reduced from 1 second to 0.5 seconds

            except psycopg2.OperationalError as e:
                logger.error(f"Database connection error: {e}")
                # Try to reinitialize connection pool
                if connection_pool:
                    connection_pool.closeall()
                time.sleep(2)  # Reduced from 5 seconds to 2 seconds
                init_connection_pool()
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                time.sleep(2)  # Reduced from 5 seconds to 2 seconds

    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        if connection_pool:
            connection_pool.closeall()
            logger.info("Closed all database connections")

if __name__ == "__main__":
    main()