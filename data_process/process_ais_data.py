import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from google.cloud.sql.connector import Connector
import logging
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection setup
def get_db_connection():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "north-sea-watch-39a510f80808.json"
    
    DB_NAME = os.getenv("DB_NAME", "ais_data_collection")
    DB_USER = os.getenv("DB_USER", "aoyamaxx")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "aoyamaxx")
    INSTANCE_CONNECTION_NAME = "north-sea-watch:europe-west4:ais-database"
    
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

def process_data_in_chunks(start_date, end_date, chunk_size_days=7, sample_interval=1000):
    """
    Process data in chunks of specified days to avoid memory issues
    Args:
        start_date: Start date for data collection
        end_date: End date for data collection
        chunk_size_days: Number of days to process in each chunk
        sample_interval: Take every Nth record (e.g., 10 means take every 10th record)
    """
    engine, connector = get_db_connection()
    
    current_date = start_date
    all_chunks = []
    
    try:
        while current_date < end_date:
            chunk_end_date = min(current_date + timedelta(days=chunk_size_days), end_date)
            
            logging.info(f"Processing chunk from {current_date} to {chunk_end_date}")
            
            # Query for the current chunk with sampling
            query = f"""
            WITH ship_stats AS (
                SELECT 
                    s.imo_number,
                    s.name,
                    s.ship_type,
                    CASE 
                        WHEN c.imo_number IS NOT NULL THEN TRUE 
                        ELSE FALSE 
                    END as has_scrubber,
                    COUNT(*) as position_count,
                    AVG(sd.sog) as avg_speed,
                    MIN(sd.timestamp_collected) as first_seen,
                    MAX(sd.timestamp_collected) as last_seen,
                    COUNT(DISTINCT sd.destination) as unique_destinations
                FROM ships s
                LEFT JOIN icct_wfr_combined c ON s.imo_number::text = c.imo_number
                JOIN ship_data sd ON s.imo_number = sd.imo_number
                WHERE sd.timestamp_collected >= '{current_date}'
                AND sd.timestamp_collected < '{chunk_end_date}'
                GROUP BY s.imo_number, s.name, s.ship_type, c.imo_number
            ),
            numbered_positions AS (
                SELECT 
                    ss.*,
                    sd.latitude,
                    sd.longitude,
                    sd.sog,
                    sd.cog,
                    sd.navigational_status_code,
                    sd.timestamp_collected,
                    sd.destination,
                    ROW_NUMBER() OVER (
                        PARTITION BY ss.imo_number 
                        ORDER BY sd.timestamp_collected
                    ) as row_num
                FROM ship_stats ss
                JOIN ship_data sd ON ss.imo_number = sd.imo_number
                WHERE sd.timestamp_collected >= '{current_date}'
                AND sd.timestamp_collected < '{chunk_end_date}'
            )
            SELECT 
                imo_number,
                name,
                ship_type,
                has_scrubber,
                position_count,
                avg_speed,
                first_seen,
                last_seen,
                unique_destinations,
                latitude,
                longitude,
                sog,
                cog,
                navigational_status_code,
                timestamp_collected,
                destination
            FROM numbered_positions
            WHERE row_num % {sample_interval} = 1
            """
            
            # Read the chunk into a DataFrame
            chunk_df = pd.read_sql(text(query), engine)
            
            if not chunk_df.empty:
                all_chunks.append(chunk_df)
                logging.info(f"Processed chunk with {len(chunk_df)} rows")
            
            current_date = chunk_end_date
            
        # Combine all chunks
        if all_chunks:
            final_df = pd.concat(all_chunks, ignore_index=True)
            
            # Save to parquet file
            output_file = f"data/processed_ais_data_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_sampled_{sample_interval}.parquet"
            final_df.to_parquet(output_file, engine='pyarrow', index=False)
            logging.info(f"Saved processed data to {output_file}")
            
            return final_df
        else:
            logging.warning("No data found in the specified date range")
            return None
            
    finally:
        connector.close()

def prepare_sankey_data(df, top_n=5):
    """
    Prepare data for Sankey diagram
    Returns separate dataframes for scrubber and non-scrubber ships
    """
    # Get top N ship types and destinations
    top_ship_types = df['ship_type'].value_counts().nlargest(top_n).index
    top_destinations = df['destination'].value_counts().nlargest(top_n).index
    
    # Filter data for top ship types and destinations
    filtered_df = df[
        (df['ship_type'].isin(top_ship_types)) & 
        (df['destination'].isin(top_destinations))
    ]
    
    # Split into scrubber and non-scrubber
    scrubber_df = filtered_df[filtered_df['has_scrubber'] == True]
    non_scrubber_df = filtered_df[filtered_df['has_scrubber'] == False]
    
    # Create flow data for each group
    def create_flow_data(group_df):
        flow_data = group_df.groupby(['ship_type', 'destination']).size().reset_index(name='value')
        return flow_data
    
    return create_flow_data(scrubber_df), create_flow_data(non_scrubber_df)

def prepare_time_series_data(df):
    """
    Prepare data for time series plot of scrubber vs non-scrubber ships
    """
    # Convert timestamp to date
    df['date'] = pd.to_datetime(df['timestamp_collected']).dt.date
    
    # Count unique ships per day by scrubber status
    daily_counts = df.groupby(['date', 'has_scrubber'])['imo_number'].nunique().reset_index()
    
    # Pivot the data for plotting
    time_series_data = daily_counts.pivot(
        index='date', 
        columns='has_scrubber', 
        values='imo_number'
    ).fillna(0)
    
    time_series_data.columns = ['Non-Scrubber', 'Scrubber']
    return time_series_data

def prepare_spatial_data(df):
    """
    Prepare data for spatial distribution map
    """
    # Get the most recent position for each ship
    latest_positions = df.sort_values('timestamp_collected').groupby('imo_number').last()
    
    # Split into scrubber and non-scrubber
    scrubber_positions = latest_positions[latest_positions['has_scrubber'] == True]
    non_scrubber_positions = latest_positions[latest_positions['has_scrubber'] == False]
    
    return scrubber_positions, non_scrubber_positions

def analyze_ship_movements(df):
    """
    Analyze ship movements and generate insights
    """
    if df is None or df.empty:
        return
    
    # Basic statistics
    total_ships = df['imo_number'].nunique()
    total_positions = len(df)
    avg_speed = df['sog'].mean()
    
    # Ship type distribution
    ship_type_dist = df.groupby('ship_type').agg({
        'imo_number': 'nunique',
        'position_count': 'sum'
    }).sort_values('imo_number', ascending=False)
    
    # Most common destinations
    top_destinations = df.groupby('destination').size().sort_values(ascending=False).head(10)
    
    # Time-based analysis
    df['hour'] = pd.to_datetime(df['timestamp_collected']).dt.hour
    hourly_activity = df.groupby('hour').size()
    
    # Prepare data for specialized visualizations
    sankey_data_scrubber, sankey_data_non_scrubber = prepare_sankey_data(df)
    time_series_data = prepare_time_series_data(df)
    spatial_data_scrubber, spatial_data_non_scrubber = prepare_spatial_data(df)
    
    # Save all processed data
    sankey_data_scrubber.to_parquet('data/sankey_data_scrubber.parquet')
    sankey_data_non_scrubber.to_parquet('data/sankey_data_non_scrubber.parquet')
    time_series_data.to_parquet('data/time_series_data.parquet')
    spatial_data_scrubber.to_parquet('data/spatial_data_scrubber.parquet')
    spatial_data_non_scrubber.to_parquet('data/spatial_data_non_scrubber.parquet')
    
    # Save analysis results
    with open('data/analysis_results.txt', 'w') as f:
        f.write(f"Total unique ships: {total_ships}\n")
        f.write(f"Total positions recorded: {total_positions}\n")
        f.write(f"Average speed: {avg_speed:.2f} knots\n\n")
        
        f.write("Ship Type Distribution:\n")
        f.write(str(ship_type_dist))
        f.write("\n\nTop 10 Destinations:\n")
        f.write(str(top_destinations))
        f.write("\n\nHourly Activity:\n")
        f.write(str(hourly_activity))

if __name__ == "__main__":
    # Example usage
    start_date = datetime(2025, 4, 19)  # Adjust as needed
    end_date = datetime(2025, 5, 19)   # Adjust as needed
    sample_interval = 100  # Take every 100th record
    
    processed_data = process_data_in_chunks(start_date, end_date, sample_interval=sample_interval)
    analyze_ship_movements(processed_data) 