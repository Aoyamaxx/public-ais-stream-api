import pandas as pd
import numpy as np
import logging
from typing import Dict, List
import geopandas as gpd
from shapely.geometry import Point, LineString
import requests
import io
import zipfile
import tempfile
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Port rules and restrictions
PORT_RULES = {
    'TILBURY': {'restriction': 'berth', 'discharge_allowed': False},
    'LEITH': {'restriction': 'port', 'discharge_allowed': False},
    'DUNDEE': {'restriction': 'port', 'discharge_allowed': False},
    'AMSTERDAM': {'restriction': 'berth', 'discharge_allowed': False}
}

# Country rules
COUNTRY_RULES = {
    'FRANCE': {'restriction': 'distance', 'distance_nm': 3},
    'BELGIUM': {'restriction': 'distance', 'distance_nm': 3},
    'GERMANY': {'restriction': 'inland_port'}
}


def load_port_coordinates(csv_path: str) -> Dict[str, Dict[str, float]]:
    """Load port coordinates and country information from CSV file."""
    logging.info("Loading port coordinates from CSV...")
    ports_df = pd.read_csv(csv_path)
    
    # Create port coordinates dictionary with country information
    port_coords = {}
    for _, row in ports_df.iterrows():
        port_name = row['PORT_NAME'].upper()
        port_coords[port_name] = {
            'latitude': row['LATITUDE'],
            'longitude': row['LONGITUDE'],
            'country': row['COUNTRY'].upper()  # Add country information
        }
    
    logging.info(f"Loaded coordinates for {len(port_coords)} ports")
    return port_coords

def load_land_data():
    """Load land data for inland detection."""
    logging.info("Loading land data...")
    
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Extract the ZIP file
        with zipfile.ZipFile('data/ne_50m.zip', 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        
        # Find the .shp file in the extracted contents
        shp_file = None
        for file in os.listdir(temp_dir):
            if file.endswith('.shp'):
                shp_file = os.path.join(temp_dir, file)
                break
        
        if shp_file is None:
            raise FileNotFoundError("Could not find shapefile in the downloaded data")
        
        # Read the shapefile
        land_gdf = gpd.read_file(shp_file)
        logging.info("Land data loaded successfully")
        return land_gdf

def is_inland(lat: float, lon: float, land_gdf: gpd.GeoDataFrame) -> bool:
    """Check if a point is inland by examining surrounding coordinates."""
    # Create a point for the ship's location
    ship_point = Point(lon, lat)
    
    # Create points to the left and right of the ship (approximately 1km away)
    left_point = Point(lon - 0.01, lat)  # ~1km to the left
    right_point = Point(lon + 0.01, lat)  # ~1km to the right
    
    # Check if both points are on land
    left_on_land = any(land_gdf.contains(left_point))
    right_on_land = any(land_gdf.contains(right_point))
    
    return left_on_land and right_on_land

def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in nautical miles."""
    R = 3440.065  # Earth's radius in nautical miles
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c

def is_discharge_allowed(row: pd.Series, port_coords: Dict[str, Dict[str, float]], land_gdf: gpd.GeoDataFrame) -> bool:
    """Determine if discharge is allowed based on location and rules."""
    ship_lat = row['latitude']
    ship_lon = row['longitude']
    
    # Check if ship is near any port with restrictions
    for port_name, port_data in port_coords.items():
        port_lat = port_data['latitude']
        port_lon = port_data['longitude']
        
        # Calculate distance to port
        distance = calculate_distance(ship_lat, ship_lon, port_lat, port_lon)
        
        # If ship is within 3 nautical miles of a port
        if distance <= 3:
            if port_name in PORT_RULES:
                rule = PORT_RULES[port_name]
                if rule['restriction'] == 'berth' and row['operation_mode'] == 'Berth':
                    return rule['discharge_allowed']
                elif rule['restriction'] == 'port' and row['operation_mode'] in ['Berth', 'Anchor']:
                    return rule['discharge_allowed']
    
    # Check country rules based on ship's location
    # First determine which country's waters the ship is in
    ship_country = None
    min_distance = float('inf')
    
    for port_name, port_data in port_coords.items():
        port_lat = port_data['latitude']
        port_lon = port_data['longitude']
        distance = calculate_distance(ship_lat, ship_lon, port_lat, port_lon)
        
        if distance < min_distance:
            min_distance = distance
            ship_country = port_data['country']  # Use country from port data
    
    if ship_country in COUNTRY_RULES:
        rule = COUNTRY_RULES[ship_country]
        if rule['restriction'] == 'distance':
            # Check if ship is within restricted distance of any port
            for port_name, port_data in port_coords.items():
                port_lat = port_data['latitude']
                port_lon = port_data['longitude']
                distance = calculate_distance(ship_lat, ship_lon, port_lat, port_lon)
                if distance <= rule['distance_nm']:
                    return False
        elif rule['restriction'] == 'inland_port':
            return not is_inland(ship_lat, ship_lon, land_gdf)
    
    return True  # Default to allowed if no rules apply

def calculate_pollution(df: pd.DataFrame, port_coords: Dict[str, Dict[str, float]], land_gdf: gpd.GeoDataFrame) -> Dict:
    """Calculate total pollution and pollution by national area with daily breakdown."""
    # Sort by timestamp to calculate time differences
    df = df.sort_values(['imo_number', 'timestamp_collected'])
    
    # Calculate time difference in hours between consecutive records for each ship
    df['time_diff'] = df.groupby('imo_number')['timestamp_collected'].diff().dt.total_seconds() / 3600
    
    # Add discharge_allowed column
    df['discharge_allowed'] = df.apply(lambda row: is_discharge_allowed(row, port_coords, land_gdf), axis=1)
    
    # Calculate pollution based on operation mode and time duration
    df['pollution'] = np.where(
        df['discharge_allowed'],
        np.select(
            [
                df['operation_mode'] == 'Berth',
                df['operation_mode'] == 'Anchor',
                df['operation_mode'] == 'Maneuver',
                df['operation_mode'] == 'Cruise'
            ],
            [
                df['emission_berth'] * df['time_diff'],
                df['emission_anchor'] * df['time_diff'],
                df['emission_maneuver'] * df['time_diff'],
                df['emission_cruise'] * df['time_diff']
            ],
            default=0
        ),
        0
    )
    
    # Add date column for daily aggregation
    df['date'] = df['timestamp_collected'].dt.date
    
    # Calculate total pollution (in cubic meters)
    total_pollution = df['pollution'].sum()
    
    # Determine country based on nearest port
    def get_nearest_country(row):
        min_distance = float('inf')
        nearest_country = 'UNKNOWN'
        
        for port_name, port_data in port_coords.items():
            distance = calculate_distance(row['latitude'], row['longitude'], 
                                       port_data['latitude'], port_data['longitude'])
            if distance < min_distance:
                min_distance = distance
                nearest_country = port_data['country']
        
        # Check if the ship is within 12nm of any port
        if min_distance <= 12:
            return nearest_country
        else:
            return 'INTERNATIONAL'  # Ship is in international waters
    
    # Update the country column in the DataFrame
    df['country'] = df.apply(get_nearest_country, axis=1)
    
    # Calculate daily pollution by country
    daily_pollution = df.groupby(['date', 'country'])['pollution'].sum().reset_index()
    
    # Calculate total pollution by country
    pollution_by_country = df.groupby('country')['pollution'].sum()
    
    # Calculate average hourly rates for reporting
    total_hours = df['time_diff'].sum()
    avg_hourly_rate = total_pollution / total_hours if total_hours > 0 else 0
    
    return {
        'total_pollution': total_pollution,  # Total kg
        'total_hours': total_hours,  # Total hours of operation
        'avg_hourly_rate': avg_hourly_rate,  # Average kg per hour
        'pollution_by_country': pollution_by_country,  # Total kg by country
        'daily_pollution': daily_pollution  # Daily pollution by country
    }

def main():
    # Load the augmented AIS data
    logging.info("Loading augmented AIS data...")
    df = pd.read_parquet('data/augmented_ais_data.parquet')
    
    # Load port coordinates from CSV
    port_coords = load_port_coordinates('data_process/filtered_ports_with_x_y.csv')
    
    # Load land data for inland detection
    land_gdf = load_land_data()
    
    # Calculate pollution
    logging.info("Calculating pollution...")
    results = calculate_pollution(df, port_coords, land_gdf)
    
    # Print summary results
    logging.info(f"Total pollution: {results['total_pollution']:.2f} kg")
    logging.info(f"Total operation time: {results['total_hours']:.2f} hours")
    logging.info(f"Average hourly rate: {results['avg_hourly_rate']:.2f} kg/hour")
    logging.info("\nPollution by country (kg):")
    for country, pollution in results['pollution_by_country'].items():
        logging.info(f"{country}: {pollution:.2f}")
    
    # Save daily pollution data
    daily_pollution_df = results['daily_pollution']
    daily_pollution_df.to_csv('data/daily_pollution_by_country.csv', index=False)
    logging.info("Daily pollution data saved to data/daily_pollution_by_country.csv")
    
    # Save summary results
    summary_df = pd.DataFrame({
        'country': results['pollution_by_country'].index,
        'total_pollution_kg': results['pollution_by_country'].values,
        'total_hours': results['total_hours'],
        'avg_hourly_rate_kg_per_hour': results['avg_hourly_rate']
    })
    summary_df.to_csv('data/pollution_analysis_results.csv', index=False)
    logging.info("Summary results saved to data/pollution_analysis_results.csv")

if __name__ == "__main__":
    main() 