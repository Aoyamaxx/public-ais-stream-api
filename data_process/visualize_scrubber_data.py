import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import folium
from folium.plugins import HeatMap
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_ship_type_mapping():
    """Load ship type mapping from CSV"""
    try:
        return pd.read_csv('data/ship_type_codes.csv')
    except FileNotFoundError:
        logging.error("ship_type_codes.csv not found")
        return None

def normalize_destinations(df):
    """Normalize destination names using mapping and regex rules"""
    # Mapping dictionary for port abbreviations to full names
    port_mapping = {
        'NLRTM': 'ROTTERDAM',
        'NLAMS': 'AMSTERDAM',
        'NLVLI': 'VLISSINGEN',
        'BEANR': 'ANTWERP',
        'GBLGP': 'LONDON',
        'GBHRW': 'HARWICH',
        'DEHAM': 'HAMBURG',
        'DECUX': 'CUXHAVEN',
        'DKSKA': 'SKAGEN',
        'DEBRV': 'BREMERHAVEN',
        'BEZEE': 'ZEEBRUGGE',
        'NLTNZ': 'TERNEUZEN',
        'ANT': 'ANTWERP',
        'CUX': 'CUXHAVEN'
    }

    # Regex-based correction rules
    corrections = {
        r'^ROT+EDAM$': 'ROTTERDAM',
        r'^NLROTTERDAM$': 'ROTTERDAM',
        r'^ROTTERDAM.*$': 'ROTTERDAM',
        r'^AMS$': 'AMSTERDAM',
        r'^AMSTERD[A-Z]*$': 'AMSTERDAM',
        r'^VLISS?INGEN$': 'VLISSINGEN',
        r'^LIVERPOOLTUGOPS$': 'LIVERPOOL',
        r'^HARBOURTOWAGE$': 'HARBOURTUG',
        r'^FISHINGGROUNDS?$': 'FISHING',
        r'^FISHGORUND$': 'FISHING',
        r'^0OSTENDE$': 'OOSTENDE',
        r'^0$': np.nan,
        r'^GBLGP.*$': 'LONDON',
        r'^GBHRW.*$': 'HARWICH',
        r'^DEHAM.*$': 'HAMBURG',
        r'^BERGENMOBERGEN$': 'BERGEN',
        r'^ANTWERPEN$': 'ANTWERP',
        r'^ROTTHULLROTT$': 'ROTTERDAM',  # Will be split into ROTTERDAM and HULL
        r'^ROTTHULL$': 'ROTTERDAM',      # Will be split into ROTTERDAM and HULL
        r'^HULLROTT$': 'HULL'            # Will be split into HULL and ROTTERDAM
    }

    # Clean and standardize destinations
    df['destination'] = df['destination'].str.strip()
    df['destination'] = df['destination'].str.upper()
    df['destination'] = df['destination'].str.replace(r'\s+', '', regex=True)
    df['destination'] = df['destination'].str.replace(r'[^A-Z0-9]', '', regex=True)

    # Apply mappings and corrections
    df['destination'] = df['destination'].replace(port_mapping)
    
    for pattern, replacement in corrections.items():
        if not isinstance(replacement, str):
            df['destination'] = df['destination'].str.replace(pattern, '', regex=True)
        else:
            df['destination'] = df['destination'].str.replace(pattern, replacement, regex=True)

    # Handle combined destinations (e.g., NLRTMGBHRW, ROTTHULLROTT)
    # Create a new column for secondary destinations
    df['secondary_destination'] = None
    
    # Check for combined destinations and split them
    for idx, row in df.iterrows():
        dest = row['destination']
        if dest and len(dest) > 5:  # If destination is longer than typical port code
            # Special case for ROTTHULLROTT pattern
            if 'ROTT' in dest and 'HULL' in dest:
                df.at[idx, 'destination'] = 'ROTTERDAM'
                df.at[idx, 'secondary_destination'] = 'HULL'
            else:
                # Try to identify known port codes in the combined string
                for code, name in port_mapping.items():
                    if code in dest:
                        # If we find a match, update the destination and set secondary
                        if df.at[idx, 'destination'] == dest:  # Only if not already processed
                            df.at[idx, 'destination'] = name
                            # Look for other known ports in the string
                            remaining = dest.replace(code, '')
                            for other_code, other_name in port_mapping.items():
                                if other_code in remaining:
                                    df.at[idx, 'secondary_destination'] = other_name
                                    break

    # Convert empty strings to NaN
    df['destination'] = df['destination'].replace('', np.nan)
    
    return df

def create_sankey_diagram(df, is_scrubber=True):
    """Create Sankey diagram for ship types to destinations"""
    # Filter data based on scrubber status
    filtered_df = df[df['has_scrubber'] == is_scrubber]
    
    # Get top 5 ship types
    top_ship_types = filtered_df['ship_type'].value_counts().nlargest(5).index
    
    # Combine primary and secondary destinations for counting
    all_destinations = pd.concat([
        filtered_df['destination'],
        filtered_df['secondary_destination']
    ]).dropna()
    
    # Get top 5 destinations from combined list
    top_destinations = all_destinations.value_counts().nlargest(5).index
    
    # Create flow data for primary destinations - count unique ships
    flow_data = filtered_df[
        filtered_df['ship_type'].isin(top_ship_types) & 
        filtered_df['destination'].isin(top_destinations)
    ].groupby(['ship_type', 'destination'])['imo_number'].nunique().reset_index(name='value')
    
    # Add flow data for secondary destinations - count unique ships
    if 'secondary_destination' in filtered_df.columns:
        secondary_flows = filtered_df[
            filtered_df['ship_type'].isin(top_ship_types) & 
            filtered_df['secondary_destination'].isin(top_destinations)
        ].groupby(['ship_type', 'secondary_destination'])['imo_number'].nunique().reset_index(name='value')
        secondary_flows = secondary_flows.rename(columns={'secondary_destination': 'destination'})
        flow_data = pd.concat([flow_data, secondary_flows])
    
    # Convert ship types to uppercase for display
    display_ship_types = [ship_type.upper() for ship_type in top_ship_types]
    display_destinations = list(top_destinations)
    
    # Prepare labels and indices
    labels = display_ship_types + display_destinations
    ship_type_to_index = {ship.upper(): i for i, ship in enumerate(top_ship_types)}
    destination_to_index = {dest: i + len(display_ship_types) for i, dest in enumerate(display_destinations)}
    
    # Define consistent colors for ship types and destinations
    ship_type_colors = {
        'CARGO': '#1f77b4',  # Blue
        'TANKER': '#ff7f0e',  # Orange
        'OTHER TYPE': '#2ca02c',  # Green
        'TUG': '#d62728',  # Red
        'FISHING': '#9467bd',  # Purple
        'PASSENGER': '#e377c2',  # Pink
    }
    
    destination_colors = {
        'ROTTERDAM': '#8c564b',  # Brown
        'AMSTERDAM': '#e377c2',  # Pink
        'ANTWERP': '#7f7f7f',  # Gray
        'HAMBURG': '#bcbd22',  # Yellow-Green
        'LONDON': '#17becf',  # Cyan
        'FISHING': '#7f7f7f',  # Gray,
        'BREMERHAVEN': '#c8491f' # Orange
    }
    
    # Create node colors list
    node_colors = []
    for label in labels:
        if label in ship_type_colors:
            node_colors.append(ship_type_colors[label])
        elif label in destination_colors:
            node_colors.append(destination_colors[label])
        else:
            # Fallback color for any unexpected labels
            node_colors.append('#636363')
    
    # Create source, target, and value lists
    source = []
    target = []
    values = []
    
    for _, row in flow_data.iterrows():
        st = row['ship_type'].upper()  # Convert to uppercase for matching
        dest = row['destination']
        if st in ship_type_to_index and dest in destination_to_index:
            source.append(ship_type_to_index[st])
            target.append(destination_to_index[dest])
            values.append(row['value'])
    
    # Create Sankey diagram
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=labels,
            color=node_colors
        ),
        link=dict(
            source=source,
            target=target,
            value=values,
            color=['rgba(128,128,128,0.4)'] * len(source)  # Semi-transparent gray for links
        )
    )])
    
    title = "Sankey Diagram: Top Ship Types to Top Destinations (Scrubber Ships)" if is_scrubber else "Sankey Diagram: Top Ship Types to Top Destinations (Non-Scrubber Ships)"
    fig.update_layout(
        title_text=title,
        font_size=18,  # Increased font size
        title_font_size=24,  # Even larger font size for title
        font_family="Arial"
    )
    
    # Save the figure
    output_file = 'graphs/scrubber_sankey.html' if is_scrubber else 'graphs/non_scrubber_sankey.html'
    fig.write_html(output_file)
    logging.info(f"Saved Sankey diagram to {output_file}")

def create_time_series_plot(df):
    """Create time series plot of scrubber vs non-scrubber ships"""
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
    
    # Create the plot
    fig = go.Figure()
    
    # Add non-scrubber line
    fig.add_trace(go.Scatter(
        x=time_series_data.index,
        y=time_series_data['Non-Scrubber'],
        name='Non-Scrubber Ships',
        line=dict(color='lightblue')
    ))
    
    # Add scrubber line
    fig.add_trace(go.Scatter(
        x=time_series_data.index,
        y=time_series_data['Scrubber'],
        name='Scrubber Ships',
        line=dict(color='red')
    ))
    
    fig.update_layout(
        title='Number of Ships Over Time',
        xaxis_title='Date',
        yaxis_title='Number of Ships',
        hovermode='x unified'
    )
    
    # Save the figure
    fig.write_html('data/ship_counts_time_series.html')
    logging.info("Saved time series plot to graphs/ship_counts_time_series.html")

def create_spatial_map(df):
    """Create spatial distribution map with radiation-style heatmap of scrubber ships"""
    logging.info("Starting spatial map creation...")
    
    # Get the most recent position for each ship
    latest_positions = df.sort_values('timestamp_collected').groupby('imo_number').last()
        
    # Only focus on scrubber ships
    scrubber_positions = latest_positions[latest_positions['has_scrubber'] == True]
    
    # Validate coordinates and convert to float explicitly
    valid_scrubber = scrubber_positions.dropna(subset=['latitude', 'longitude'])
    
    # Convert coordinates to proper numeric types
    for col in ['latitude', 'longitude']:
        valid_scrubber[col] = pd.to_numeric(valid_scrubber[col], errors='coerce')
    
    # Drop any rows that couldn't be converted to numeric
    valid_scrubber = valid_scrubber.dropna(subset=['latitude', 'longitude'])    
    # Create base map with dark theme for better visualization of heat effect
    m = folium.Map(
        location=[56, 3],  # Center on North Sea
        zoom_start=6,
        tiles='CartoDB dark_matter'
    )
    
    # Add alternative tile layers
    folium.TileLayer('CartoDB positron').add_to(m)
    folium.TileLayer('OpenStreetMap').add_to(m)
    
    # Collect heatmap data - only using scrubber ships
    heatmap_data = []
    ship_locations = []  # Store for later use
    
    for idx, row in valid_scrubber.iterrows():
        try:
            lat, lon = float(row['latitude']), float(row['longitude'])
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                heatmap_data.append([lat, lon, 1])  # Weight of 1 for the basic heatmap
                ship_locations.append([lat, lon])
        except Exception as e:
            logging.error(f"Error processing coordinates for row {idx}: {e}")
        
    # Create a more intense radiation effect by adding multiple heatmap layers
    # with different radiuses and intensities
    if heatmap_data:
        # Primary heat layer - large radius, low intensity
        HeatMap(
            data=heatmap_data,
            radius=30,  # Larger radius for wider spread
            blur=20,    # More blur for softer edges
            gradient={
                '0.2': 'blue',
                '0.4': 'lime',
                '0.6': 'yellow',
                '0.8': 'orange',
                '1.0': 'red'
            },
            min_opacity=0.3,
            max_zoom=8
        ).add_to(m)
        
        # Secondary heat layer - smaller radius, higher intensity at center
        HeatMap(
            data=heatmap_data,
            radius=15,
            blur=10,
            gradient={
                '0.4': 'rgba(0,0,255,0.7)',  # Semi-transparent blue
                '0.65': 'rgba(0,255,0,0.8)',  # Semi-transparent green
                '0.85': 'rgba(255,255,0,0.9)', # Semi-transparent yellow
                '1.0': 'rgba(255,0,0,1)'       # Solid red
            },
            min_opacity=0.5,
            max_zoom=12
        ).add_to(m)
    else:
        logging.warning("No valid data for heatmap!")
    
    # Add layer control
    folium.LayerControl().add_to(m)
    
    # Add an improved legend that better explains the visualization
    legend_html = """
    <div style="position: fixed; 
              bottom: 50px; left: 50px; width: 220px; 
              border: 2px solid grey; z-index: 9999; background-color: rgba(255,255,255,0.8);
              padding: 10px; font-size: 14px;">
      <p style="text-align:center; margin:0; font-weight:bold;">Scrubber Discharge</p>
      <div style="display: flex; align-items: center; margin-top: 8px;">
        <div style="flex-grow: 1; height: 20px; background: linear-gradient(to right, blue, lime, yellow, red);"></div>
      </div>
      <div style="display: flex; justify-content: space-between; font-size: 12px;">
        <span>Low</span>
        <span>High</span>
      </div>
      <p style="margin-top: 10px; font-size: 12px;">Shows radiation pattern of <br>scrubber discharge in<br>the North Sea region.</p>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
    
    # Save the heatmap
    m.save('data/imputed_scrubber_heatmap.html')
    logging.info("Saved radiation-style heatmap to graphs/scrubber_heatmap.html")
    
    # Create an alternative visualization with markers
    marker_map = folium.Map(
        location=[56, 3],
        zoom_start=6,
        tiles='CartoDB positron'
    )
    
    # Add scrubber ships only
    scrubber_markers = folium.FeatureGroup(name="Scrubber Ships")
    
    for _, row in valid_scrubber.iterrows():
        try:
            lat, lon = float(row['latitude']), float(row['longitude'])
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=3,
                    color='red',
                    fill=True,
                    popup=f"Ship: {row['name']}<br>Type: {row['ship_type']}<br>Destination: {row['destination']}"
                ).add_to(scrubber_markers)
        except Exception as e:
            pass
    
    scrubber_markers.add_to(marker_map)
    folium.LayerControl().add_to(marker_map)
    
    marker_legend = """
    <div style="position: fixed; 
              bottom: 50px; left: 50px; width: 180px; 
              border: 2px solid grey; z-index: 9999; background-color: white;
              padding: 10px; font-size: 14px;">
      <p><strong>Scrubber Discharge Locations</strong></p>
      <p><span style="color: red;">●</span> Ships with Scrubbers</p>
    </div>
    """
    marker_map.get_root().html.add_child(folium.Element(marker_legend))
    marker_map.save('data/imputed_scrubber_points_map.html')
    logging.info("Saved point map to data/scrubber_points_map.html")

def analyze_destinations(df):
    """Analyze and display unique destinations in the dataset"""
    # Get unique destinations and their counts
    unique_dests = df['destination'].value_counts()
    
    # Get unique secondary destinations and their counts
    if 'secondary_destination' in df.columns:
        unique_secondary = df['secondary_destination'].value_counts()
    else:
        unique_secondary = pd.Series()
    
    # # Print results
    # print("\nUnique Primary Destinations:")
    # print("---------------------------")
    # for dest, count in unique_dests.items():
    #     if pd.notna(dest):  # Skip NaN values
    #         print(f"{dest}: {count}")
    
    # if not unique_secondary.empty:
    #     print("\nUnique Secondary Destinations:")
    #     print("-----------------------------")
    #     for dest, count in unique_secondary.items():
    #         if pd.notna(dest):  # Skip NaN values
    #             print(f"{dest}: {count}")
    
    # Save to file for easier inspection
    with open('data/destination_analysis.txt', 'w') as f:
        f.write("Unique Primary Destinations:\n")
        f.write("---------------------------\n")
        for dest, count in unique_dests.items():
            if pd.notna(dest):
                f.write(f"{dest}: {count}\n")
        
        if not unique_secondary.empty:
            f.write("\nUnique Secondary Destinations:\n")
            f.write("-----------------------------\n")
            for dest, count in unique_secondary.items():
                if pd.notna(dest):
                    f.write(f"{dest}: {count}\n")
    
    logging.info("Destination analysis saved to data/destination_analysis.txt")

def main():
    # Load the processed data
    try:
        df = pd.read_parquet('data/processed_ais_data_20250419_20250519_sampled_100.parquet')
    except FileNotFoundError:
        logging.error("Processed data file not found")
        return
    
    # Load and apply ship type mapping
    ship_type_mapping = load_ship_type_mapping()
    if ship_type_mapping is not None:
        # Convert ship_type to string in both dataframes to ensure matching types
        df['ship_type'] = df['ship_type'].astype(str)
        ship_type_mapping['type_code'] = ship_type_mapping['type_code'].astype(str)
        
        # Merge the dataframes
        df = pd.merge(df, ship_type_mapping, left_on='ship_type', right_on='type_code', how='left')
        df['ship_type'] = df['type']
        df = df.drop(['type_code', 'type'], axis=1)
    
    # Normalize destinations
    df = normalize_destinations(df)
    
    # Analyze destinations before creating visualizations
    analyze_destinations(df)
    
    # Create visualizations
    create_sankey_diagram(df, is_scrubber=True)
    create_sankey_diagram(df, is_scrubber=False)
    create_time_series_plot(df)
    create_spatial_map(df)

if __name__ == "__main__":
    main() 