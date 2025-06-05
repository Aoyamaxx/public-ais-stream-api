import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from datetime import datetime, timedelta

# Read the data
df = pd.read_csv('data/daily_pollution_by_country.csv')
df['date'] = pd.to_datetime(df['date'])

# Filter out INTERNATIONAL category
df = df[df['country'] != 'INTERNATIONAL']

# Create the initial visualization
plt.figure(figsize=(15, 8))
for country in df['country'].unique():
    country_data = df[df['country'] == country]
    plt.plot(country_data['date'], country_data['pollution'], label=country, marker='o', markersize=2)

plt.title('Daily Pollution by Country')
plt.xlabel('Date')
plt.ylabel('Total Discharge (kg)')
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)

# Format y-axis labels to show values in millions of kg
plt.ticklabel_format(axis='y', style='plain', useOffset=False)
plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: '{:.1f}M'.format(x/1e6)))

plt.tight_layout()
plt.savefig('pollution_by_country.png')
plt.close()

# Prepare data for forecasting
def prepare_forecast_data(df, country):
    country_data = df[df['country'] == country].sort_values('date')
    
    # Create features
    country_data['day_of_week'] = country_data['date'].dt.dayofweek
    country_data['month'] = country_data['date'].dt.month
    country_data['day'] = country_data['date'].dt.day
    
    # Create lag features
    for lag in [1, 7, 14]:
        country_data[f'lag_{lag}'] = country_data['pollution'].shift(lag)
    
    # Drop rows with NaN values
    country_data = country_data.dropna()
    
    # Prepare features and target
    features = ['day_of_week', 'month', 'day', 'lag_1', 'lag_7', 'lag_14']
    X = country_data[features]
    y = country_data['pollution']
    
    return X, y, country_data

# Train model and make predictions for each country
def forecast_country(df, country):
    X, y, country_data = prepare_forecast_data(df, country)
    
    # Train model
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X, y)
    
    # Make predictions for historical data
    historical_predictions = model.predict(X)
    
    # Prepare future dates
    last_date = country_data['date'].max()
    future_dates = pd.date_range(start=last_date, periods=31, freq='D')
    
    # Create future features
    future_data = pd.DataFrame({
        'date': future_dates,
        'day_of_week': future_dates.dayofweek,
        'month': future_dates.month,
        'day': future_dates.day
    })
    
    # Initialize predictions array
    future_predictions = []
    last_values = country_data['pollution'].values[-14:]
    
    # Make predictions one day at a time
    for i in range(len(future_data)):
        # Create features for this day
        features = {
            'day_of_week': future_data.iloc[i]['day_of_week'],
            'month': future_data.iloc[i]['month'],
            'day': future_data.iloc[i]['day'],
            'lag_1': last_values[-1] if i == 0 else future_predictions[-1],
            'lag_7': last_values[-(7-i)] if i < 7 else future_predictions[-(7-i)],
            'lag_14': last_values[-(14-i)] if i < 14 else future_predictions[-(14-i)]
        }
        
        # Make prediction
        prediction = model.predict(pd.DataFrame([features]))[0]
        future_predictions.append(prediction)
    
    future_data['prediction'] = future_predictions
    
    # Calculate confidence intervals using tree variance
    predictions = []
    for estimator in model.estimators_:
        tree_predictions = []
        for i in range(len(future_data)):
            features = {
                'day_of_week': future_data.iloc[i]['day_of_week'],
                'month': future_data.iloc[i]['month'],
                'day': future_data.iloc[i]['day'],
                'lag_1': last_values[-1] if i == 0 else tree_predictions[-1],
                'lag_7': last_values[-(7-i)] if i < 7 else tree_predictions[-(7-i)],
                'lag_14': last_values[-(14-i)] if i < 14 else tree_predictions[-(14-i)]
            }
            tree_predictions.append(estimator.predict(pd.DataFrame([features]))[0])
        predictions.append(tree_predictions)
    
    predictions = np.array(predictions)
    std = np.std(predictions, axis=0)
    
    return country_data, historical_predictions, future_data, std

# Get all forecasts first to determine global y-axis limits
all_forecasts = {}
max_pollution = 0
min_pollution = float('inf')

for country in df['country'].unique():
    country_data, historical_predictions, future_data, std = forecast_country(df, country)
    all_forecasts[country] = (country_data, historical_predictions, future_data, std)
    
    # Update global min/max
    max_pollution = max(max_pollution, 
                       country_data['pollution'].max(),
                       future_data['prediction'].max() + 1.96 * std.max())
    min_pollution = min(min_pollution,
                       country_data['pollution'].min(),
                       future_data['prediction'].min() - 1.96 * std.min())

# Plot results with standardized scales
for country, (country_data, historical_predictions, future_data, std) in all_forecasts.items():
    plt.figure(figsize=(15, 8))
    
    # Plot historical data
    plt.plot(country_data['date'], country_data['pollution'], 
             label='Actual', color='blue', marker='o', markersize=2)
    
    # Plot historical predictions
    plt.plot(country_data['date'], historical_predictions, 
             label='Model Fit', color='green', linestyle='--')
    
    # Prepend the last actual data point to the forecast data for plotting
    forecast_dates_plot = pd.concat([country_data['date'].tail(1), future_data['date']])
    forecast_predictions_plot = pd.concat([country_data['pollution'].tail(1), future_data['prediction']])
    forecast_std_plot = np.concatenate([np.array([0]), std]) # Add 0 std for the last actual point

    # Plot future predictions
    plt.plot(forecast_dates_plot, forecast_predictions_plot,
             label='Forecast', color='red', linestyle='--')
    
    # Plot confidence intervals
    plt.fill_between(forecast_dates_plot,
                     forecast_predictions_plot - 1.96 * forecast_std_plot,
                     forecast_predictions_plot + 1.96 * forecast_std_plot,
                     color='red', alpha=0.2, label='95% Confidence Interval')
    
    # Set standardized y-axis limits
    plt.ylim(min_pollution, max_pollution)
    
    # Format y-axis labels to show values in millions of kg
    plt.ticklabel_format(axis='y', style='plain', useOffset=False)
    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: '{:.1f}M'.format(x/1e6)))

    plt.title(f'Pollution Forecast for {country} (kg)')
    plt.xlabel('Date')
    plt.ylabel('Total Discharge (kg)')
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.savefig(f'forecast_{country}.png')
    plt.close() 