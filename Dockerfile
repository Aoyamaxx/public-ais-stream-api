# Use official Python image as base
FROM python:3.11

# Set working directory inside container
WORKDIR /app

# Install system dependencies for GeoPandas
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    libspatialindex-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables for GDAL
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Copy requirements first for better caching
COPY requirements.txt /app/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all other files to /app inside the container
COPY . /app

# Copy shapefile directory to the container
COPY north_sea_watch_region_merged /app/north_sea_watch_region_merged

# Run the AIS collection script by default
CMD ["python", "ais_collector.py"]
