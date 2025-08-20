"""
Main ETL Pipeline for Ride Booking Data Processing

This module contains the main ETL pipeline that processes ride booking data
from CSV files, cleans and transforms the data, and generates business metrics.

The pipeline includes:
- Data extraction from CSV
- Data standardization and cleaning
- Feature engineering
- Outlier detection and removal
- Business metrics generation
"""

from utils.extract import extract_data
from utils.load import generate_metrics, save_data
from utils.transform import transform_data

INPUT_PATH = "data/ncr_ride_bookings.csv"
OUTPUT_PATH = "output/ride_bookings.csv"

def etl_pipeline_ride_bookings(input_path: str, output_path: str) -> None:
    """
    Execute the complete ETL pipeline for ride booking data.
    
    This function orchestrates the entire data processing workflow:
    1. Extract data from CSV file
    2. Transform and clean the data
    3. Load/save the cleaned data
    4. Generate business metrics
    
    Args:
        input_path (str): Path to the input CSV file
        output_path (str): Path where cleaned data will be saved
        
    Returns:
        None: Processes data and saves results to file
    """
    # Extract: Load data from CSV
    raw_data = extract_data(input_path)
    
    # Transform: Clean and process the data
    cleaned_data = transform_data(raw_data)

    # Load: Save cleaned data to output file
    save_data(cleaned_data, output_path)

    # Generate business metrics
    generate_metrics(cleaned_data)

if __name__ == "__main__":
    etl_pipeline_ride_bookings(INPUT_PATH, OUTPUT_PATH)
