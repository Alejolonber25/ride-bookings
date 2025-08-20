import pandas as pd
from .queries import total_income, average_distance, apparent_cancellation_rate

def save_data(df: pd.DataFrame, path: str) -> None:
    """
    Save the cleaned DataFrame to a CSV file.
    
    Creates the output directory if it doesn't exist and saves the DataFrame as a CSV file without index.
    
    Args:
        df (pd.DataFrame): Cleaned DataFrame to be saved
        path (str): Output file path for the CSV file
        
    Returns:
        None: Saves file to disk and prints confirmation
    """
    import os
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    # Save the CSV file
    df.to_csv(path, index=False)
    print(f"Clean data saved to: {path}")
    print(f"Total rows saved: {len(df)}")
    print("=" * 50)

def generate_metrics(df: pd.DataFrame) -> None:
    """
    Generate and display business metrics from the cleaned DataFrame.
    
    Calculates and prints key business metrics including:
    - Total income from all bookings
    - Average ride distance
    - Apparent cancellation rate
    
    Args:
        df (pd.DataFrame): Cleaned DataFrame containing ride booking data
        
    Returns:
        None: Prints metrics to console
    """
    print("Business Metrics:")
    print(f"Total Income: ${total_income(df):,.2f}")
    print(f"Average Distance: {average_distance(df):.2f} km")
    print(f"Apparent Cancellation Rate: {apparent_cancellation_rate(df):.2%}")
    print("=" * 50)
