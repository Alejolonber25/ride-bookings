import pandas as pd

def total_income(df: pd.DataFrame) -> float:
    """
    Calculate total income by summing all booking_value entries.
    
    This function processes the booking_value column to compute the total revenue
    generated from all rides, filtering out null values to ensure accuracy.
    
    Args:
        df (pd.DataFrame): Input DataFrame containing booking_value column
        
    Returns:
        float: Total income from all bookings, 0.0 if no booking_value column exists
    """
    if 'booking_value' in df.columns:
        # Filter out null values and sum
        total = df['booking_value'].dropna().sum()
        return float(total)
    return 0.0

def average_distance(df: pd.DataFrame) -> float:
    """
    Calculate the average distance of all rides.
    
    This function computes the mean distance from the ride_distance column,
    excluding null values to provide an accurate average ride distance.
    
    Args:
        df (pd.DataFrame): Input DataFrame containing ride_distance column
        
    Returns:
        float: Average distance in kilometers, 0.0 if no ride_distance column exists
    """
    if 'ride_distance' in df.columns:
        # Calculate mean of non-null values
        avg = df['ride_distance'].dropna().mean()
        return float(avg) if pd.notna(avg) else 0.0
    return 0.0

def apparent_cancellation_rate(df: pd.DataFrame) -> float:
    """
    Calculate the apparent cancellation rate based on booking_status.
    
    This function determines the percentage of bookings that were cancelled
    by either customers or drivers, using case-insensitive pattern matching
    on the booking_status column.
    
    Args:
        df (pd.DataFrame): Input DataFrame containing booking_status column
        
    Returns:
        float: Cancellation rate as a decimal (0.0 to 1.0), 0.0 if no data
    """
    if 'booking_status' in df.columns:
        total_bookings = len(df)
        if total_bookings == 0:
            return 0.0
        
        # Count rows containing "cancel" (case insensitive)
        cancelled_bookings = df['booking_status'].str.contains('cancel', case=False, na=False).sum()
        
        return float(cancelled_bookings / total_bookings)
    return 0.0
