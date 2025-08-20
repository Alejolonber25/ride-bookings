import pandas as pd

def extract_data(path: str) -> pd.DataFrame:
    """
    Extract data from a CSV file and return as DataFrame.
    
    Args:
        path (str): File path to the CSV file to be loaded
        
    Returns:
        pd.DataFrame: DataFrame containing the loaded CSV data
    """
    return pd.read_csv(path)
