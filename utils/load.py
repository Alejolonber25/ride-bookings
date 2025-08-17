import pandas as pd
from .queries import total_income, average_distance, apparent_cancellation_rate

def save_data(df: pd.DataFrame, path: str):
    pass

def generate_metrics(df):
    print("Metrics")
    print(f"Total Income: ${total_income(df):,.2f}")
    print(f"Average Distance: {average_distance(df):.2f} km")
    print(f"Apparent Cancellation Rate: {apparent_cancellation_rate(df):.2%}")