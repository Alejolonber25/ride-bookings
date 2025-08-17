from utils.extract import extract_data
from utils.load import generate_metrics, save_data
from utils.transform import transform_data

INPUT_PATH = "data/ncr_ride_bookings.csv"
OUTPUT_PATH = "output/ride_bookings.csv"

def etl_pipeline_ride_bookings(input_path: str, output_path: str):
    df = extract_data(input_path)
    df_clean = transform_data(df)
    print(df_clean.head())
    print(df_clean.info())

    save_data(df_clean, output_path)

    generate_metrics(df_clean)

if __name__ == "__main__":
    etl_pipeline_ride_bookings(INPUT_PATH, OUTPUT_PATH)