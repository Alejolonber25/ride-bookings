import pandas as pd

def to_snake_case(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    return df

def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.lower().str.replace(" ", "_")
    return df

def create_datetime(df: pd.DataFrame) -> pd.DataFrame:
    if 'date' in df.columns and 'time' in df.columns:
        df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'])
        df = df.drop(['date', 'time'], axis=1)
    return df

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    duplicados = df[df.duplicated(subset=["booking_id"], keep=False)]
    if not duplicados.empty:
        print("Algunos duplicados encontrados por booking_id:")
        print(duplicados.head(10))
        conteo_duplicados = df.duplicated(subset=["booking_id"]).sum()
        print(f"Número total de filas duplicadas por booking_id: {conteo_duplicados}")
    else:
        print("No se encontraron duplicados por booking_id")
        
    df = df.drop_duplicates(subset=["booking_id"])
    return df

def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    if 'booking_value' in df.columns:
        Q1 = df['booking_value'].quantile(0.25)
        Q3 = df['booking_value'].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        outliers_booking = df[(df['booking_value'] < lower_bound) | (df['booking_value'] > upper_bound)]
        print(f"Outliers encontrados en booking_value: {len(outliers_booking)}")
        
        df = df[(df['booking_value'] >= lower_bound) & (df['booking_value'] <= upper_bound)]
    
    if 'ride_distance' in df.columns:
        Q1 = df['ride_distance'].quantile(0.25)
        Q3 = df['ride_distance'].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        outliers_distance = df[(df['ride_distance'] < lower_bound) | (df['ride_distance'] > upper_bound)]
        print(f"Outliers encontrados en ride_distance: {len(outliers_distance)}")
        
        df = df[(df['ride_distance'] >= lower_bound) & (df['ride_distance'] <= upper_bound)]
    
    for rating_col in ['driver_ratings', 'customer_rating']:
        if rating_col in df.columns:
            outliers_rating = df[(df[rating_col] < 0) | (df[rating_col] > 5)]
            if len(outliers_rating) > 0:
                print(f"Outliers encontrados en {rating_col}: {len(outliers_rating)}")
                df = df[(df[rating_col] >= 0) & (df[rating_col] <= 5)]
    
    for time_col in ['avg_vtat', 'avg_ctat']:
        if time_col in df.columns:
            Q1 = df[time_col].quantile(0.25)
            Q3 = df[time_col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers_time = df[(df[time_col] < lower_bound) | (df[time_col] > upper_bound)]
            print(f"Outliers encontrados en {time_col}: {len(outliers_time)}")
            
            df = df[(df[time_col] >= lower_bound) & (df[time_col] <= upper_bound)]
    
    numeric_cols = ['booking_value', 'ride_distance', 'avg_vtat', 'avg_ctat', 
                   'cancelled_rides_by_customer', 'cancelled_rides_by_driver', 'incomplete_rides']
    for col in numeric_cols:
        if col in df.columns:
            negative_count = (df[col] < 0).sum()
            if negative_count > 0:
                print(f"Valores negativos eliminados en {col}: {negative_count}")
                df = df[df[col] >= 0]
    
    return df

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    df = to_snake_case(df)
    df = convert_types(df)
    df = create_datetime(df)
    df = remove_duplicates(df)
    df = remove_outliers(df)
    return df