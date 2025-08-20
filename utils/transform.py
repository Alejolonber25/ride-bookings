import pandas as pd


def to_snake_case(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert DataFrame column names to snake_case format.

    Args:
        df (pd.DataFrame): Input DataFrame with original column names

    Returns:
        pd.DataFrame: DataFrame with normalized column names in snake_case
    """
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    return df


def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert and normalize data types in the DataFrame.

    This function:
    1. Converts numeric columns from string to appropriate numeric types
    2. Normalizes categorical string columns (lowercase, underscores)

    Args:
        df (pd.DataFrame): Input DataFrame with mixed data types

    Returns:
        pd.DataFrame: DataFrame with proper data types and normalized values
    """
    # Define numeric columns
    numeric_columns = [
        "avg_vtat",
        "avg_ctat",
        "cancelled_rides_by_customer",
        "cancelled_rides_by_driver",
        "incomplete_rides",
        "booking_value",
        "ride_distance",
        "driver_ratings",
        "customer_rating",
    ]

    # Convert numeric columns to proper types
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col])

    # Define categorical columns
    categorical_columns = [
        "booking_status",
        "vehicle_type",
        "pickup_location",
        "drop_location",
        "reason_for_cancelling_by_customer",
        "driver_cancellation_reason",
        "incomplete_rides_reason",
        "payment_method",
    ]

    # Normalize categorical columns
    for col in categorical_columns:
        if col in df.columns and df[col].dtype == "object":
            df[col] = df[col].str.lower().str.replace(" ", "_")

    return df


def create_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Combine separate date and time columns into a single datetime column.
    Drops the original date and time columns after combination.

    Args:
        df (pd.DataFrame): Input DataFrame with 'date' and 'time' columns

    Returns:
        pd.DataFrame: DataFrame with combined 'datetime' column
    """
    if "date" in df.columns and "time" in df.columns:
        df["datetime"] = pd.to_datetime(df["date"] + " " + df["time"])
        df = df.drop(["date", "time"], axis=1)
    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate records based on booking_id and report findings.

    Args:
        df (pd.DataFrame): Input DataFrame potentially containing duplicates

    Returns:
        pd.DataFrame: DataFrame with duplicates removed based on booking_id
    """
    duplicate_records = df[df.duplicated(subset=["booking_id"], keep=False)]
    if not duplicate_records.empty:
        print(f"Some duplicates found by booking_id")
        duplicate_count = df.duplicated(subset=["booking_id"]).sum()
        print(f"Total number of duplicate rows by booking_id: {duplicate_count}")
    else:
        print("No duplicates found by booking_id")

    df = df.drop_duplicates(subset=["booking_id"])
    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove outliers from the DataFrame considering business rules based on booking_status.

    Business Rules by Status:
    - Completed: Must have booking_value, ride_distance, ratings; cancellation reasons should be null
    - Cancelled by Customer: Must have customer cancellation reason; ratings should be null
    - Cancelled by Driver: Must have driver cancellation reason; ratings should be null
    - Incomplete: Must have incomplete reason and booking_value; ratings should be null
    - Driver not found: Various columns may be null; ratings should be null

    Args:
        df (pd.DataFrame): Input DataFrame with potential outliers

    Returns:
        pd.DataFrame: DataFrame with outliers removed based on business logic
    """
    initial_count = len(df)

    print(f"Booking status distribution:")
    if "booking_status" in df.columns:
        status_counts = df["booking_status"].value_counts()
        for status, count in status_counts.items():
            print(f"  {status}: {count}")

    # Step 1: Remove records with negative values in numeric fields
    negative_outliers = 0
    numeric_positive_cols = ["booking_value", "ride_distance", "avg_vtat", "avg_ctat"]
    for col in numeric_positive_cols:
        if col in df.columns:
            before_count = len(df)
            df = df[(df[col].isna()) | (df[col] >= 0)]
            after_count = len(df)
            removed = before_count - after_count
            if removed > 0:
                print(f"Removed {removed} records with negative {col}")
                negative_outliers += removed

    # Step 2: Remove records with invalid ratings (should be between 0-5)
    rating_outliers = 0
    for rating_col in ["driver_ratings", "customer_rating"]:
        if rating_col in df.columns:
            before_count = len(df)
            df = df[
                (df[rating_col].isna())
                | ((df[rating_col] >= 0) & (df[rating_col] <= 5))
            ]
            after_count = len(df)
            removed = before_count - after_count
            if removed > 0:
                print(
                    f"Removed {removed} records with invalid {rating_col} (outside 0-5 range)"
                )
                rating_outliers += removed

    # Step 3: Apply business rules validation for each booking status
    business_rule_violations = 0

    if "booking_status" in df.columns:
        # Business rules for COMPLETED rides
        completed_violations = df[
            (df["booking_status"] == "completed")
            & (
                df["booking_value"].isna()
                | df["ride_distance"].isna()
                | df["reason_for_cancelling_by_customer"].notna()
                | df["driver_cancellation_reason"].notna()
                | df["incomplete_rides_reason"].notna()
            )
        ]
        if len(completed_violations) > 0:
            print(
                f"Removed {len(completed_violations)} completed rides violating business rules"
            )
            df = df.drop(completed_violations.index)
            business_rule_violations += len(completed_violations)

        # Business rules for CANCELLED BY CUSTOMER rides
        customer_cancel_violations = df[
            (df["booking_status"] == "cancelled_by_customer")
            & (
                df["reason_for_cancelling_by_customer"].isna()
                | df["driver_ratings"].notna()
                | df["customer_rating"].notna()
                | df["ride_distance"].notna()
                | df["driver_cancellation_reason"].notna()
                | df["incomplete_rides_reason"].notna()
            )
        ]
        if len(customer_cancel_violations) > 0:
            print(
                f"Removed {len(customer_cancel_violations)} customer cancelled rides violating business rules"
            )
            df = df.drop(customer_cancel_violations.index)
            business_rule_violations += len(customer_cancel_violations)

        # Business rules for CANCELLED BY DRIVER rides
        driver_cancel_violations = df[
            (df["booking_status"] == "cancelled_by_driver")
            & (
                df["driver_cancellation_reason"].isna()
                | df["driver_ratings"].notna()
                | df["customer_rating"].notna()
                | df["ride_distance"].notna()
                | df["reason_for_cancelling_by_customer"].notna()
                | df["incomplete_rides_reason"].notna()
            )
        ]
        if len(driver_cancel_violations) > 0:
            print(
                f"Removed {len(driver_cancel_violations)} driver cancelled rides violating business rules"
            )
            df = df.drop(driver_cancel_violations.index)
            business_rule_violations += len(driver_cancel_violations)

        # Business rules for INCOMPLETE rides
        incomplete_violations = df[
            (df["booking_status"] == "incomplete")
            & (
                df["incomplete_rides_reason"].isna()
                | df["booking_value"].isna()
                | df["driver_ratings"].notna()
                | df["customer_rating"].notna()
                | df["reason_for_cancelling_by_customer"].notna()
                | df["driver_cancellation_reason"].notna()
            )
        ]
        if len(incomplete_violations) > 0:
            print(
                f"Removed {len(incomplete_violations)} incomplete rides violating business rules"
            )
            df = df.drop(incomplete_violations.index)
            business_rule_violations += len(incomplete_violations)

        # Business rules for DRIVER NOT FOUND rides
        driver_not_found_violations = df[
            (df["booking_status"] == "driver_not_found")
            & (
                df["driver_ratings"].notna()
                | df["customer_rating"].notna()
                | df["reason_for_cancelling_by_customer"].notna()
                | df["driver_cancellation_reason"].notna()
                | df["incomplete_rides_reason"].notna()
            )
        ]
        if len(driver_not_found_violations) > 0:
            print(
                f"Removed {len(driver_not_found_violations)} driver not found rides violating business rules"
            )
            df = df.drop(driver_not_found_violations.index)
            business_rule_violations += len(driver_not_found_violations)

    # Step 4: Apply conservative IQR outlier detection only to completed rides
    completed_rides = (
        df[df["booking_status"] == "completed"]
        if "booking_status" in df.columns
        else df
    )
    iqr_outliers = 0

    if len(completed_rides) > 0:
        # Conservative outlier detection for booking_value (only extreme cases)
        if "booking_value" in completed_rides.columns:
            valid_values = completed_rides["booking_value"].dropna()
            if len(valid_values) > 0:
                Q1 = valid_values.quantile(0.05)
                Q3 = valid_values.quantile(0.95)
                extreme_outliers = df[
                    (df["booking_status"] == "completed")
                    & (df["booking_value"].notna())
                    & (
                        (df["booking_value"] < Q1 * 0.1)
                        | (df["booking_value"] > Q3 * 3)
                    )
                ]
                if len(extreme_outliers) > 0:
                    print(
                        f"Removed {len(extreme_outliers)} completed rides with extreme booking_value outliers"
                    )
                    df = df.drop(extreme_outliers.index)
                    iqr_outliers += len(extreme_outliers)

        # Conservative outlier detection for ride_distance (only extreme cases)
        if "ride_distance" in completed_rides.columns:
            valid_values = completed_rides["ride_distance"].dropna()
            if len(valid_values) > 0:
                Q3 = valid_values.quantile(0.95)
                extreme_outliers = df[
                    (df["booking_status"] == "completed")
                    & (df["ride_distance"].notna())
                    & (df["ride_distance"] > Q3 * 2)
                ]
                if len(extreme_outliers) > 0:
                    print(
                        f"Removed {len(extreme_outliers)} completed rides with extreme ride_distance outliers"
                    )
                    df = df.drop(extreme_outliers.index)
                    iqr_outliers += len(extreme_outliers)

    total_removed = initial_count - len(df)
    print(f"Total outliers removed: {total_removed}")
    print(f"  - Negative values: {negative_outliers}")
    print(f"  - Invalid ratings: {rating_outliers}")
    print(f"  - Business rule violations: {business_rule_violations}")
    print(f"  - IQR outliers (completed rides): {iqr_outliers}")

    return df


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply complete data transformation pipeline to the DataFrame.

    This function orchestrates all transformation steps including:
    - Column name normalization
    - Data type conversion and normalization
    - DateTime creation
    - Duplicate removal
    - Outlier detection and removal

    Args:
        df (pd.DataFrame): Raw input DataFrame

    Returns:
        pd.DataFrame: Fully transformed and cleaned DataFrame
    """
    print(f"\n[STEP 1/6] Raw Data Analysis - Shape: {df.shape}")
    print("=" * 50)
    print(df.head())
    print(df.info())

    print(f"\n[STEP 2/6] Normalizing Column Names to Snake Case")
    print("-" * 50)
    df = to_snake_case(df)
    print(f"Columns after normalization: {list(df.columns)}")

    print(f"\n[STEP 3/6] Converting Data Types and Normalizing Values")
    print("-" * 50)
    df = convert_types(df)
    print(f"Data types converted successfully. Shape: {df.shape}")
    print(df.info())

    print(f"\n[STEP 4/6] Creating DateTime Column from Date and Time")
    print("-" * 50)
    df = create_datetime(df)
    print(f"DateTime creation completed. Current shape: {df.shape}")

    print(f"\n[STEP 5/6] Removing Duplicate Records")
    print("-" * 50)
    initial_count = len(df)
    df = remove_duplicates(df)
    final_count = len(df)
    removed_count = initial_count - final_count
    print(f"Removed {removed_count} duplicate records. Final shape: {df.shape}")

    print(f"\n[STEP 6/6] Removing Outliers and Invalid Values")
    print("-" * 50)
    initial_count = len(df)
    df = remove_outliers(df)
    final_count = len(df)
    removed_count = initial_count - final_count
    print(f"Removed {removed_count} outlier records. Final shape: {df.shape}")
    print(f"Data transformation pipeline completed successfully!")
    print("=" * 50)
    return df
