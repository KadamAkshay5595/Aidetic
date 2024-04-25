import os
import tempfile
import pytest
from pyspark.sql import SparkSession
from test2 import load_dataset, convert_to_timestamp, filter_earthquakes_by_magnitude, categorize_earthquakes, calculate_distance, save_to_csv, visualize_data,avg_calculator

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("Earthquake-Data-Analysis-Test") \
        .getOrCreate()

def test_load_dataset(spark):
    df = load_dataset()
    assert df.count() > 0

def test_convert_to_timestamp(spark):
    df = load_dataset()
    df = convert_to_timestamp(df)
    assert "Timestamp" in df.columns

def test_filter_earthquakes_by_magnitude(spark):
    df = load_dataset()
    df_filtered = filter_earthquakes_by_magnitude(df)
    assert df_filtered.filter(df_filtered["Magnitude"] <= 5.0).count() == 0

def test_cat_earthquakes(spark):
    assert categorize_earthquakes(4.0) == "Low"
    assert categorize_earthquakes(6.0) == "Moderate"
    assert categorize_earthquakes(8.0) == "High"

def test_calculate_distance(spark):
    assert calculate_distance(-20.579,-173.972 ) == 17634.749687902233
    

def test_visualize_data(spark):
    # Create a mock DataFrame for testing
    mock_data = [
        {"Latitude": 0, "Longitude": 0, "Magnitude": 5},
        {"Latitude": 10, "Longitude": 10, "Magnitude": 6},
        {"Latitude": -10, "Longitude": -10, "Magnitude": 7}
    ]
    mock_df = spark.createDataFrame(mock_data)

    # Test that the function does not raise any exceptions
    try:
        visualize_data(mock_df)
    except Exception as e:
        pytest.fail(f"visualize_data raised an exception: {e}")

    # You can add more specific assertions if needed
    assert True  # Add your assertions here

def test_save_to_csv(spark):
    # Create a mock DataFrame for testing
    mock_data = [
        {"Latitude": 0, "Longitude": 0, "Magnitude": 5},
        {"Latitude": 10, "Longitude": 10, "Magnitude": 6},
        {"Latitude": -10, "Longitude": -10, "Magnitude": 7}
    ]
    mock_df = spark.createDataFrame(mock_data)

    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Save DataFrame to CSV in the temporary directory
        output_path = os.path.join(temp_dir, "output.csv")
        save_to_csv(mock_df, output_path)

        # Check if CSV file was created
        assert os.path.exists(output_path)

        # Check if the temporary directory contains the file
        temp_dir_contents = os.listdir(temp_dir)
        assert "output.csv" in temp_dir_contents

        # Check if CSV file is not empty
        assert os.path.getsize(output_path) > 0


def test_avg_calculations(spark):
 
    df = load_dataset()

    # Calculate average depth and magnitude
    result_df = avg_calculator(df)

    # Convert result to Python dictionary for easy comparison
    result_dict = {row["Type"]: (row["avg(Depth)"], row["avg(Magnitude)"]) for row in result_df.collect()}

    # Check the calculated averages
    assert result_dict["Explosion"] == (0.0, 5.85)
    assert result_dict["Rock Burst"] == (1.0, 6.2)
    assert result_dict["Nuclear Explosion"] == (0.3, 5.850685714285718)
    assert result_dict["Earthquake"] == (71.31391348140497, 5.882762568870756)

