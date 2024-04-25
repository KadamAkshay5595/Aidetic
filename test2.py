from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, to_timestamp, udf
from pyspark.sql.types import DoubleType
from math import radians, sin, cos, sqrt, atan2
import pandas as pd
import folium

def load_dataset():
    spark = SparkSession.builder \
        .appName("Earthquake-Data-Ananlysis-Task") \
        .getOrCreate()
    
    df = spark.read.csv("file:///home/akshay/Downloads/database.csv", header=True, inferSchema=True)
    df.show(2)
    return df

def convert_to_timestamp(df):
    df = df.withColumn("Timestamp", to_timestamp(concat(col("Date"), lit(" "), col("Time")), "MM/dd/yyyy HH:mm:ss"))
    df.printSchema()
    return df

def filter_earthquakes_by_magnitude(df):
    df_filtered = df.filter(col("Magnitude") > 5.0)
    return df_filtered

def categorize_earthquakes(magnitude):
    if magnitude < 5.0:
        return "Low"
    elif magnitude < 7.0:
        return "Moderate"
    else:
        return "High"

categorize_udf = udf(categorize_earthquakes)

def avg_calculator(df):

    avg_depth_magnitude = df.groupby("Type").avg("Depth", "Magnitude")
    return avg_depth_magnitude

def calculate_distance(lat, long):
    lat2, long2 = 0, 0  ## reference location(0, 0)
    R = 6371.0  # radius of earth

    lat1_rad = radians(lat)
    long1_rad = radians(long)
    lat2_rad = radians(lat2)
    long2_rad = radians(long2)

    dlong = long2_rad - long1_rad
    dlat = lat2_rad - lat1_rad

    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlong / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance

calculate_distance_udf = udf(calculate_distance, DoubleType())

def visualize_data(df):
    df_pd = pd.DataFrame(df.collect(), columns=df.columns)
    m = folium.Map(location=[0, 0], zoom_start=2)

    for index, row in df_pd.iterrows():
        folium.CircleMarker(
            location=[row['Latitude'], row['Longitude']],
            radius=int(row['Magnitude'])*2,
            color='red',
            fill_color='red'
        ).add_to(m)

    m.save('OUTPUT/earthquake_map.html')

def save_to_csv(df, output_path):
    df.write.csv(output_path, header=True)

def main():
    df = load_dataset()
    df = convert_to_timestamp(df)
    df_filtered = filter_earthquakes_by_magnitude(df)
    df_filtered = df_filtered.withColumn("Magnitude_Level", categorize_udf(col("Magnitude")))
    df_filtered.select(col('Magnitude'),col('Magnitude_Level')).show(4)
    df_filtered = df_filtered.withColumn("Dist_From_Ref", calculate_distance_udf(col("Latitude"), col("Longitude")))
    df_filtered.select(col('Latitude'),col('Longitude'),col('Dist_From_Ref')).show(5)
    save_to_csv(df_filtered, "file:///home/akshay/OUTPUT/filtered_dataset.csv")
    visualize_data(df_filtered)
    avg_df=avg_calculator(df_filtered)
    avg_df.show()

if __name__ == "__main__":
    main()
