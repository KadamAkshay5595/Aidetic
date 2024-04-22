from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, to_timestamp, udf
from pyspark.sql.types import DoubleType
from math import radians, sin, cos, sqrt, atan2
import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import folium

### Creating Spark Session for our Task

spark = SparkSession.builder \
    .appName("Earthquake-Data-Ananlysis-Task") \
    .getOrCreate()

### Task 1: Loading the dataset into a PySpark DataFrame
df = spark.read.csv("file:///home/akshay/Downloads/database.csv", header=True, inferSchema=True)

### Validating Data and Schema of Data:

df.show(2)
df.printSchema()

### Task 2: Convert the Date and Time columns into a timestamp column named Timestamp
df = df.withColumn("Timestamp", to_timestamp(concat(col("Date"), lit(" "), col("Time")), "MM/dd/yyyy HH:mm:ss"))

### Task 3: Filter the dataset to include only earthquakes with a magnitude greater than 5.0
df_filtered = df.filter(col("Magnitude") > 5.0)

### Task 4: Calculate the average depth and magnitude of earthquakes for each earthquake type

df_filtered.groupby("Type").avg("Depth", "Magnitude").show()
avg_depth_magnitude = df_filtered.groupby("Type").avg("Depth", "Magnitude")

### Task 5: Implement a UDF to categorize the earthquakes into levels based on their magnitudes

def cat_earthquakes(magnitude):

    if magnitude < 5.0:
        return "Low"
    elif magnitude < 7.0:
        return "Moderate"
    else:
        return "High"

categorize_udf = udf(cat_earthquakes)

df_filtered = df_filtered.withColumn("Magnitude_Level", categorize_udf(col("Magnitude")))

### Task 6: Calculate the distance of each earthquake from a reference location

def calculate_distance(lat1, long1):

    lat2, long2 = 0, 0  ## reference location(0, 0)
    R = 6371.0  # radius of earth

    lat1_rad = radians(lat1)
    long1_rad = radians(long1)
    lat2_rad = radians(lat2)
    long2_rad = radians(long2)

    dlong = long2_rad - long1_rad
    dlat = lat2_rad - lat1_rad

    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlong / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance

calculate_distance_udf = udf(calculate_distance, DoubleType())

df_filtered = df_filtered.withColumn("Dist_From_Ref", calculate_distance_udf(col("Latitude"), col("Longitude")))
df_filtered.show(3)

### Task 8: Save the final DataFrame to a CSV file
df_filtered.write.csv("file:///home/akshay/OUTPUT/filtered_dataset.csv", header=True)

### Task 7 : Data Visualisation

# Convert Spark DataFrame to Pandas DataFrame

df_pd = pd.DataFrame(df_filtered.collect(), columns=df_filtered.columns)

# Plot using Basemap
plt.figure(figsize=(12, 6))
m = Basemap(projection='mill',llcrnrlat=-90,urcrnrlat=90,llcrnrlon=-180,urcrnrlon=180,resolution='c')
m.drawcoastlines()
m.drawcountries()
m.drawmapboundary(fill_color='lightblue')

for index, row in df_pd.iterrows():
    x, y = m(row['Longitude'], row['Latitude'])
    m.plot(x, y, 'ro', markersize=row['Magnitude'], alpha=0.6)

plt.title("Earthquake Distribution (Basemap)")
plt.show()

# Plot using Folium
m = folium.Map(location=[0, 0], zoom_start=2)

for index, row in df_pd.iterrows():
    folium.CircleMarker(
        location=[row['Latitude'], row['Longitude']],
        radius=row['Magnitude']*2,
        color='red',
        fill_color='red'
    ).add_to(m)

m.save('OUTPUT/earthquake_map.html')

### Stop the Spark session
spark.stop()

