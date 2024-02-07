# Earthquake Data Processing 

This repository contains a PySpark script for loading earthquake data, performing transformations, and loading the processed data into a CSV file. 

## Prerequisites
- Python 3.x
- Apache Spark installed and configured
- PySpark library installed
- Earthquake data source (CSV format)

## Setup
1. Clone this repository to your local machine.
2. Ensure that Apache Spark is installed and configured properly.
3. Install the required Python libraries by running:

'pip install pyspark'

4. Obtain the earthquake data in CSV format and place it in the data directory.

## Usage
1. Open a terminal and navigate to the directory containing the PySpark script.
2. Run the PySpark script using the following command:

'spark-submit --master local[*] earthquake_processing.py'

3. The script will load the earthquake data, perform necessary transformations, and save the processed data into a CSV file.
4. Once the script finishes execution, you will find the processed data saved in the output directory.
