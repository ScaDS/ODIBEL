import argparse
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, count, when, lag
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import pandas as pd

# Function to read JSON files from a directory using PySpark
def load_json_files(input_path):
    spark = SparkSession.builder.appName("JSONLoader").getOrCreate()
    # Load all JSON files in the given directory
    data = spark.read.json(input_path)
    data.show(5)
    return data

# Function to process data for plotting
def process_data(data):
    # Convert timestamps to readable format
    data = data.withColumn("start_time", from_unixtime(col("tFrom") / 1000))
    data = data.withColumn("end_time", from_unixtime(col("tUntil") / 1000))

    # Count revisions over time
    revisions_over_time = data.groupBy("start_time").count().orderBy("start_time")

    # Count changes over time by calculating distinct changes in "tail"
    window_spec = Window.orderBy("start_time")
    data = data.withColumn("prev_tail", lag("tail").over(window_spec))
    data = data.withColumn("is_change", when(col("tail") != col("prev_tail"), 1).otherwise(0))
    changes_over_time = data.groupBy("start_time").agg(count("is_change").alias("changes"))

    return revisions_over_time.toPandas(), changes_over_time.toPandas()

# Function to plot data
def plot_data(revisions, changes, output_path):
    # Plot revisions over time
    revisions["start_time"] = pd.to_datetime(revisions["start_time"])
    revisions = revisions.sort_values("start_time")
    plt.figure(figsize=(10, 6))
    plt.plot(revisions["start_time"], revisions["count"], marker="o", label="Revisions")
    plt.title("Revisions Over Time")
    plt.xlabel("Time")
    plt.ylabel("Number of Revisions")
    plt.legend()
    plt.grid()
    plt.savefig(os.path.join(output_path, "revisions_over_time.png"))
    plt.close()

    # Plot changes over time
    changes["start_time"] = pd.to_datetime(changes["start_time"])
    changes = changes.sort_values("start_time")
    plt.figure(figsize=(10, 6))
    plt.plot(changes["start_time"], changes["changes"], marker="x", label="Changes")
    plt.title("Changes Over Time")
    plt.xlabel("Time")
    plt.ylabel("Number of Changes")
    plt.legend()
    plt.grid()
    plt.savefig(os.path.join(output_path, "changes_over_time.png"))
    plt.close()

# Main function to orchestrate the process
def main(input_path, output_path):
    # Load JSON files
    data = load_json_files(input_path)

    # Process data
    revisions, changes = process_data(data)

    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)

    # Plot and save data
    plot_data(revisions, changes, output_path)

if __name__ == "__main__":
    # Argument parser to handle input/output paths
    parser = argparse.ArgumentParser(description="Process and visualize JSON data for DBpedia triples.")
    parser.add_argument("--input", required=True, help="Path to the directory containing JSON files.")
    parser.add_argument("--output", required=True, help="Path to the directory for saving output plots.")

    args = parser.parse_args()

    # Pass the input and output paths to the main function
    main(args.input, args.output)
