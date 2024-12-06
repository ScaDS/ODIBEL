import argparse
import glob
import os
import matplotlib.pyplot as plt
import pandas as pd

def plot_count_over_time(input_dir, output_dir, aggregate=None, plot_type="line"):
    """
    Plots data from CSV files in subfolders. Automatically detects 'time' and 'count' columns.

    Parameters:
    - input_dir: Path to the root folder containing subfolders with CSV files.
    - output_dir: Directory to save the output plots.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # iterate over subfolders
    for folder_name in os.listdir(input_dir):
        folder_path = os.path.join(input_dir, folder_name)

        if os.path.isdir(folder_path):
            # find all csv files in the subfolders
            csv_files = glob.glob(os.path.join(folder_path, "*.csv"))

            for file_path in csv_files:
                # loads csv data
                data = pd.read_csv(file_path)

                # detect 'time' and 'count'-Columns
                time_col = next((col for col in data.columns if "time" in col.lower()), None)
                count_col = next( (col for col in data.columns if ("count" in col.lower() or "changes" in col.lower()) ), None)

                if not time_col or not count_col:
                    print(f"Skipping file {file_path}: Unable to detect 'time' or 'count' columns.")
                    continue

                # convert 'time' column to datetime
                data[time_col] = pd.to_datetime(data[time_col])
                # remove rows with time from 1970
                data = data[data[time_col].dt.year != 1970]
                # Aggregate data if specified
                if aggregate:
                    data = data.set_index(time_col).resample(aggregate).sum().reset_index()

                # create plot
                plt.figure(figsize=(10, 6))
                if plot_type == "line":
                    plt.plot(data[time_col], data[count_col], marker='o', linestyle='-', label=f"{folder_name}: {count_col}")
                elif plot_type == "bar":
                    plt.bar(data[time_col], data[count_col], label=f"{folder_name}: {count_col}", width=50)
                else:
                    print(f"Unknown plot type '{plot_type}' for {file_path}. Defaulting to line plot.")
                    plt.plot(data[time_col], data[count_col], marker='o', linestyle='-', label=f"{folder_name}: {count_col}")

                plt.xlabel("Time")
                plt.ylabel("Count")
                plt.title(f"Plot for {folder_name}")
                plt.grid(True)
                plt.legend()

                # save the plot
                output_file = os.path.join(output_dir, f"{folder_name}.png")
                plt.savefig(output_file, bbox_inches='tight')
                plt.close()

                print(f"Plot saved for {file_path} as {output_file}")


if __name__ == "__main__":
    # Argument parser to handle input/output paths
    parser = argparse.ArgumentParser(description="Process and visualize CSV data for DBpedia triples.")
    parser.add_argument("--input", required=True,
                        help="Path to the directory containing the directories of the CSV files.")
    parser.add_argument("--output", required=True,
                        help="Path to the directory for saving output plots.")
    parser.add_argument("--aggregate", required=False, default="Y",
                        help="Aggregation interval (e.g.,'Y' for year, 'M' for month, 'W' for week).")
    parser.add_argument("--plot-type", required=False, choices=["line", "bar"], default="line",
                        help="Type of plot: 'line' or 'bar'.")

    args = parser.parse_args()

    # Pass the input and output paths, aggregation interval, and plot type to the main function
    plot_count_over_time(input_dir=args.input, output_dir=args.output,
                         aggregate=args.aggregate, plot_type=args.plot_type)