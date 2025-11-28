import pandas as pd
import matplotlib.pyplot as plt
import os
import glob
import argparse

def create_boxplots(input_dir, output_dir):
    folder_path = os.path.join(input_dir, "calculate_temporal_activity_span_over_time")

    # Find all CSV files in the subfolders
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))

    for input_file in csv_files:

        # Read the data
        try:
            data = pd.read_csv(input_file)
        except FileNotFoundError:
            print(f"Error: File not found at {input_file}")
            return

        # Extract relevant columns for boxplot
        years = data['year']

        # Create the plot
        plt.figure(figsize=(12, 8))

        # Create boxplots for each year and visualize them clearly
        data_list = []
        for _, row in data.iterrows():
            data_list.append([row['min'], row['q25'], row['median'], row['q75'], row['max']])

        plt.boxplot(data_list, positions=years, widths=0.6, showfliers=True, patch_artist=True,
                    boxprops=dict(facecolor="lightblue", color="blue"),
                    medianprops=dict(color="red"))

        # Adjust x-axis labels to be more readable
        plt.xticks(ticks=years, labels=years, rotation=45, fontsize=10)

        # Use logarithmic scale for y-axis to handle extreme values
        plt.yscale("log")

        # Add titles and labels
        plt.title("Boxplot Over Time", fontsize=14)
        plt.xlabel("Year", fontsize=12)
        plt.ylabel("Values (Log Scale)", fontsize=12)
        plt.grid(axis='y', linestyle="--", alpha=0.7)

        # Save the plot
        output_file = os.path.join(output_dir, f"activity_span_boxplots_over_time.png")
        plt.tight_layout()
        plt.savefig(output_file)
        print(f"Boxplot saved at {output_file}")
        plt.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process and visualize CSV data for DBpedia triples.")
    parser.add_argument("--input", required=True,
                        help="Path to the directory containing the directory of the CSV file.")
    parser.add_argument("--output", required=True,
                        help="Path to the directory for saving output plots.")

    args = parser.parse_args()

    # Pass the input and output paths to the main function
    create_boxplots(input_dir=args.input, output_dir=args.output)
