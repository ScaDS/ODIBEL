import argparse
import glob
import os
import matplotlib.pyplot as plt
import pandas as pd

def filter_outliers(df, column):
    """
    Removes outliers from a DataFrame based on the IQR method for a specific column.

    Parameters:
    - df: The DataFrame.
    - column: The column to check for outliers.

    Returns:
    - A DataFrame with outliers removed.
    """
    q1 = df[column].quantile(0.25)  # 1st Quartile (25th percentile)
    q3 = df[column].quantile(0.75)  # 3rd Quartile (75th percentile)
    iqr = q3 - q1  # Interquartile Range
    upper_bound = q3 + 1.5 * iqr  # Upper fence

    return df[df[column] <= upper_bound]



def plot_degree_distribution(input_dir, output_dir, scale_type="log", remove_outliers=False):
    """
    Processes CSV files and generates scatter plots for degree distributions with frequency on the y-axis
    and degree on the x-axis, with years as colors.

    Parameters:
    - input_dir: Path to the root folder containing subfolders with CSV files.
    - output_dir: Directory to save the output scatter plots.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # iterate over subfolders
    for folder_name in os.listdir(input_dir):
        folder_path = os.path.join(input_dir, folder_name)

        if os.path.isdir(folder_path) and "degree_per_year" in folder_name.lower():
            csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
            for file_path in csv_files:
                # Read the data
                try:
                    data = pd.read_csv(file_path)
                except FileNotFoundError:
                    print(f"Error: File not found at {file_path}")
                    return
                # identify degree column and year column
                if "in_degree" in folder_name.lower():
                    degree_col = "in_degree"
                elif "out_degree" in folder_name.lower():
                    degree_col = "out_degree"
                else:
                    print(f"Skipping file {file_path}: folder_name.lower() do not match expected 'degree' format.")

                    print(data.columns)
                    print("in_degree" in data.columns)
                    continue
                # Extract relevant columns for boxplot
                years = data['year']
                # Create the plot
                plt.figure(figsize=(12, 8))
                # Create boxplots for each year and visualize them clearly
                data_list = []
                for _, row in data.iterrows():
                    data_list.append([row['min'], row['q25'], row['median'], row['q75'], row['max']])
                plt.boxplot(data_list, positions=years, widths=0.6, showfliers=False, patch_artist=True,
                            boxprops=dict(facecolor="lightblue", color="blue"),
                            medianprops=dict(color="red"))
                # Adjust x-axis labels to be more readable
                plt.xticks(ticks=years, labels=years, rotation=45, fontsize=10)
                # Use logarithmic scale for y-axis to handle extreme values
                plt.yscale("linear")
                # Add titles and labels
                plt.title(f"{degree_col} Distribution Boxplot Over Time", fontsize=14)
                plt.xlabel("Year", fontsize=12)
                plt.ylabel(f"{degree_col} (Log Scale)", fontsize=12)
                plt.grid(axis='y', linestyle="--", alpha=0.7)
                # Save the plot
                output_file = os.path.join(output_dir, f"{degree_col}_distribution_per_year.png")
                plt.tight_layout()
                plt.savefig(output_file)
                print(f"Boxplot saved at {output_file}")
                plt.close()


        if os.path.isdir(folder_path) and "degree_frequency" in folder_name.lower():
            csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
            for file_path in csv_files:
                try:
                    data = pd.read_csv(file_path)
                except FileNotFoundError:
                    print(f"Error: File not found at {file_path}")
                    return
                # identify degree column and year column
                if "in_degree" in data.columns:
                    degree_col = "in_degree"
                    year_col = "year"
                elif "out_degree" in data.columns:
                    degree_col = "out_degree"
                    year_col = "year"
                else:
                    print(f"Skipping file {file_path}: Columns do not match expected 'degree' format.")
                    continue
                # Remove outliers from the degree column if specified
                if remove_outliers:
                    data = filter_outliers(data, degree_col)
                # normalize years for a color gradient
                years = data[year_col].unique()
                cmap = plt.cm.coolwarm  # use a blue-to-red color map
                # create scatter plot
                plt.figure(figsize=(12, 8))
                scatter = plt.scatter(
                    data[degree_col],
                    data["frequency"],
                    c=data[year_col],
                    cmap=cmap,
                    alpha=0.7,
                    edgecolor="k",
                    linewidth=0.1,
                    s=20  # Adjust marker size
                )
                # handle scale types
                if scale_type == "log":
                    plt.xscale("log")
                    plt.yscale("log")
                elif scale_type == "symlog":
                    plt.xscale("symlog")
                    plt.yscale("symlog")
                elif scale_type == "linear":
                    plt.xscale("linear")
                    plt.yscale("linear")
                else:
                    print(f"Unknown scale type '{scale_type}', defaulting to log.")
                    plt.xscale("log")
                    plt.yscale("log")
                # label and grid
                plt.xlabel("Degree (Number of Connections)")
                plt.ylabel("Frequency (Count of Occurrences)")
                plt.title(f"Degree Distribution for {folder_name} ({scale_type.capitalize()} Scale)")
                plt.grid(True, linestyle="--", linewidth=0.5)
                # add color bar with year labels
                cbar = plt.colorbar(scatter, ax=plt.gca(), orientation="vertical")
                cbar.set_label("Year")
                cbar.set_ticks(years)
                cbar.ax.set_yticklabels([str(year) for year in years])
                # save the plot
                output_file = os.path.join(output_dir, f"{folder_name}_count_on_{scale_type}_scala.png")
                plt.tight_layout()
                plt.savefig(output_file, bbox_inches='tight')
                plt.close()
                print(f"Degree distribution plot saved for {file_path} as {output_file}")


if __name__ == "__main__":
    # Argument parser to handle input/output paths
    parser = argparse.ArgumentParser(description="Process and visualize CSV data for DBpedia triples.")
    parser.add_argument("--input", required=True,
                        help="Path to the directory containing the directories of the CSV files.")
    parser.add_argument("--output", required=True,
                        help="Path to the directory for saving output plots.")
    parser.add_argument("--scale-type", required=False, choices=["log", "symlog", "linear"], default="log",
                        help="Scale Type for the Plots: 'log', 'lin' or 'symlog'.")
    parser.add_argument("--remove-outliers", required=False, choices=[True, False], default=True,
                        help="Removes Outliers based on IQR from the Plots: True or False.")

    args = parser.parse_args()

    # Pass the input and output paths, scale type to the main function
    plot_degree_distribution(input_dir=args.input, output_dir=args.output,
                             scale_type=args.scale_type, remove_outliers=args.remove_outliers)
