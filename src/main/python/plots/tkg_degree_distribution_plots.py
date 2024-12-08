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

def plot_yearly_instance_scatter(data, output_path, year_col, degree_col, title):
    """
    Creates a scatter plot with year on the x-axis and each instance/resource
    on the y-axis, visualized by in/out degrees.

    Parameters:
    - data: The DataFrame containing the data.
    - output_path: Path to save the output plot.
    - year_col: The column representing the year.
    - degree_col: The degree column (in_degree or out_degree).
    - title: Title for the plot.
    """
    # Generate a scatter plot
    plt.figure(figsize=(12, 8))
    plt.scatter(data[year_col], data[degree_col], alpha=0.7, color="blue", edgecolor="k", linewidth=0.5)
    plt.xlabel("Year")
    plt.ylabel("Degree (In/Out)")
    plt.title(title)
    plt.grid(True, linestyle="--", linewidth=0.5)

    # Save the scatter plot
    plt.tight_layout()
    plt.savefig(output_path, bbox_inches="tight")
    plt.close()

    print(f"Yearly instance scatter plot saved as {output_path}")


def plot_degree_csv(input_dir, output_dir, scale_type="log", remove_outliers=False):
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

        if os.path.isdir(folder_path) and "degree" in folder_name.lower():
            csv_files = glob.glob(os.path.join(folder_path, "*.csv"))

            for file_path in csv_files:
                data = pd.read_csv(file_path)

                # identify degree column and year column
                if "in_degree" in data.columns and "tail" in data.columns:
                    degree_col = "in_degree"
                    year_col = "year"
                elif "out_degree" in data.columns and "head" in data.columns:
                    degree_col = "out_degree"
                    year_col = "year"
                else:
                    print(f"Skipping file {file_path}: Columns do not match expected 'degree' format.")
                    continue

                # calculate frequency of degrees per year
                grouped_data = data.groupby([year_col, degree_col]).size().reset_index(name="frequency")
                # Remove outliers from the degree column if specified
                grouped_data = filter_outliers(grouped_data, degree_col)

                # normalize years for a color gradient
                years = grouped_data[year_col].unique()
                cmap = plt.cm.coolwarm  # use a blue-to-red color map

                # create scatter plot
                plt.figure(figsize=(12, 8))
                scatter = plt.scatter(
                    grouped_data[degree_col],
                    grouped_data["frequency"],
                    c=grouped_data[year_col],
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

                # yearly instance scatter plot
                instance_scatter_output = os.path.join(output_dir, f"{folder_name}_distribution.png")
                plot_yearly_instance_scatter(
                    data,
                    output_path=instance_scatter_output,
                    year_col=year_col,
                    degree_col=degree_col,
                    title=f"Scatter Plot of Instances vs Year ({degree_col})"
                )

if __name__ == "__main__":
    # Argument parser to handle input/output paths
    parser = argparse.ArgumentParser(description="Process and visualize CSV data for DBpedia triples.")
    parser.add_argument("--input", required=True,
                        help="Path to the directory containing the directories of the CSV files.")
    parser.add_argument("--output", required=True,
                        help="Path to the directory for saving output plots.")
    parser.add_argument("--scale-type", required=False, choices=["log", "symlog", "linear"], default="linear",
                        help="Scale Type for the Plots: 'log', 'lin' or 'symlog'.")
    parser.add_argument("--remove-outliers", required=False, choices=[True, False], default=False,
                        help="Removes Outliers based on IQR from the Plots: True or False.")

    args = parser.parse_args()

    # Pass the input and output paths, scale type to the main function
    plot_degree_csv(input_dir=args.input, output_dir=args.output,
                    scale_type=args.scale_type, remove_outliers=args.remove_outliers)
