import matplotlib.pyplot as plt
import pandas as pd
import sys

output_file=sys.argv[2]
# Example data: Window durations in hours
# data = {'duration_hours': [1, 5, 10, 24, 50, 100, 200, 1000, 5000, 10000, 70000]}
df = pd.read_csv(sys.argv[1])

# Define custom hourly bins
bins = [0, 1, 12, 24, 168, 5040, 60480, float('inf')]
labels = ["0-1 hours", "1-12 hours", "12-24 hours", "1-7 days", "7-30 days", "11-12 months", "Over (1 Year)"]

# Bin the data
df['binned'] = pd.cut(df['duration_hours'], bins=bins, labels=labels, right=False)

# Aggregate counts per bin
bin_counts = df['binned'].value_counts().sort_index()

# Plot the binned data
plt.figure(figsize=(10, 6))
plt.bar(bin_counts.index, bin_counts.values, edgecolor='k')
plt.yscale('log')  # Logarithmic y-axis for better visibility
plt.xlabel("Duration Ranges (Hours)")
plt.ylabel("Frequency (Log Scale)")
plt.title("Distribution of Window Durations in Hours")
plt.xticks(rotation=45)
# plt.grid(True, which="both", linestyle="--", alpha=0.5)

plt.tight_layout()
plt.savefig(output_file)
