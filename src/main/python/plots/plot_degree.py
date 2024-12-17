import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Adjusting mock data for compatibility with the requested axes
mock_data_heatmap_adjusted = pd.DataFrame({
    "startDate": pd.date_range(start="2000-01-01", end="2024-12-31", freq="6M"),
    "daysBetween": np.random.randint(1, 10000, size=50),
    "numberOfEventWithWindow": np.random.randint(10, 500, size=50)
})

print(mock_data_heatmap_adjusted)

# Creating bins for the log scale on 'daysBetween'
duration_bins = np.logspace(0.1, 4, num=20)  # Logarithmic bins from 1 to 10,000
mock_data_heatmap_adjusted["duration_bins"] = pd.cut(mock_data_heatmap_adjusted["daysBetween"], bins=duration_bins)

# Pivot the data for heatmap
heatmap_data_adjusted = mock_data_heatmap_adjusted.pivot_table(
    index="duration_bins",
    columns="startDate",
    values="numberOfEventWithWindow",
    aggfunc="sum",
    fill_value=0  # Fill missing values with 0
)

# Plotting the heatmap
plt.figure(figsize=(16, 10))
sns.heatmap(heatmap_data_adjusted, cmap="coolwarm", cbar=True, annot=False)
plt.title("Heatmap: Start Date vs Duration with Event Frequency", fontsize=16)
plt.xlabel("Start Date (2000-2024 in 6-month steps)", fontsize=12)
plt.ylabel("Duration (Log Scale)", fontsize=12)
plt.yticks(rotation=0)  # Keep y-axis labels horizontal
plt.tight_layout()
plt.show()