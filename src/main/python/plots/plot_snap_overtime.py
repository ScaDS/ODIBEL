import pandas as pd
import os
import matplotlib.pyplot as plt

# Directory where the folders are stored
base_dir = '/home/marvin/workspace/data/dbpedia-tkg/stats_1217_1825/FULL/SNAP'

# List to store dataframes
dataframes = []

# Traverse each folder in the base directory
for folder in os.listdir(base_dir):
    folder_path = os.path.join(base_dir, folder)
    
    # Check if it's a directory
    if os.path.isdir(folder_path):
        summary_path = os.path.join(folder_path, 'summary')
        
        # Locate CSV file in the summary subdirectory
        for file in os.listdir(summary_path):
            if file.endswith('.csv'):
                csv_path = os.path.join(summary_path, file)
                
                # Load CSV into a dataframe
                df = pd.read_csv(csv_path)
                
                # Add a new column with the folder name (date)
                df['date'] = folder
                
                # Append dataframe to the list
                dataframes.append(df)

# Concatenate all dataframes into a single dataframe
final_df = pd.concat(dataframes, ignore_index=True)

# Display the resulting dataframe

# Convert to DataFrame and ensure 'date' is datetime
df = final_df
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(by='date')

df2 = pd.read_csv("/home/marvin/workspace/data/dbpedia-tkg/stats_1217_1825/FULL/yearlyTripleDiffStats/part-00000-f1c15373-69e3-4607-9a39-fba4e6e75b58-c000.csv")

df2['prevYear'] = pd.to_datetime(df2['prevYear'].astype(str) + '-06-01')
df2['currYear'] = pd.to_datetime(df2['currYear'].astype(str) + '-06-01')

# Plot with separate y-axis for df2 data
fig, ax1 = plt.subplots(figsize=(12, 8))

# Primary y-axis for df1
for column in ['triples', 'distTriples', 'distEntities', 'distTypes', 'disRelations', 'distVersions']:
    ax1.plot(df['date'], df[column], label=column)

ax1.set_xlabel('Date')
ax1.set_yscale('log')
ax1.set_ylabel('Metrics (df1)', color='blue')
ax1.tick_params(axis='y', labelcolor='blue')

# Secondary y-axis for df2
ax2 = ax1.twinx()
for column in ['prevSize', 'currSize', 'addCount', 'delCount']:
    ax2.plot(df2['prevYear'], df2[column], '--', label=f"df2_{column}")

ax2.set_ylabel('Metrics (df2)', color='orange')
ax2.tick_params(axis='y', labelcolor='orange')

# Combine legends from both axes
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

# Title and grid
plt.title('Metrics Over Time with Separate Scales for df1 and df2')
plt.grid()
plt.tight_layout()

# Show plot
plt.savefig("snapshot_evolution.png")
# df2['prevYear'] = pd.to_datetime(df2['prevYear'].astype(str) + '-06-01')
# df2['currYear'] = pd.to_datetime(df2['currYear'].astype(str) + '-06-01')


# # Sort the dataframe by date
# df = df.sort_values(by='date')

# # Plot all columns except 'date' over time
# plt.figure(figsize=(12, 8))
# columns =  ['triples', 'distTriples', 'distEntities', 'distTypes', 'disRelations', 'distVersions']
# columnLabels = ['Triples', 'Unique Triples', 'Unique Entities', 'Unique Types', 'Unique Relations', 'Unique Versions']
# for column in columns:
#     plt.plot(df['date'], df[column], label=[columnLabels[columns.index(column)]])

# # Plot columns from df2
# for column in ['prevSize', 'currSize', 'addCount', 'delCount']:
#     plt.plot(df2['prevYear'], df2[column], '--', label=f"df2_{column}")

# # Customize plot
# plt.xlabel('Date')
# plt.ylabel('Number of')
# plt.title('Metrics over Time')
# plt.legend()
# plt.grid()
# plt.tight_layout()

# # Show the plot
# plt.show()
