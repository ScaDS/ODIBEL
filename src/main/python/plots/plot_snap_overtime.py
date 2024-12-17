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

# Sort the dataframe by date
df = df.sort_values(by='date')

# Plot all columns except 'date' over time
plt.figure(figsize=(12, 8))
for column in ['triples', 'distTriples', 'distEntities', 'distTypes', 'disRelations', 'distVersions']:
    plt.plot(df['date'], df[column], label=column)

# Customize plot
plt.xlabel('Date')
plt.ylabel('Values')
plt.title('Metrics over Time')
plt.legend()
plt.grid()
plt.tight_layout()

# Show the plot
plt.show()
