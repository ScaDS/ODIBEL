import pandas as pd
import matplotlib.pyplot as plt

# Create DataFrame
df = pd.read_csv("/home/marvin/workspace/data/dbpedia-tkg/stats_1217_1825/FULL/countEndTriplesOverTime/part-00000-1dd47cdb-f389-48b9-a8f3-5857197d6381-c000.csv")
df2 = pd.read_csv("/home/marvin/workspace/data/dbpedia-tkg/stats_1217_1825/FULL/countStartTriplesOverTime/part-00000-025be79f-9303-4040-8985-8e3505ced520-c000.csv")

# Convert end_time to datetime
df['end_time'] = pd.to_datetime(df['end_time'])
df2['start_time'] = pd.to_datetime(df2['start_time'])

# Generate a date range from 2000 to 2025
date_range = pd.date_range(start="2000-01-01", end="2025-12-31", freq="Y")
accumulated_ends = df.set_index('end_time').resample('Y').sum().reindex(date_range, fill_value=0)
accumulated_starts = df2.set_index('start_time').resample('Y').sum().reindex(date_range, fill_value=0)
# accumulated_ends['cumulative_count'] = accumulated_ends['count_end_triples'].cumsum()

# Plot the data
plt.figure(figsize=(12, 6))
plt.plot(accumulated_ends.index, accumulated_ends['count_end_triples'], label="Deleted Triples")
plt.plot(accumulated_starts.index, accumulated_starts['count_start_triples'], label="Added Triples")
plt.xlabel("Date")
plt.ylabel("Cumulative Count")
plt.title("Accumulated Event Ends (2000-2025)")
plt.legend()
plt.grid()
plt.show()
