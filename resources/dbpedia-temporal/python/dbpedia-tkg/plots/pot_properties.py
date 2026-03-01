import pandas as pd
import matplotlib.pyplot as plt

# Simulate a large dataset
import random
random.seed(42)

# data = {
#     "rel": [f"rel_{i}" for i in range(1, 101)],
#     "avg_changes": [random.uniform(0, 10) for _ in range(100)],
#     "min_changes": [random.randint(0, 3) for _ in range(100)],
#     "max_changes": [random.randint(5, 15) for _ in range(100)],
#     "25th_percentile": [random.uniform(0, 5) for _ in range(100)],
#     "median": [random.uniform(2, 8) for _ in range(100)],
#     "75th_percentile": [random.uniform(5, 10) for _ in range(100)],
#     "total_heads": [random.randint(1, 50) for _ in range(100)],
# }

# Create a DataFrame
df = pd.read_csv("/home/marvin/workspace/data/dbpedia-tkg/eval/stats/propertyChangesAll/part-00000-5ef3cc5c-8f46-4ff9-8de3-12ead3b28bb5-c000.csv")
df["rel"] = df["rel"].str.replace("http://dbpedia.org/ontology/", "dbo:", regex=False)
df["rel"] = df["rel"].str.replace("http://purl.org/dc/terms/", "purl:", regex=False)
df["rel"] = df["rel"].str.replace("http://xmlns.com/foaf/0.1/","foaf:", regex=False)
df["rel"] = df["rel"].str.replace("http://www.w3.org/1999/02/22-rdf-syntax-ns#","rdf:", regex=False)
# Sort relations by total_heads for clarity
# df_sorted = df.sort_values(by="max_changes", ascending=False)


df_sorted = df.sort_values(by="median", ascending=False)


# Select top 10 relations
df = df_sorted.head(20)

# Plot charts
fig, axes = plt.subplots(2, 2, figsize=(15, 10))

# Average Changes
df.plot(x="rel", y="avg_changes", kind="bar", ax=axes[0, 0], legend=False)
axes[0, 0].set_title("Top Relations: Average Changes")
axes[0, 0].set_ylabel("Average Changes")
axes[0, 0].tick_params(axis='x', rotation=45)
for label in axes[0, 0].get_xticklabels():
    label.set_ha("right")  # Align labels to the start

# Min and Max Changes
df.plot(x="rel", y=["min_changes", "max_changes"], kind="bar", ax=axes[0, 1])
axes[0, 1].set_title("Top Relations: Min and Max Changes")
axes[0, 1].set_ylabel("Changes")
axes[0, 1].tick_params(axis='x', rotation=45)
for label in axes[0, 1].get_xticklabels():
    label.set_ha("right")

# Percentiles
df.plot(
    x="rel",
    y=["25th_percentile", "median", "75th_percentile"],
    kind="bar",
    ax=axes[1, 0],
)
axes[1, 0].set_title("Top Relations: Percentiles")
axes[1, 0].set_ylabel("Values")
axes[1, 0].tick_params(axis='x', rotation=45)
for label in axes[1, 0].get_xticklabels():
    label.set_ha("right")

# Total Heads
# df.plot(x="rel", y="total_heads", kind="bar", ax=axes[1, 1], color="orange", legend=False)
# axes[1, 1].set_title("Top Relations: Total Heads")
# axes[1, 1].set_ylabel("Total Heads")
# axes[1, 1].tick_params(axis='x', rotation=45)
# for label in axes[1, 1].get_xticklabels():
#     label.set_ha("right")

# Adjust layout
plt.tight_layout()
plt.savefig("properties_plot.png")
