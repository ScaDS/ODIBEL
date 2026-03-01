import pandas as pd
import matplotlib.pyplot as plt
import sys

df_revs = pd.read_csv(sys.argv[1])
df_pages = pd.read_csv(sys.argv[2])
output_file=sys.argv[3]

# Prepare data
all_revs = df_revs.groupby('year')['count'].sum().reset_index()
all_pages = df_pages.groupby('year')['count'].sum().reset_index()
# Separate namespace 0 and namespace 14
ns_0 = df_revs[df_revs['ns'] == 0].groupby('year')['count'].sum().reset_index()
ns_14 = df_revs[df_revs['ns'] == 14].groupby('year')['count'].sum().reset_index()

# Plot the data
plt.figure(figsize=(10, 6))

# Plot all pages
plt.plot(all_pages['year'], all_pages['count'], marker='o', label='All Pages (All NS)', linestyle='-')

# Plot all namespace
plt.plot(all_revs['year'], all_revs['count'], marker='o', label='All Revisions (All NS)', linestyle='-')

# Plot namespace 0
plt.plot(ns_0['year'], ns_0['count'], marker='s', label='Article Revisions', linestyle='--')

# Plot namespace 14
plt.plot(ns_14['year'], ns_14['count'], marker='^', label='Category Revisions', linestyle='-.')

plt.yscale("log")
# Add labels and legend
plt.xlabel('Year')
plt.ylabel('Edits Count')
plt.title('Edits per Year for All, Articles and Category Pages')
plt.legend()
plt.grid(True)
plt.tight_layout()

# Display plot
plt.savefig(output_file,)