import pandas as pd

df = []
df.append(pd.read_csv("memory_usage_ray1.csv", names=["Timestamp", "Memory Usage"]))
df.append(pd.read_csv("memory_usage_ray2.csv", names=["Timestamp", "Memory Usage"]))
df.append(pd.read_csv("memory_usage_ray3.csv", names=["Timestamp", "Memory Usage"]))
df.append(pd.read_csv("memory_usage_ray4.csv", names=["Timestamp", "Memory Usage"]))

cumulative_df = pd.concat([df[0], df[1], df[2], df[3]], ignore_index=True)
cumulative_df.sort_values(by="Timestamp", inplace=True)
cumulative_df.to_csv("cumulative_memory_usage.csv", index=False)

# df = pd.read_csv("cumulative_memory_usage.csv")
# df['Memory Usage'] = pd.to_numeric(df['Memory Usage'], errors='coerce')
# max_row_index = cumulative_df["Memory Usage"].idxmax()
# print("Row index with maximum memory usage:", max_row_index)


df = pd.read_csv("cumulative_memory_usage.csv")
df['Memory Usage'] = pd.to_numeric(df['Memory Usage'], errors='coerce')
max_memory_index = df['Memory Usage'].idxmax()
max_row = df.loc[max_memory_index]

print("Row index with maximum memory usage:", max_memory_index)
print("Row with maximum memory usage:\n", max_row)