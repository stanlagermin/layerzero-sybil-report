import pandas as pd
from tqdm import tqdm

# Load the small CSV file
a_df = pd.read_csv('final-0.csv')

# Extract the values from the first column of a_df
a_values = set(a_df.iloc[:, 0])

# Prepare a set to collect values from initialList.csv
initial_list_values = set()

# Define the chunk size
chunk_size = 10**6  # Adjust the chunk size based on your memory capacity

# Get the total number of rows in the large CSV file
total_rows = sum(1 for _ in open('initialList.csv')) - 1  # Adjusting for header row

# Process the large CSV file in chunks to build the set of initial list values
for chunk in tqdm(pd.read_csv('initialList.csv', chunksize=chunk_size), total=total_rows // chunk_size + 1):
    # Update the set with values from the current chunk
    initial_list_values.update(chunk.iloc[:, 0])

# Prepare a DataFrame to collect non-duplicates from final-0.csv
non_duplicates = a_df[~a_df.iloc[:, 0].isin(initial_list_values)]

# Save the non-duplicates to a new CSV file
non_duplicates.to_csv('final-wallet-sybil-wallet.csv', index=False)