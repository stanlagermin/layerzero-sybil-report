import pandas as pd
from tqdm import tqdm

# Load the small CSV file a.csv
a_df = pd.read_csv('sybil-wallet.csv')

# Extract the values from the first column of a_df
a_values = set(a_df.iloc[:, 0])

# Prepare a DataFrame to collect duplicates
duplicates = pd.DataFrame()

# Define the chunk size
chunk_size = 10**6  # Adjust the chunk size based on your memory capacity

# Get the total number of rows in the large CSV file
total_rows = sum(1 for _ in open('2024-05-15-snapshot1_transactions.csv')) - 1  # Adjusting for header row

# Process the large CSV file b.csv in chunks
for chunk in tqdm(pd.read_csv('2024-05-15-snapshot1_transactions.csv', chunksize=chunk_size), total=total_rows // chunk_size + 1):
    # Filter the chunk to find matching rows
    filtered_chunk = chunk[chunk.iloc[:, 6].isin(a_values)]
    
    # Append the filtered chunk to the duplicates DataFrame
    duplicates = pd.concat([duplicates, filtered_chunk])

# Save the duplicates to a new CSV file
duplicates.to_csv('duplicate-wallet.csv', index=False)