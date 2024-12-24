import pandas as pd

# Load the CSV file
file_path = 'london_crime_by_lsoa.csv'  # Replace with the path to your CSV file
data = pd.read_csv(file_path)

# Filter rows where the year is 2012 or greater
filtered_data = data[data['year'] >= 2012]

# Save the filtered data back to a CSV
filtered_data.to_csv('filtered_file.csv', index=False)
print("Filtered CSV file saved as 'filtered_file.csv'")


# %%

import pandas as pd

file_path = 'filtered_file.csv'  # Replace with the path to your CSV file
data = pd.read_csv(file_path)

filtered_data = data[data['value'] > 0]

print(len(filtered_data))

# 7494780 quello filtrato
# 3419099 totale senza zeri
# 1886686 filtrato senza zeri


# %%
