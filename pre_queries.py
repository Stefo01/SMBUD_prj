import pandas as pd
import kagglehub

# First, download in main the global_terrorism file
path = kagglehub.dataset_download("START-UMD/gtd")

# Save the file to your main repository
# Assuming 'path' is a directory containing the dataset files
import shutil
import os

# # Define the target file and destination
file_name = "globalterrorismdb_0718dist.csv"  # Replace with the exact filename if different
source_path = os.path.join(path, file_name)

current_dir = os.path.dirname(os.path.abspath(__file__))
destination_path = os.path.join(current_dir, file_name)  # Update with your repo path

# # Move the file
shutil.move(source_path, destination_path)

print(f"Dataset saved to {destination_path}")

# -----------------------------------------------------

# Secondly, download London Crime dataset

path = kagglehub.dataset_download("jboysen/london-crime")

print("Path to dataset files:", path)

# downloaded_dir = path  # Path returned by kagglehub.dataset_download
# print(f"Files in {downloaded_dir}:")
# for root, dirs, files in os.walk(downloaded_dir):
#     for name in files:
#         print(os.path.join(root, name))

print("\n\n")

file_name = "london_crime_by_lsoa.csv"  # Replace with the exact filename if different
source_path = os.path.join(path, file_name)
destination_path = os.path.join(current_dir + "/MongoDB/archive/", file_name)  # Update with your repo path


shutil.move(source_path, destination_path)

print(f"Dataset saved to {destination_path}")


# Load the CSV file
file_path = current_dir + '/MongoDB/archive/london_crime_by_lsoa.csv'  # Replace with the path to your CSV file
data = pd.read_csv(file_path)

# Filter rows where the year is 2012 or greater and without 0 value
filtered_data = data[data['value'] > 0]
filtered_data = filtered_data[filtered_data['year'] >= 2012]

# Save the filtered data back to a CSV
filtered_data.to_csv(current_dir + '/MongoDB/archive/filtered_file.csv', index=False)
print("Filtered CSV file saved as 'filtered_file.csv'")

