# -*- coding: utf-8 -*-
"""
Created on Thu Aug 15 16:21:04 2024

@author: mameiste
"""

import os
import pandas as pd
from datetime import datetime, timedelta

# Define the parent folder path
parent_folder_path = r"\\smb.isibhv.dmawi.de\projects\p_OZA_BaleenFRAM\Bowhead\TestData_all_recorder\Selection_Tables_clear_signals"

# Loop through each subfolder in the parent directory
for subfolder_name in os.listdir(parent_folder_path):
    subfolder_path = os.path.join(parent_folder_path, subfolder_name)
    
    if os.path.isdir(subfolder_path):  # Check if it is a directory
        # Determine the time to add based on the subfolder name
        if subfolder_name == 'ARKF04-19_SV1088':
            seconds_to_add = 598
        else:
            seconds_to_add = 600

        # Initialize an empty list to store data from each file
        all_filenames = []

        # Iterate over all txt files in the subfolder
        for file_name in os.listdir(subfolder_path):
            if file_name.endswith('.txt'):
                file_path = os.path.join(subfolder_path, file_name)
                # Read the file into a DataFrame
                df = pd.read_csv(file_path, delimiter='\t')  # Adjust delimiter if necessary
                # Append the "File" column to the list
                all_filenames.extend(df['File'].tolist())

        # Create a DataFrame from the filenames
        df_combined = pd.DataFrame(all_filenames, columns=['Filename'])

        # Create the Selection column
        df_combined['Selection'] = range(1, len(df_combined) + 1)

        # Extract Begin Date and Begin Time Clock Time
        df_combined['Begin Date'] = df_combined['Filename'].apply(lambda x: datetime.strptime(x.split('_')[0], '%Y%m%d-%H%M%S').date())
        df_combined['Begin Clock Time'] = df_combined['Filename'].apply(lambda x: datetime.strptime(x.split('_')[0], '%Y%m%d-%H%M%S').time())

        # Calculate End Clock Time (Begin Time + seconds_to_add)
        df_combined['End Clock Time'] = df_combined.apply(lambda row: (datetime.combine(row['Begin Date'], row['Begin Clock Time']) + timedelta(seconds=seconds_to_add)).time(), axis=1)

        # Determine End Date (same as Begin Date, or next day if End Clock Time is earlier than Begin Time)
        df_combined['End Date'] = df_combined.apply(lambda row: row['Begin Date'] if row['End Clock Time'] > row['Begin Clock Time'] else row['Begin Date'] + timedelta(days=1), axis=1)

        # Define the output CSV file path
        output_csv_path = os.path.join(parent_folder_path, f'{subfolder_name}_TestData_Logs.csv')

        # Save the DataFrame to a CSV file
        df_combined.to_csv(output_csv_path, index=False)
