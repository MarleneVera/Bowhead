# -*- coding: utf-8 -*-
"""
Created on Tue Sep  3 14:48:20 2024

@author: mameiste
"""

import os
import librosa
import soundfile as sf
import paramiko
from tqdm import tqdm

# SMB source directory
source_dir = r"\\smb.isibhv.dmawi.de\projects\p_OZA_BaleenFRAM\_tmp_ArcticPAMData_original\ARKF04-15_SV1026"
# Local temporary directory to store downloaded files
local_temp_dir = r"C:\temp\smb_download"
# SCP (SSH) connection details
scp_server = 'levante.dkrz.de'
scp_port = 22
scp_username = 'a270248'
scp_password = 'Bowheadwhale&22'
scp_remote_base_path = '/work/ka1176/marlene/bowhead_whale_detection/data/audio/ARKF04-15_SV1026/'
# Target sample rate for downsampling
target_sample_rate = 5000

def download_and_downsample_files(day_folder):
    if not os.path.exists(local_temp_dir):
        os.makedirs(local_temp_dir)

    # Create list of all wav files in the day folder
    wav_files = []
    for root, dirs, filenames in os.walk(day_folder):
        for filename in filenames:
            if filename.lower().endswith('.wav'):
                wav_files.append(os.path.join(root, filename))

    # Loop through each wav file with a progress bar
    for wav_file in tqdm(wav_files, desc=f"Downloading and downsampling files from {day_folder}", unit="file"):
        try:
            # Extract filename from file path
            file_name = os.path.basename(wav_file)
            # Local path to save the downsampled file
            local_file_path = os.path.join(local_temp_dir, file_name)
            
            # Load the audio file using librosa
            waveform_np, sample_rate = librosa.load(wav_file, sr=None)
            
            # Downsample the waveform to the target sample rate
            if sample_rate != target_sample_rate:
                waveform_np = librosa.resample(waveform_np, orig_sr=sample_rate, target_sr=target_sample_rate)
            
            # Save the downsampled audio file
            sf.write(local_file_path, waveform_np, target_sample_rate)
        except Exception as e:
            # Log any errors for debugging purposes
            print(f"Error processing {wav_file}: {e}")

def upload_files_via_scp(day_folder):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(scp_server, port=scp_port, username=scp_username, password=scp_password)

    sftp = ssh.open_sftp()

    # Extract day folder name to use as subfolder name on the remote server
    day_subfolder = os.path.basename(os.path.normpath(day_folder))
    # Construct remote subfolder path using forward slashes for Linux
    remote_subfolder_path = f"{scp_remote_base_path}{day_subfolder}"

    # Check if the subfolder exists, if not, create it
    try:
        sftp.listdir(remote_subfolder_path)
        print(f"Subfolder {remote_subfolder_path} already exists.")
    except IOError:
        print(f"Subfolder {remote_subfolder_path} does not exist. Creating it.")
        sftp.mkdir(remote_subfolder_path)

    # Loop through files in local_temp_dir with a progress bar
    for file in tqdm(os.listdir(local_temp_dir), desc=f"Uploading files to {remote_subfolder_path}", unit="file"):
        local_file_path = os.path.join(local_temp_dir, file)
        # Construct remote file path using forward slashes for Linux
        remote_file_path = f"{remote_subfolder_path}/{file}"
        
        # Upload the file to the correct subfolder
        print(f"Uploading {file} to {remote_file_path}")
        sftp.put(local_file_path, remote_file_path)

    sftp.close()
    ssh.close()

def process_all_files():
    # Traverse all month and day subdirectories
    for month_folder in os.listdir(source_dir):
        month_path = os.path.join(source_dir, month_folder)
        if os.path.isdir(month_path):
            for day_folder in os.listdir(month_path):
                day_path = os.path.join(month_path, day_folder)
                if os.path.isdir(day_path):
                    print(f"Processing files in {day_path}")
                    download_and_downsample_files(day_path)
                    upload_files_via_scp(day_path)
                    # Clean up temporary directory
                    for file in os.listdir(local_temp_dir):
                        os.remove(os.path.join(local_temp_dir, file))
                    print(f"Finished processing files for {day_folder}. Temporary files cleaned up.")
                    print("-" * 50)

# Main script execution
process_all_files()

print("All files processed and uploaded successfully.")
