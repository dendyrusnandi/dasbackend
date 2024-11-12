import ftplib
import os
import time

# FTP connection details
ftp_host = "210.210.166.82"
ftp_user = "ftptest"
ftp_passwd = "ftptest0309"  # Replace with the correct password
remote_dir = "/home/ftptest/mcz/"  # Remote directory to copy
local_dir = '/home/cbi/dbmcz/' # Local directory to save the files

# Ensure the local directory exists
if not os.path.exists(local_dir):
    os.makedirs(local_dir)

# Establish FTP connection
ftp = ftplib.FTP(ftp_host)
ftp.login(ftp_user, ftp_passwd)

# Function to download the latest file and delete all remote files
def download_and_delete_files():
    try:
        # List files in the remote directory
        files = ftp.nlst(remote_dir)
        print(f"Files found: {files}")
        
        # If there are no files, exit
        if not files:
            print("No files found in the remote directory.")
            return
        
        # Download all files from the remote directory
        for file_name in files:
            # Ensure local file path is correct
            local_path = os.path.join(local_dir, os.path.basename(file_name))
            
            # Open local file for writing
            with open(local_path, "wb") as local_file:
                # Download the file from the FTP server
                ftp.retrbinary(f"RETR {file_name}", local_file.write)
                print(f"Downloaded: {file_name}")
            
            # Delete the file from the remote server after download
            ftp.delete(file_name)
            print(f"Deleted: {file_name}")
    
    except ftplib.error_perm as e:
        print(f"FTP Permission Error: {e}")
    except Exception as e:
        print(f"Error: {e}")

# Loop to download files and delete them from the remote server every 5 seconds
while True:
    download_and_delete_files()  # Download and delete files
    time.sleep(65)  # Wait for 5 seconds before downloading again

# Close the FTP connection
ftp.quit()
