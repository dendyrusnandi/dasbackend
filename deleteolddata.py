import os
import time
from datetime import datetime, timedelta
import schedule
import config

def delete_old_files():
    # Define the directory and the file pattern prefix
    directory = config.folder_path
    prefix = "002-"
    suffix = ".mv1.csv"
    one_hour_ago = datetime.now() - timedelta(hours=24)

    # Iterate over files in the directory
    for filename in os.listdir(directory):
        # Check if the file matches the prefix and suffix
        if filename.startswith(prefix) and filename.endswith(suffix):
            # Extract the timestamp part of the filename
            try:
                timestamp_str = filename.split("-")[1] + filename.split("-")[2][:2]
                file_time = datetime.strptime(timestamp_str, "%Y%m%d%H")
                
                # Check if the file is older than 5 hours
                if file_time < one_hour_ago:
                    # Construct the full file path
                    file_path = os.path.join(directory, filename)
                    
                    # Delete the file
                    os.remove(file_path)
                    print(f"Deleted file: {file_path}")
            except (IndexError, ValueError):
                # Handle any parsing errors in case the filename is unexpected
                print(f"Skipped file due to format error: {filename}")

# Schedule the job every hour
schedule.every(1).hour.do(delete_old_files)

# Keep the script running to allow schedule to trigger the job
while True:
    schedule.run_pending()
    time.sleep(60)  # Check every minute for scheduled tasks
