import os
import pandas as pd
import json
import mysql.connector
import config
import socket
from decimal import Decimal, getcontext
import schedule
import time
import math

# Set the precision for Decimal operations
getcontext().prec = 50 

connection = config.connection
folder_path = config.folder_path
udp_ip = config.ip_local
udp_port = config.udp_port_data
udp_port_warning = config.udp_port_warning



def send_data_via_udp(data, udp_ip, udp_port):
    """Convert data to JSON and send via UDP."""
    try:
        # Convert data to JSON format
        json_data = json.dumps(data)
        
        # Send data over UDP
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(json_data.encode('utf-8'), (udp_ip, udp_port))
        print(f"Data sent to {udp_ip}:{udp_port}")
    
    except socket.error as e:
        print(f"Socket error: {e}")

def save_mean_value(row):
    # Extract key and MeanValue from the row
    key = row['key'].replace('.', '')
    mean_value = row['MeanValue']
    
    # Define the directory to store files
    folder_path = 'file'
    os.makedirs(folder_path, exist_ok=True)
    
    # Define the file path based on the key
    file_path = os.path.join(folder_path, f"{key}.txt")
    
    # Write or update the file with the new MeanValue
    with open(file_path, 'w') as file:
        file.write(str(mean_value))
    
    print(f"Saved MeanValue '{mean_value}' to file '{file_path}'.")


def job():
   


    try:
        csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
        if not csv_files:
            print("No CSV files found.")
            data = "{\"identifier\" : \"alarm\", \"type\" : \"DAS\", \"message\" : \"DAS No Data [" + udp_ip + "]\"}"
            print(data)
            send_data_via_udp(data, udp_ip, udp_port_warning)
            return

        csv_files = sorted(csv_files, reverse=True)
        latest_file_path = os.path.join(folder_path, csv_files[0])
        #old_file_path = os.path.join(folder_path, csv_files[1]) if len(csv_files) > 1 else None  # Second latest file, if it exists


        # Load the latest CSV file
        df = pd.read_csv(latest_file_path, delimiter=';')
        print(f"Loaded CSV file: {latest_file_path}")
        #print(f"Loaded CSV file: {old_file_path}")

        # Load the CompoID mapping from JSON
        with open('mapping.json', 'r') as json_file:
            compoid_mapping = json.load(json_file)

        # Convert JSON mapping into a DataFrame
        mapping_df = pd.DataFrame(
            [(key, int(value) if value else None) for key, value in compoid_mapping.items()],
            columns=["key", "CompoID"]
        )

        # Merge the CSV data with the mapping DataFrame on CompoID
        merged_df = pd.merge(mapping_df, df, on="CompoID", how="left")

        cursor = connection.cursor(dictionary=True)

        # Loop over each row in merged_df and fetch related data from MySQL
        for index, row in merged_df.iterrows():
            key = row['key']  # Get the key from the row
            MeanValue = row['MeanValue']
            CompoID = row['CompoID']


            if math.isnan(MeanValue):
                MeanValue = 0
                print(f'kirim alarm notif {key} Nan')
                data = "{\"identifier\" : \"alarm\", \"type\" : \"DAS\", \"message\" : \"DAS Data "+key+" 0 [" + udp_ip + "]\"}"
                print(data)
                send_data_via_udp(data, udp_ip, udp_port_warning)


            row = {'key': key, 'MeanValue': MeanValue}
            save_mean_value(row)

            # #if not math.isnan(MeanValue):
            # if key is not None and pd.notna(CompoID):
            #     print(f'Key: {key}, CompoID: {CompoID}, Value: {MeanValue}')
            #     query = "SELECT id as parameter_id, disabled_threshold, orchestrator_reduction, factor, min_value, max_value, formula,orchestrator_factor FROM datalogger_parameters WHERE `key` = %s"
            #     cursor.execute(query, (key,))
            #     mysql_data = cursor.fetchone()

            #     if mysql_data:
            #         parameter_id = mysql_data['parameter_id']
            #         raw_value = float(MeanValue)  # Assuming MeanValue is defined earlier in your code
            #         disabled_threshold = mysql_data['disabled_threshold']
            #         orchestrator_reduction = format(mysql_data['orchestrator_reduction'], '.25f')
            #         factor = format(mysql_data['factor'], '.5f')
            #         min_value = float(format(mysql_data['min_value'], '.5f'))
            #         max_value = float(format(mysql_data['max_value'], '.5f'))
            #         orchestrator_factor = format(mysql_data['orchestrator_factor'], '.5f')
            #         formula = mysql_data['formula']

            #         json_data = {
            #             'parameter_id': str(parameter_id),
            #             'raw_value': float(MeanValue),
            #             'disabled_threshold': disabled_threshold,
            #             'orchestrator_reduction': orchestrator_reduction,
            #             'factor': factor,
            #             'min_value': min_value,
            #             'max_value': max_value,
            #             'formula': formula,
            #             'orchestrator_factor': orchestrator_factor
            #         }

            #         # Convert the dictionary to a JSON string
            #         json_string = json.dumps(json_data)
            #         #print(json_string)
            #         #send_data_via_udp(json_data, udp_ip, udp_port)  # Send via UDP

            #         #os.remove(old_file_path)
            #         #print(f"Deleted CSV file: {old_file_path}")

            #     else:
            #         print("No MySQL data found for key:", key)


        #os.remove(old_file_path)
        #print(f"Deleted CSV file: {old_file_path}")

        cursor.close()  # Close the cursor after processing

    except Exception as e:
        print(f"An error occurred: {e}")

# Schedule the job to run every second
schedule.every(10).seconds.do(job)
#schedule.every(1).minutes.do(job)


while True:
     schedule.run_pending()
     time.sleep(1)  # Wait for one second before checking again

# if __name__ == "__main__":
#     job()
