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
from datetime import datetime
import logging


current_directory = os.getcwd()
mapping_json_file = os.path.join(current_directory, 'mapping.json')


# Set the precision for Decimal operations
getcontext().prec = 50 

# Configure logging
logging.basicConfig(
    filename='error_log.txt',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

folder_path = config.folder_path
udp_ip = config.ip_local
udp_port = config.udp_port_warning
udp_port_warning = config.udp_port_warning



def send_data_via_udp(data, udp_ip, udp_port):
    """Convert data to JSON and send via UDP."""
    try:
        # Convert data to JSON format
        # json_data = json.dumps(data)
        
        # Send data over UDP
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(data.encode('utf-8'), (udp_ip, udp_port))
        print(f"Data sent to {udp_ip}:{udp_port}")
    
    except socket.error as e:
        print(f"Socket error: {e}")




def save_mean_value(row):
    key = row['key']
    mean_value = row['MeanValue']
    folder_path = 'file'
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, f"{key}.txt")
    
    try:
        with open(file_path, 'w') as file:
            file.write(str(mean_value))
        print(f"Saved MeanValue '{mean_value}' to file '{file_path}'.")
    except Exception as e:
        logging.error(f"Error saving MeanValue to file '{file_path}': {e}")
        print(f"Error saving MeanValue to file '{file_path}': {e}")

def job():
    try:
        csv_files = [f for f in os.listdir(folder_path) if f.endswith('.mv1.csv')]
        if not csv_files:
            #data = '{"identifier": "alarm", "type": "comm", "message": "DAS No Data [' + udp_ip + ']"}'
            data = '{"identifier": "alarm", "type": "comm", "message": "DAS No Data File"}'
            #data = "{\"identifier\" : \"alarm\", \"type\" : \"DAS\", \"message\" : \"DAS No Data [" + udp_ip + "]\"}"
            send_data_via_udp(data, udp_ip, udp_port_warning)
            return

        csv_files = sorted(csv_files, reverse=True)
        latest_file_path = os.path.join(folder_path, csv_files[0])
        csv_file_name = csv_files[0].split('-')[1] + "-" + csv_files[0].split('-')[2].split('.')[0]
        csv_file_time = csv_file_name.replace("-", "")
        
        datetimes = datetime.now()
        current_datetime = datetimes.strftime("%Y%m%d%H%M")

        if csv_file_time < current_datetime:
            #data = "{\"identifier\" : \"alarm\", \"type\" : \"DAS\", \"message\" : \"DAS File Not Update [" + udp_ip + "]\"}"
            #data = '{"identifier": "alarm", "type": "comm", "message": "DAS File not Update [' + udp_ip + ']"}'
            data = '{"identifier": "alarm", "type": "comm", "message": "DAS File not Update "}'
            send_data_via_udp(data, udp_ip, udp_port_warning)
        else:
            print("csv_file_time Update")

        if len(csv_files) > 10:
            files_to_delete = csv_files[10:]
            for old_file in files_to_delete:
                old_file_path = os.path.join(folder_path, old_file)
                os.remove(old_file_path)
                print(f"Deleted old file: {old_file_path}")

        df = pd.read_csv(latest_file_path, delimiter=';')
        print(f"Loaded CSV file: {latest_file_path}")

        with open(mapping_json_file, 'r') as json_file:
            compoid_mapping = json.load(json_file)

        mapping_df = pd.DataFrame(
            [(key, int(value) if value else None) for key, value in compoid_mapping.items()],
            columns=["key", "CompoID"]
        )

        merged_df = pd.merge(mapping_df, df, on="CompoID", how="left")
        connection = config.connection
        cursor = connection.cursor(dictionary=True)

        for index, row in merged_df.iterrows():
            key = row['key']
            MeanValue = row['MeanValue']
            CompoID = row['CompoID']

            if math.isnan(MeanValue):
                MeanValue = 0
                #data = "{\"identifier\" : \"alarm\", \"type\" : \"DAS\", \"message\" : \"DAS Data "+key+" 0 [" + udp_ip + "]\"}"
                #data = '{"identifier": "alarm", "type": "comm", "message": "DAS Data '+key+' 0 [' + udp_ip + ']"}'
                data = '{"identifier": "alarm", "type": "comm", "message": "DAS Data ['+key+'] Not Available"}'
                send_data_via_udp(data, udp_ip, udp_port_warning)

            row = {'key': key, 'MeanValue': MeanValue}
            save_mean_value(row)

        cursor.close()

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")

# Schedule the job to run every 5 seconds


def schedule_run():
    while True:
        try:
            current_time = datetime.now() 
            if current_time.second == 30:  #setiap detik ke 5
                job()
        except Exception as e:
            print(f"An error occurred: {e}")
            logging.error(f"error: {e}")
        time.sleep(1)  


schedule.every(1).minutes.do(schedule_run)

while True:
    schedule.run_pending()
    time.sleep(1)
