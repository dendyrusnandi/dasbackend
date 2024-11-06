import socket
import config
import os
import json

connection = config.connection
port = config.port_request
host = config.ip_local
udp_port_data = config.udp_port_data
folder_path = 'file'
# Define the possible values we want to check for
valid_values = ['co', 'co2', 'hc', 'no2', 'noise', 'o3', 'pm10', 'pm2.5', 'so2', 'tvoc']



def send_data_via_udp(data, host, udp_port_data):
    """Convert data to JSON and send via UDP."""
    try:
        # Convert data to JSON format
        json_data = json.dumps(data)
        
        # Send data over UDP
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(json_data.encode('utf-8'), (host, udp_port_data))
        print(f"Data sent to {host}:{udp_port_data}")
    
    except socket.error as e:
        print(f"Socket error: {e}")


def udp_server(host=host, port=port):
    # Create a UDP socket
    response_message = 'response_message'
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    # Bind the socket to the address
    sock.bind((host, port))
    
    print(f"Listening for UDP packets on {host}:{port}...")
    
    while True:
        # Receive data from the client
        data, addr = sock.recvfrom(1024)  # Buffer size is 1024 bytes
        message = data.decode()  # Decode the message to string
        #print(f"Received message from {addr}: {message}")
        if message in valid_values:
            print('Valid message')
            # Construct the file name based on the message (e.g., "message.txt")
            file_path = os.path.join(folder_path, f"{message}.txt")
            
            print(file_path)
            # Check if the file exists
            if os.path.exists(file_path):
                # Read the content of the file
                with open(file_path, 'r') as file:
                    file_content = file.read()
                response_message = file_content
                print(file_content)

                cursor = connection.cursor(dictionary=True)
                query = "SELECT id as parameter_id, disabled_threshold, orchestrator_reduction, factor, min_value, max_value, formula,orchestrator_factor FROM datalogger_parameters WHERE `key` = %s"
                cursor.execute(query, (message,))
                mysql_data = cursor.fetchone()
                print(mysql_data)
                if mysql_data:
                     parameter_id = mysql_data['parameter_id']
                     raw_value = float(file_content)  # Assuming MeanValue is defined earlier in your code
                     disabled_threshold = mysql_data['disabled_threshold']
                     orchestrator_reduction = format(mysql_data['orchestrator_reduction'], '.25f')
                     factor = format(mysql_data['factor'], '.5f')
                     min_value = float(format(mysql_data['min_value'], '.5f'))
                     max_value = float(format(mysql_data['max_value'], '.5f'))
                     orchestrator_factor = format(mysql_data['orchestrator_factor'], '.5f')
                     formula = mysql_data['formula']

                     json_data = {
                         'parameter_id': str(parameter_id),
                         'raw_value': float(file_content),
                         'disabled_threshold': disabled_threshold,
                         'orchestrator_reduction': orchestrator_reduction,
                         'factor': factor,
                         'min_value': min_value,
                         'max_value': max_value,
                         'formula': formula,
                         'orchestrator_factor': orchestrator_factor
                     }

                     # Convert the dictionary to a JSON string
                     json_string = json.dumps(json_data)
                     print(json_string)
                     send_data_via_udp(json_data, host, udp_port_data)  # Send via UDP


            else:
                response_message = f"File for {message} not found."
        else:
            print('Invalid message')
            response_message = 'Invalid Message'
        
        sock.sendto(response_message.encode(), addr)


        print(f"Sent response to {addr}: {response_message}")

# Run the UDP server
udp_server()
