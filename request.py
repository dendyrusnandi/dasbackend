import socket
import config
import os

port = config.port_request
host = config.ip_local
folder_path = 'file'
# Define the possible values we want to check for
valid_values = ['co', 'co2', 'hc', 'no2', 'noise', 'o3', 'pm10', 'pm25', 'so2', 'tvoc']


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
            else:
                response_message = f"File for {message} not found."
        else:
            print('Invalid message')
            response_message = "Invalid message"
        
        sock.sendto(response_message.encode(), addr)
        print(f"Sent response to {addr}: {response_message}")

# Run the UDP server
udp_server()
