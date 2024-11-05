import subprocess
import socket
import time
import config
import schedule
import json

ip_das = config.ip_das
udp_ip = config.ip_local
udp_port = config.udp_port_warning

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


def is_valid_ip(ip):
    """Check if the provided IP address is valid."""
    try:
        socket.inet_aton(ip)  # For IPv4
        return True
    except socket.error:
        return False

def ping_server(server_ip):
    """Ping a server and return True if reachable, False otherwise."""
    if not is_valid_ip(server_ip):
        print(f"Invalid IP address: {server_ip}")
        return False

    try:
        # Determine the correct ping command based on the OS
        param = '-n' if subprocess.os.name == 'nt' else '-c'
        command = ['ping', param, '2', server_ip]
        
        # Execute the ping command
        response = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Check the return code to determine if the ping was successful
        if response.returncode == 0:
            print(f"Ping to {server_ip} successful.")
            
            data = "{\"identifier\" : \"alarm\", \"type\" : \"DAS\", \"message\" : \"DAS OK [" + server_ip + "]\"}"
            send_data_via_udp(data, udp_ip, udp_port)
	       

            return True
        else:
            print(f"Ping to {server_ip} failed.")

            data = "{\"identifier\" : \"alarm\", \"type\" : \"DAS\", \"message\" : \"DAS failed [" + server_ip + "]\"}"
            send_data_via_udp(data, udp_ip, udp_port)


            return False
    except Exception as e:
        print(f"An error occurred while pinging {server_ip}: {e}")
        return False

def job():
	print(f"Check Connection {ip_das}")
	ping_server(ip_das)


schedule.every(5).seconds.do(job)

while True:
     schedule.run_pending()
     time.sleep(1)

# if __name__ == "__main__":
#     while True:
#         job()  # Call the job function to ping the server
#         time.sleep(10)  # Wait for 10 seconds before the next iteration
