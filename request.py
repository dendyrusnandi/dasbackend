import socket
import config
import os
import json
import mysql.connector  # Assuming you're using MySQL with MySQL Connector

port = config.port_request
host = config.ip_local
udp_port_data = config.udp_port_data
udp_port_warning = config.udp_port_warning
folder_path = 'file'
valid_values = ['co', 'co2', 'hc', 'no2', 'noise', 'o3', 'pm10', 'pm2.5', 'so2', 'tvoc','temperature']


def log_error(error_message):
    """Logs an error message to error_log.txt."""
    with open("error_log.txt", "a") as log_file:
        log_file.write(f"{error_message}\n")


def send_data_via_udp(data, host, udp_port_data):
    """Convert data to JSON and send via UDP."""
    try:
        json_data = json.dumps(data)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(json_data.encode('utf-8'), (host, udp_port_data))
        print(f"Data sent to {host}:{udp_port_data}")

    except socket.error as e:
        log_error(f"Socket error while sending data: {e}")
    finally:
        sock.close()


def udp_server(host=host, port=port):
    response_message = 'response_message'
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))

    print(f"Listening for UDP packets on {host}:{port}...")

    while True:
        try:
            data, addr = sock.recvfrom(1024)
            message = data.decode()

            if message in valid_values:
                print('Valid message')
                file_path = os.path.join(folder_path, f"{message}.txt")

                if os.path.exists(file_path):
                    with open(file_path, 'r') as file:
                        file_content = file.read()
                    response_message = file_content
                    print(file_content)

                    try:
                        connection = config.connection
                        cursor = connection.cursor(dictionary=True)
                        query = "SELECT id as parameter_id, disabled_threshold, orchestrator_reduction, factor, min_value, max_value, formula, orchestrator_factor FROM datalogger_parameters WHERE `key` = %s"
                        cursor.execute(query, (message,))
                        mysql_data = cursor.fetchone()

                        if mysql_data:
                            parameter_id = mysql_data['parameter_id']
                            raw_value = float(file_content)
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

                            json_string = json.dumps(json_data)
                            print(json_string)
                            send_data_via_udp(json_data, host, udp_port_data)

                    except mysql.connector.Error as db_err:
                        log_error(f"Database error: {db_err}")
                        data = "{\"identifier\" : \"alarm\", \"type\" : \"DAS\", \"message\" : \"DAS Error Database [" + udp_ip + "]\"}"
                        send_data_via_udp(data, host, udp_port_warning)
                    finally:
                        if cursor:
                            cursor.close()
                else:
                    response_message = f"File for {message} not found."
            else:
                print('Invalid message')
                response_message = 'Invalid Message'

        except socket.error as e:
            log_error(f"Socket error while receiving data: {e}")
        except UnicodeDecodeError as e:
            log_error(f"Decoding error: {e}")
        except Exception as e:
            log_error(f"Unexpected error: {e}")

        try:
            sock.sendto(response_message.encode(), addr)
            print(f"Sent response to {addr}: {response_message}")
        except Exception as e:
            log_error(f"Error sending response to {addr}: {e}")

# Run the UDP server
udp_server()
