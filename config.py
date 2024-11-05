import mysql.connector
# MySQL database connection
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="aqmstang"
)

folder_path = 'C:/xampp/htdocs/cbi/ftp/'
ip_local = "127.0.0.1"  # Destination IP for UDP transmission
udp_port_data = 2042 
udp_port_warning = 10240
ip_das  = '127.0.0.1'