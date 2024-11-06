import mysql.connector
# MySQL database connection
connection = mysql.connector.connect(
    host="localhost",
    user="cbi",
    password="cbipa55wd01!",
    database="aqms"
)

folder_path = '/home/cbi/dbmcz/'
ip_local = "127.0.0.1"  # Destination IP for UDP transmission
udp_port_data = 2042 
udp_port_warning = 10240
ip_das  = '127.0.0.1'
port_request = 1024

