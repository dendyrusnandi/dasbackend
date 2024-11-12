import mysql.connector
# MySQL database connection
try:
    connection = mysql.connector.connect(
        host="localhost",
        user="cbi",
        password="cbipa55wd01!",
        database="aqms"
    )
except mysql.connector.Error as err:
    print(f"Error connecting to MySQL: {err}")
    
folder_path = '/home/cbi/dbmcz/'
ip_local = "127.0.0.1"  # Destination IP for UDP transmission
udp_port_data = 2042 
udp_port_warning = 2046
ip_das  = '127.0.0.1'
port_request = 1024

