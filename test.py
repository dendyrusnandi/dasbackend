import mysql.connector
from mysql.connector import Error

def test_mysql_connection():
    connection = None  # Initialize connection variable
    
    try:
        # Replace with your MySQL server details
        connection = mysql.connector.connect(
            host='localhost',  # MySQL server address
            user='cbi',
            password='cbipa55wd01!',
            database='aqms'
        )

        # Check if the connection is successful
        if connection.is_connected():
            print("Connection to MySQL database was successful")
            
            # Get MySQL server version
            cursor = connection.cursor()
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()
            print(f"MySQL version: {version[0]}")

            cursor.close()

    except Error as e:
        print("Error while connecting to MySQL:", e)
    
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("MySQL connection is closed")

# Test the connection
test_mysql_connection()

