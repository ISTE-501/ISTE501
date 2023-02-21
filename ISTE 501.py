import mysql.connector
from mysql.connector import Error
from apachelogs import LogParser

#filename = 'access_log_20230213-022706.log'
filename = 'access_log_20230221-002559.log'
parser = LogParser("%h %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"")

def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute("DROP DATABASE mydatabase")
        cursor.execute(query)
        cursor.execute("USE mydatabase")
        print("Database created")
    except Error as err:
        print(f"Error: '{err}'")

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

connection = create_server_connection("localhost", "root", "student")
create_database_query = "CREATE DATABASE mydatabase"
create_database(connection, create_database_query)
create_table = """
    CREATE TABLE logs (
    ip VARCHAR(255), 
    date VARCHAR(255), 
    request VARCHAR(255), 
    code VARCHAR(255), 
    bytes VARCHAR(255), 
    referer VARCHAR(255), 
    useragent VARCHAR(255)
    );
"""

execute_query(connection, create_table)

def reader(filename):
    with open(filename) as f:
        for entry in parser.parse_lines(f):
            execute_query(connection, "INSERT INTO logs (ip, date, request, code, bytes, referer, useragent) VALUES ('" + 
                          str(entry.remote_host) + "', '" +
                          str(entry.directives["%t"])  + "', '" + 
                          str(entry.request_line)  + "', '" + 
                          str(entry.final_status)  + "', '" + 
                          str(entry.bytes_sent) + "', '" + 
                          str(entry.headers_in["Referer"]) + "', '" + 
                          str(entry.headers_in["User-Agent"]) + 
                        "')")

if __name__ == '__main__':
    reader(filename)