import mysql.connector
from mysql.connector import Error
from apachelogs import LogParser

#filename = 'access_log_20230213-022706.log'
filename = 'access_log_20230321-231106.log'
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
create_table_log = """
    CREATE TABLE Logs (
        StudentID VARCHAR(50) NOT NULL, 
        date VARCHAR(255) NOT NULL, 
        request VARCHAR(255), 
        code VARCHAR(255) NOT NULL, 
        referer VARCHAR(255), 
        useragent VARCHAR(255),
        classID VARCHAR(255) NOT NULL,
        PRIMARY KEY (StudentID)
    );
"""
create_table_studentinfo = """
    CREATE TABLE StudentInfo (
        FirstName VARCHAR(50) NOT NULL,
        LastName VARCHAR(50) NOT NULL,
        StudentID VARCHAR(7) NOT NULL,
        PRIMARY KEY (FirstName)
    );
"""

create_table_classlist = """
    CREATE TABLE ClassList (
        classID VARCHAR(255),
        class VARCHAR(255),
        PRIMARY KEY (classID)
    );
"""
execute_query(connection, create_table_log)
execute_query(connection, create_table_studentinfo)
execute_query(connection, create_table_classlist)

def reader(filename):
    with open(filename) as f:
        for entry in parser.parse_lines(f):
            execute_query(connection, "INSERT INTO Logs (StudentID, date, request, code, referer, useragent, classID) VALUES ('" + 
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