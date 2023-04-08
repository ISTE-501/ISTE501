import mysql.connector
from mysql.connector import Error
from apachelogs import LogParser

#filename = 'access_log_20230213-022706.log'
filename = 'access_log_20230407-030806.log'
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

create_table_studentinfo = """
    CREATE TABLE StudentInfo (
        studentID VARCHAR(7) NOT NULL,
        firstName VARCHAR(50) NOT NULL,
        lastName VARCHAR(50) NOT NULL,
        PRIMARY KEY (StudentID)
    );
"""

create_table_classlist = """
    CREATE TABLE ClassList (
        classID INT,
        class VARCHAR(255),
        PRIMARY KEY (classID)
    );
"""

create_table_log = """
    CREATE TABLE Logs (
        studentID VARCHAR(7) NOT NULL, 
        timestamp VARCHAR(255) NOT NULL, 
        request VARCHAR(255), 
        code INT NOT NULL, 
        classID INT NOT NULL,
        PRIMARY KEY (timestamp),
        FOREIGN KEY (studentID) REFERENCES StudentInfo(studentID),
        FOREIGN KEY (classID) REFERENCES ClassList(classID)
    );
"""

insert_into_classlist = """
    INSERT INTO ClassList (classID, class) VALUES 
    (1, "ISTE 140"),
    (2, "ISTE 240"),
    (3, "ISTE 340"),
    (4, "ISTE 341")
"""

execute_query(connection, create_table_studentinfo)
execute_query(connection, create_table_classlist)
execute_query(connection, create_table_log)
execute_query(connection, insert_into_classlist)

def reader(filename):
    with open(filename) as f:
        for entry in parser.parse_lines(f):
            execute_query(connection, "INSERT INTO StudentInfo (firstName, lastName, studentID) VALUES ('" + 
                         str(entry.headers_in["Referer"]) + "', '" + #first
                         str(entry.headers_in["User-Agent"]) + "', '" + #last
                         str(entry.remote_host) + #studentID
                       "')")
            execute_query(connection, "INSERT INTO Logs (studentID, timestamp, request, code, classID) VALUES ('" + 
                          str(entry.remote_host) + "', '" + #studentID
                          str(entry.directives["%t"])  + "', '" + #timestamp
                          str(entry.request_line)  + "', '" + #request
                          str(entry.final_status)  + "', '" + #code
                          str(entry.bytes_sent) + #classID
                        "')")
            
if __name__ == '__main__':
    reader(filename)