import mysql.connector
from mysql.connector import Error
from apachelogs import LogParser

filename = 'access_log_20230417-152001.log'
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
        studentID VARCHAR(255) NOT NULL,
        firstName VARCHAR(255) NOT NULL,
        lastName VARCHAR(255) NOT NULL,
        PRIMARY KEY (firstName, lastName)
    );
"""

create_table_classlist = """
    CREATE TABLE ClassList (
        classID INT NOT NULL,
        sessionID INT,
        class VARCHAR(255) NOT NULL,
        profID INT NOT NULL,
        taID INT,
        code VARCHAR (255) NOT NULL,
        PRIMARY KEY (classID)
    );
"""

create_table_log = """
    CREATE TABLE Logs (
        timestamp VARCHAR(255) NOT NULL, 
        request VARCHAR(255) NOT NULL, 
        code INT NOT NULL, 
        studentID VARCHAR(255) NOT NULL,
        classID INT NOT NULL,
        PRIMARY KEY (timestamp),
        FOREIGN KEY (classID) REFERENCES ClassList(classID)
    );
"""

alter_table = """
    ALTER TABLE Logs
    ADD CONSTRAINT logs_ibfk_2
    FOREIGN KEY(studentID) 
    REFERENCES StudentInfo(studentID);
"""

insert_into_classlist = """
    INSERT INTO ClassList (classID, sessionID, class, profID, taID, code) VALUES
    (1,	1,	'Web & Mobile 1',	0,	NULL,	'ISTE-140'),
    (2,	2,	'Web & Mobile 2',	0,	1,	'ISTE-240'),
    (3,	3,	'Client Programming',	0,	NULL,	'ISTE-340'),
    (4,	4,	'Server Programming',	0,	NULL,	'ISTE-341'),
    (5,	1,	'Web & Mobile 1',	3,	1,	'ISTE-140');
"""

execute_query(connection, create_table_studentinfo)
execute_query(connection, create_table_classlist)
execute_query(connection, create_table_log)
#execute_query(connection, alter_table)
execute_query(connection, insert_into_classlist)

def reader(filename):
    with open(filename) as f:
        for entry in parser.parse_lines(f):
            execute_query(connection, "INSERT IGNORE INTO StudentInfo (firstName, lastName, studentID) VALUES ('" + 
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