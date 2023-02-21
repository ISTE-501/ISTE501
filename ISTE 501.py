import mysql.connector
import re
from apachelogs import LogParser

def create_server_connection(host_name, user_name, user)

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created")
    except Error as err:
        print(f"Error: '{err}'")

ip = []
date = []
request = []
code = []
bytes = []
referer = []
useragent = []
#filename = 'access_log_20230213-022649.log'
filename = 'access_log_20230221-002559.log'
parser = LogParser("%h %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"")

mydb = mysql.connector.connect (
    host="localhost",
    user="root",
    password="student"
)

def reader(filename):
    with open(filename) as f:
        for entry in parser.parse_lines(f):
            ip.append(entry.remote_host)
            date.append(entry.directives["%t"])
            request.append(entry.request_line)
            code.append(entry.final_status)
            bytes.append(entry.bytes_sent)
            referer.append(entry.headers_in["Referer"])
            useragent.append(entry.headers_in["User-Agent"])
if __name__ == '__main__':
    reader(filename)

mycursor = mydb.cursor()
#sql = "INSERT INTO logs (ip, date, request, code, bytes, referer, useragent) VALUES (%s, %s, %s, %s, %s, %s, %s)"
val = [
    (ip),
    (date),
    (request),
    (code),
    (bytes),
    (referer),
    (useragent)
]

mycursor.execute("DROP DATABASE mydatabase")
mycursor.execute("CREATE DATABASE mydatabase")
mycursor.execute("USE mydatabase")
mycursor.execute("CREATE TABLE logs (ip VARCHAR(255), date VARCHAR(255), request VARCHAR(255), code VARCHAR(255), bytes VARCHAR(255), referer VARCHAR(255), useragent VARCHAR(255))")
for x in 
sql = "INSERT INTO logs (ip, date) VALUES (%s, %s)"
val = ("123.123.123.123", "2023-02-21 04:48:59+01:00")

mycursor.execute(sql, val)

mydb.commit()