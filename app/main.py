import sys
from dataclasses import dataclass
import sqlite3

# Function to get the page size from the SQLite file header
def get_page_size(database_file):
    # Seek to byte 16 in the header where page size is stored
    database_file.seek(16)
    # Read 2 bytes to get the page size, which is in big-endian format
    page_size = int.from_bytes(database_file.read(2), byteorder="big")
    return page_size

def print_no_of_tables(database_file_path):
    try:
        # attempt to connect to sqlite file path
        conn = sqlite3.connect(database_file_path)
        cursor = conn.cursor()

        # executing a query to retrive all the table names from sqlite_master
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        #printing the number of tables
        print(f"Number of tables : {len(tables)}")

        #printing the names of tables 
        print("Names of the tables :")
        for table in tables:
            print(f"Table : {table[0]}")

        #closing the connection
        conn.close()
    except sqlite3.Error as e:
        print(f"SQLite Error Occured : {e}")

def count_rows_in_tables(database_file_path, table_name):
    try:
        conn = sqlite3.connect(database_file_path)
        cursor = conn.cursor()

        # query to count rows
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        rowcount = cursor.fetchone()[0]

        #printing the output
        print(f"Table {table_name} have {rowcount} rows.")

        conn.close()

    except sqlite3.Error as e:
        print(f"SQLite error Occured : {e}")

def get_column_names(database_file_path,table_name):
    try:
        conn = sqlite3.connect(database_file_path)
        cursor = conn.cursor()

        # Executing the PRAGMA query to get column info from the specified table
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()

        #printing column names from the result
        for col in columns:
            print(f"-- {col[1]}") #the column name is in the second position of the tuple

        conn.close()
    except sqlite3.Error as e:
        print(f"SQLite error occured : {e}")

def read_data_from_column(database_file_path, table_name, column_name):
    try:
        conn = sqlite3.connect(database_file_path)
        cursor = conn.cursor()
        
        # query to load data from the specified column

        cursor.execute(f"SELECT {column_name} FROM {table_name};")
        rows = cursor.fetchall()

        #printing the data from specified column
        print(f"Data from Column '{column_name}' in table : '{table_name}';")
        for row in rows:
            print(row[0]) # value from the column

        conn.close()
    
    except sqlite3.Error as e:
        print(f"SQLite error occured : {e}")

def filter_data(database_file_path, table_name, column_name, filter_value):
    try:
        conn = sqlite3.connect(database_file_path)
        cursor = conn.cursor()

        #sql query with where clause
        query  = f"SELECT * FROM {table_name} WHERE {column_name} = ?"

        #execute the query
        cursor.execute(query,(filter_value,))

        # fetching all matching rows
        rows = cursor.fetchall()
        if rows:
            for row in rows:
                print(row)
        else:
            print("No matches found for the given criteria")

        conn.close()
    except sqlite3.Error as e:
        print(f"SQLite error occured : {e}")

def read_full_table(database_file_path, table_name):
    try:
        conn = sqlite3.connect(database_file_path)
        cursor = conn.cursor()

        #sql query to select all data
        cursor.execute(f"SELECT * FROM {table_name};")

        rows = cursor.fetchall()
        
        if rows:
            for row in rows:
                print(row)
        else:
            print("Table is empty")

        conn.close()
    except sqlite3.Error as e:
        print(f"SQLite error occured : {e}")

def retrive_with_index(database_file_path,table_name,column_name,index_value):
    try:
        conn = sqlite3.connect(database_file_path)
        cursor = conn.cursor()

        #creating an index on that column if not exist
        index_name = f"{table_name}_{column_name}_idx"
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column_name});")

        #query the data using indexed column
        query = f"SELECT * FROM {table_name} WHERE {column_name} = ?"
        cursor.execute(query,(index_value,))

        # fetch and print all rows from the data
        rows = cursor.fetchall()
        if rows:
            for row in rows:
                print(row)
        else:
            print("No match found")

        conn.close()
    except sqlite3.Error as e:
        print(f"SQLite error occured : {e}")

def create_table(database_file_path,table_name, columns):
    try:
        conn = sqlite3.connect(database_file_path)
        cursor = conn.cursor()

        #constructing the create table query contents
        column_definations = ", ".join(columns)
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_definations});"

        cursor.execute(create_table_query) # executing the created query
        print(f"Table created with Name : {table_name} with columns {column_definations}")

        conn.close()
    except sqlite3.Error as e:
        print(f"SQLite error occured : {e}")

def insert_data(database_file_path, table_name,columns, values):
    try:
        conn = sqlite3.connect(database_file_path)
        cursor = conn.cursor()

        # preparing the insert into command
        columnnames = ",".join(columns)
        data = ", ".join("?" for _ in values)

        insert_query = f"INSERT INTO {table_name} ({columnnames}) VALUES ({data});"

        cursor.execute(insert_query, values)
        conn.commit() # commiting the changes
        print(f"Data inserted successfully in table {table_name} : {values}")

        conn.close()
    
    except sqlite3.Error as e:
        print(f"SQLite error occured : {e}")
    
def create_empty_db(database_file_path):
    print("Note : this will only create the DB if it don't exist.")
    try:
        conn = sqlite3.connect(database_file_path)
        print(f"New empty DB is created as {database_file_path}")
        conn.close()
    except sqlite3.Error as e:
        print(f"SQLite error occured : {e}")

if __name__ == '__main__':
# File path and command input
    database_file_path = sys.argv[1]
    command = sys.argv[2]
# Main execution based on the command
    if command == ".dbinfo":
        try:
            with open(database_file_path, "rb") as database_file:
                # Print page size by calling the function
                page_size = get_page_size(database_file)
                print(f"Database page size: {page_size}")
        except FileNotFoundError:
            print(f"File not found: {database_file_path}")
        except Exception as e:
            print(f"An error occurred: {e}")
    elif command == ".tables":
        print_no_of_tables(database_file_path)
    elif command == ".countrows":
        table_name = sys.argv[3]
        count_rows_in_tables(database_file_path,table_name)
    elif command == ".readcolumns":
        table_name = sys.argv[3]
        get_column_names(database_file_path,table_name)
    elif command == ".readcolumn":
        table_name = sys.argv[3]
        column_name = sys.argv[4]
        read_data_from_column(database_file_path, table_name, column_name)
    elif command == ".filter":
        table_name = sys.argv[3]
        column_name = sys.argv[4]
        filter_value = sys.argv[5]
        filter_data(database_file_path, table_name, column_name, filter_value)
    elif command == ".readall":
        table_name = sys.argv[3]
        read_full_table(database_file_path, table_name)
    elif command == ".index": # start retriving based on index {create if not exist}:
        table_name = sys.argv[3]
        column_name = sys.argv[4]
        index_value = sys.argv[5]
        retrive_with_index(database_file_path,table_name,column_name,index_value)
    elif command == ".createTable":
        table_name = sys.argv[3]
        columns = sys.argv[4:] # getting the columns as additional arguments
        create_table(database_file_path,table_name, columns)
    elif command == ".insert_data":
        table_name = sys.argv[3]
        columns = sys.argv[4].split(",") # accepting the column names as comma saperated value
        values = sys.argv[5:] # values as additional arguments
        insert_data(database_file_path, table_name,columns, values)
    elif command == ".createDatabase":
        create_empty_db(database_file_path)
    else:
        print(f"Invalid command: {command}")

