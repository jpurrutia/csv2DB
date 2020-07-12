import psycopg2
import sys
import rds_config


def pg_load_table(file_path,table_name,dbname,host,port,user,pwd):
    """
    This function upload csv to a target table_name
    """
    try:
        connection = psycopg2.connect(dbname=dbname,
                                      host=host,
                                      port=port,
                                      user=user,
                                      password=pwd)
        print("Connecting to DB")
        # allows python code to run SQL commands
        cursor = connection.cursor()
        f = open(file_path,"r")

        # Truncate the table - remove all rows
        cursor.execute("Truncate {} Cascade;".format(table_name))
        print("Truncated {}".format(table_name))
        # Load table from file with header - copy_expert submit user-composed COPY statment
        cursor.copy_expert("copy {} from STDIN CSV HEADER QUOTE '\"'".format(table_name), f)
        cursor.execute("commit;")
        print("Loaded data into {}".format(table_name))
        connection.close()
        print("DB connection closed.")
    
    except Exception as e:
        print("Error: {}".format(str(e)))
        sys.exit(1)


# Execution
file_path = '/Users/jpurrutia/Desktop/rfdb/tables/file.csv'
table_name = 'schema.table'
dbname = rds_config.db_name
host = rds_config.db_endpoint
port = rds_config.db_port
user = rds_config.db_username
pwd = rds_config.db_password
pg_load_table(file_path, table_name, dbname, host, port, user, pwd)

# filepath_1
# Filepath_2
# filepath_3
# filepath_4
# filepath_5
# filepath_6
# filepath_7
# Filepath_8
# filepath_9
# filepath_10
# filepath_11
# filepath_12
# filepath_13
# filepath_14
# filepath_15
# filepath_16
# filepath_17
# filepath_18
# filepath_19
# filepath_20
# filepath_21
# filepath_22