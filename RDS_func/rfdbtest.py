import rds_config
import mysql.connector
import csv

connection = None

try:
    connection = mysql.connector.connect(user=rds_config.db_username,
                                         password=rds_config.db_password,
                                         host=rds_config.db_endpoint,
                                         port=rds_config.db_port)
                                         #database=rds_config.db_name)
        
    cursor = connection.cursor()
    cursor.execute("CREATE SCHEMA rfanalytics")
    cursor.execute("USE rfanalytics")


    cursor.execute("CREATE TABLE hitters (Name VARCHAR(255), Age INTEGER, Tm VARCHAR(255), Lg VARCHAR(255), G INTEGER, PA INTEGER, AB INTEGER, R INTEGER \
                    ,H INTEGER, TWOB INTEGER, THREEB INTEGER, HR INTEGER, RBI INTEGER, SB INTEGER, CS INTEGER, BB INTEGER \
                    ,SO INTEGER, BA DECIMAL(10,2) , OBP DECIMAL(10,2), SLG DECIMAL(10,2), OPS DECIMAL(10,2), OPS_plus INTEGER, TB INTEGER, GDP INTEGER \
                    ,HBP INTEGER, SH INTEGER, SF INTEGER, IBB INTEGER)")

except mysql.connector.Error as e:
    print(e)

with open('/Users/jpurrutia/Desktop/RockFence/hitters_test.csv','r') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    next(csv_reader)
    for row in csv_reader:
        print(row)
        Name=row[0]
        Age=row[1]
        Tm=row[2]
        Lg=row[3]
        G=row[4]
        PA=row[5]
        AB=row[6]
        R=row[7]
        H=row[8]
        TWOB=row[9]
        THREEB=row[10]
        HR=row[11]
        RBI=row[12]
        SB=row[13]
        CS=row[14]
        BB=row[15]
        SO=row[16]
        BA=row[17]
        OBP=row[18]
        SLG=row[19]
        OPS=row[20]
        OPS_plus=row[21]
        TB=row[22]
        GDP=row[23]
        HBP=row[24]
        SH=row[25]
        SF=row[26]
        IBB=row[27]
        cursor.execute("USE rfanalytics")
        cursor.execute("""INSERT INTO hitters (Name,Age,Tm,Lg,G,PA,AB,R,H,TWOB,THREEB,HR,RBI,SB,CS,BB,SO,BA,OBP,SLG,OPS,OPS_plus,TB,GDP,HBP,SH,SF,IBB) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (Name,Age,Tm,Lg,G,PA,AB,R,H,TWOB,THREEB,HR,RBI,SB,CS,BB,SO,BA,OBP,SLG,OPS,OPS_plus,TB,GDP,HBP,SH,SF,IBB))
        connection.commit()



