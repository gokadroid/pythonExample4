"""
format of json from where db conenction strings and other details are pulled is below. You can save this json 
in say connection.json file and then pass the absolute file path in pathToJson in line 87 below:
{
	"hostname": "DB-hostname",
	"port": portNumber,
	"sid": "myDBSid",
	"user": "DB-UserName",
	"pwd": "DB-password",
	"query": [
				{"first_query": "Select *  From table1"},
				{"second_query": "Select *  From table1" }
			 ],
	"csvs": [
				{"firstFileheader":"Name, Age, RollNo",
				"firstFileLoc": "Path where the csv can be saved"
				},
				{"secondFileheader":"Name, Id, Salary",
				"secondFileLoc": "./data/"
				}
			 ]

}
"""


import json
from pathlib import Path
import cx_Oracle
import sys
import time
import datetime


def connectToDb(pathToJson):
    print("Inside connect to Db "+valueSupplied)
    today = datetime.date.today()
    
    #Open the pathToJson json file to fetch db details
    with open(pathToJson) as json_data:
        db_json=json.load(json_data)
#        db_json['query'][0]['first_query']
# open file 1 to save data fetched from query 1
        myFileName=db_json['csvs'][0]['firstFileLoc']+"myFirstDataFile-"+str(today)+".csv"
        print(str(datetime.datetime.now()),": Opening "+myFileName)
# Open the file in write mode, this overwrites any existing file of the name used
        mf = open(myFileName, "w")
        
# Write the header of the csv which is fetched from "firstFileheader"
        mf.write(db_json['csvs'][0]['firstFileheader']+'\n')
        
#Fetch other connection details from json

        hostname = db_json['hostname']
        port = db_json['port']
        sid = db_json['sid']
        user = db_json['user']
        password = db_json['pwd']

#Build the tns string from host, port and sid
        dsn_tns = cx_Oracle.makedsn(hostname, port, sid)
        try:
            print(str(datetime.datetime.now()),": Connecting to DB")
            #Make db connection
            cnx = cx_Oracle.connect(user, password, dsn_tns)
        except mysql.connector.Error as err:
            #catch any errors in making connection and write them to same csv file if error happens
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                mf.write("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                mf.write("Database does not exist")
            else:
                mf.write(err)
        else:
            #Connection has been made
            curs = cnx.cursor()
            print(str(datetime.datetime.now()),": Executing the query on first table")
            curs.execute(db_json['query'][0]['first_query'])
            for row in curs:
                mf.write(row[0]+","+row[1]+","+row[2]+'\n')
            print(str(datetime.datetime.now()),": Writing "+myFileName+" complete.")
        mf.close()
        cnx.close()
    return
    
    #update the path of json file having db connection configs
    pathToJson = 'path to json config file'
    my_json_file = Path(pathToJson)
    #check if path exists, and if so call the connectToDb function to make connection
    if my_json_file.exists():
        connectToDb(pathToJson)
    else:
        print("File "+pathToJson+" doesnt exist!!")
        sys.exit(3)
