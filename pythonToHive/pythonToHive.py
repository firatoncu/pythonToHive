#!/usr/bin/env python
# coding: utf-8

import paramiko
import subprocess
import pandas as pd
from paramiko import SSHClient
from scp import SCPClient
from pyhive import hive
from random import randint

def createConnection():
    
    host_name = 'atlashive.infoshop.com.tr'
    port = 10000
    user = 'dsapp'
    password = 'ds1q2w3e4r5t!'
    database = 'singlecatalog'
    
#     serverURL = 'jdbc:hive2://hadoopnn02.infoshop.com.tr:10000'
    conn = hive.Connection(host=host_name, port=port, username=user, password=password,
                            database=database, auth='CUSTOM')
    curs = conn.cursor()
    return curs, conn


def executeSQL(SQL):
 
    curs, conn = createConnection()
    curs.execute(SQL)

    
#     res = [col.encode('utf8') if isinstance(col, unicode) else col for col in row for row in curs.fetchall()]
    columns = [desc[0] for desc in curs.description] #getting column headers

    #convert the list of tuples from fetchall() to a df
    data  = pd.DataFrame(curs.fetchall(), columns=columns) 
    curs.close()
    conn.close()
    return data

def create_insert_table(query):
    
    curs, conn = createConnection()
    curs.execute(query)
    
    curs.close()
    conn.close()


def ssh_scp_files(table_name, source_volume):
    
    username = "dsapp_scienceuser" #sasserver01 user
    password = "UnHp2T3*z"
    unique_filename = randint(0,10000)
    
    columntype = []
    columns = ""
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect("10.16.16.21", username="dsapp", password="ds1q2w3e4r5t!", look_for_keys=False) #ssh connection to edgenode with dsapp user
    subprocess.call("""sshpass -p "ds1q2w3e4r5t!" ssh -o StrictHostKeyChecking=no dsapp@atlase01.infoshop.com.tr -mkdir /home/dsapp/Python_CSV/""" + str(unique_filename)+""";""")
    with SCPClient(ssh.get_transport()) as scp:
        scp.put(source_volume, recursive=True, remote_path="/home/dsapp/Python_CSV/"+str(unique_filename)+"/") #uploads target CSV to edgenode
    subprocess.call("""sshpass -p "ds1q2w3e4r5t!" ssh -o StrictHostKeyChecking=no dsapp@atlase01.infoshop.com.tr hadoop fs -rmdir -r /user/dsapp/projects/Python_CSV/"""+str(unique_filename)+""";""")
    subprocess.call("""sshpass -p "ds1q2w3e4r5t!" ssh -o StrictHostKeyChecking=no dsapp@atlase01.infoshop.com.tr hadoop fs -mkdir /user/dsapp/projects/Python_CSV/"""+str(unique_filename)+""";""")
    subprocess.call("""sshpass -p "ds1q2w3e4r5t!" ssh -o StrictHostKeyChecking=no dsapp@atlase01.infoshop.com.tr hadoop fs -put /home/dsapp/Python_CSV/""" + str(unique_filename)+"""/*.csv /user/dsapp/projects/Python_CSV"""+str(unique_filename)+""";""")
    subprocess.call("""sshpass -p "ds1q2w3e4r5t!" ssh -o StrictHostKeyChecking=no dsapp@atlase01.infoshop.com.tr -rmdir -r /home/dsapp/Python_CSV/""" + str(unique_filename)+""";""")
    df = pd.read_csv(source_volume, sep='\t')
    #reading csv file's column types and make column types suitable for hql
    columnname = df.columns
    columnname =  columnname + "_" ##order, min , max veya sayı gibi hive' da kolon adı olarak kabul edilmeyen case' lerin önüne geçmek için
    d = {
        "object": "string", 
        "int64" : "int", 
        "float64" : "float", 
        "bool" : "boolean"
    }
    columntype = [d[str(item)] for item in df.dtypes]
    
    columns = []
    for item in zip(columnname, columntype):
          columns.append(" ".join(item))
    columndetails = ",".join(columns)
    #creating external table
    create_insert_table("""
    CREATE EXTERNAL TABLE """ + table_name + """_ext
    (
    """ +columndetails+"""
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY "\t"
    LOCATION
    """ + location + """
    TBLPROPERTIES("skip.header.line.count"="1")
    """)
    #creating orc formatted hive table instead of external table
    subprocess.call("""sshpass -p "ds1q2w3e4r5t!" ssh -o StrictHostKeyChecking=no dsapp@atlase01.infoshop.com.tr /home/dsapp/SSH_SCP/create_table.sh""" + tablename + """;""")
    #cleaning edgenode and hdfs
    subprocess.call("""sshpass -p "ds1q2w3e4r5t!" ssh -o StrictHostKeyChecking=no dsapp@atlase01.infoshop.com.tr hadoop fs -rmdir -r /user/dsapp/projects/Python_CSV/"""+str(unique_filename)+""";""")
    subprocess.call("""sshpass -p "ds1q2w3e4r5t!" ssh -o StrictHostKeyChecking=no dsapp@atlase01.infoshop.com.tr -rmdir -r /home/dsapp/Python_CSV/""" + str(unique_filename)+""";""")



