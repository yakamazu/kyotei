# coding: utf-8

#import library
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime as dt
import datetime
import configparser
import requests
import subprocess
import os

#variable
config = configparser.ConfigParser()
config.read('../config/kyotei.config')

from_date = config.get('date', 'from_date')
to_date = config.get('date', 'to_date')

download_url = config.get('file', 'download_url_2')
file_dir = config.get('file', 'file_dir')
file_name_race_result = config.get('file', 'file_name_race_result')
file_name_lzh = ''
file_name_txt = ''

db_connect_info = config.get('db', 'db_connect_info')
db_host = config.get('db', 'db_host')
db_port = config.get('db', 'db_port')
db_dbname = config.get('db', 'db_dbname')
db_user = config.get('db', 'db_user')
db_password = config.get('db', 'db_password')

#-----------------------------
#function for download and clean raceinfo file
#-----------------------------
def DownloadFile():
    global file_name_lzh
    global file_name_txt
    
    file_name_lzh = 'b' + race_date[2:8] + '.lzh'
    file_name_txt = 'b' + race_date[2:8] + '.txt'

    #download file
    url = download_url + race_date[0:6] +  '/'+ file_name_lzh
    response = requests.get(url)
    with open(file_dir + file_name_lzh, 'wb') as saveFile:
        saveFile.write(response.content)
    
    #lha file
    cmd = "lha ew=%s %s" % (file_dir, file_dir + file_name_lzh)
    returncode = subprocess.call(cmd, shell=True)
    
    #encoding file
    cmd = "iconv -f SHIFT_JIS -t UTF-8 %s > %s" % (file_dir + file_name_txt, file_dir + file_name_race_result)
    returncode = subprocess.call(cmd, shell=True)

def CleanFile():
    #remove download_file
    cmd = "rm %s" % (file_dir + 'b*')
    returncode = subprocess.call(cmd, shell=True)

    #remove encoding file
    cmd = "rm %s" % (file_dir + file_name_race_result)
    returncode = subprocess.call(cmd, shell=True)

#-----------------------------
#function for set raceinfo
#-----------------------------
def SetRaceResult():
    race_result_attribute = pd.DataFrame(columns=['race_date', 'kaijo', 'race_no', 'entry_order', 'toroku_no', 'age', 'branch', 'weight', 'player_class', 'win_per', 'win_per_within_2', 'win_per_kaijo', 'win_per_kaijo_within_2', 'motor_win_per_within_2', 'boat_win_per_within_2'])
    #debug abc=1
    with open(file_dir + file_name_race_result, 'r') as f:
        for line in f:
            '''
            set race_result variable
                date
                kaijo
                race_no
                entry_order
                toroku_no
                age
                branch
                weight
                player_class
                win_per
                win_per_within_2
                win_per_kaijo
                win_per_kaijo_within_2
                motor_win_per_within_2
                boat_win_per_within_2
            ''' 
            #debug print(str(abc) + ' ' + str(len(line)))
            #debug abc += 1
            if line.find('BGN') > -1:
                kaijo = line[0:2]
            
            if line[2:3]=='Ｒ':
                race_no = int(line[0:2])
            
            if len(line) == 74 and line[0:1].isdigit() == True and 1 <= int(line[0:1]) <= 6:
                entry_order = int(line[0:1])
                toroku_no = int(line[2:6])
                age = int(line[10:12])
                branch = line[12:14]
                weight = int(line[14:16])
                player_class = line[16:18]
                win_per = float(line[18:23])
                win_per_within_2 = float(line[23:29])
                win_per_kaijo = float(line[29:35])
                win_per_kaijo_within_2 = float(line[35:40])
                motor_win_per_within_2 = float(line[44:49])
                boat_win_per_within_2 = float(line[53:58])
                
                #set race_result dataframe 
                race_result_attribute_tmp = pd.DataFrame([[race_date, kaijo, race_no, entry_order, toroku_no, age, branch, weight, player_class, win_per, win_per_within_2, win_per_kaijo, win_per_kaijo_within_2, motor_win_per_within_2, boat_win_per_within_2]], columns=['race_date', 'kaijo', 'race_no', 'entry_order', 'toroku_no', 'age', 'branch', 'weight', 'player_class', 'win_per', 'win_per_within_2', 'win_per_kaijo', 'win_per_kaijo_within_2', 'motor_win_per_within_2', 'boat_win_per_within_2'])
                race_result_attribute = race_result_attribute.append(race_result_attribute_tmp)
                
    return race_result_attribute
 
#-----------------------------
#functino for postgresql
#-----------------------------    
def Pands2Postgre(race_result_attribute):
    engine=create_engine(db_connect_info)

    race_result_attribute.to_sql("wk_race_result_attribute", engine, if_exists="replace", index=False)
    
    
def ExecuteSql(sqltext):
    conn = psycopg2.connect("host=" + db_host + " port=" + db_port + " dbname=" + db_dbname + " user=" + db_user + " password=" + db_password)
    cur = conn.cursor()

    with open(sqltext,'r') as f:
        sql_race_result = f.read()
    
        cur.execute(sql_race_result)
        
        conn.commit()
        cur.close()
        conn.close()
    

#-----------------------------
#function for main
#-----------------------------
def main():
    
    #race result to pandas
    race_result_attribute = SetRaceResult()
    
    #from pandas to postgresql (copy)
    Pands2Postgre(race_result_attribute)
    
    #insert race_result_attribute
    ExecuteSql('../sql/delete_race_result_attribute.sql')
    ExecuteSql('../sql/insert_race_result_attribute.sql')
    
    #clean file
    CleanFile()
    
    return race_result_attribute

    
#-----------------------------
#execute
#-----------------------------
if __name__ == '__main__':
    
    while dt.strptime(from_date, '%Y%m%d').strftime('%Y%m%d') <= dt.strptime(to_date, '%Y%m%d').strftime('%Y%m%d'):
        
        race_date = from_date
        
        DownloadFile()
        print(file_dir + file_name_txt)
        
        if os.path.isfile(file_dir + file_name_txt) == False:
            from_date = (dt.strptime(from_date, '%Y%m%d') + datetime.timedelta(days=1)).strftime('%Y%m%d')
            print(str(race_date) + ' pass')
            continue
        
        main()
        
        from_date = (dt.strptime(from_date, '%Y%m%d') + datetime.timedelta(days=1)).strftime('%Y%m%d')
        
        print(str(race_date) + ' complete')
    
    print('ALL complete')
    
