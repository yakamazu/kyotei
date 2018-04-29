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

download_url = config.get('file', 'download_url')
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
    
    file_name_lzh = 'k' + race_date[2:8] + '.lzh'
    file_name_txt = 'k' + race_date[2:8] + '.txt'

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
    cmd = "rm %s" % (file_dir + 'k*')
    returncode = subprocess.call(cmd, shell=True)

    #remove encoding file
    cmd = "rm %s" % (file_dir + file_name_race_result)
    returncode = subprocess.call(cmd, shell=True)

#-----------------------------
#function for set raceinfo
#-----------------------------
def SetRaceResult():
    race_result = pd.DataFrame(columns=['race_date', 'kaijo', 'race_no', 'distance', 'weather', 'wind_direction', 'wind_volume', 'wave', 'rank', 'entry_order', 'toroku_no', 'motor_no', 'boat_no', 'tenji'])
    race_result_odds = pd.DataFrame(columns=['race_date', 'kaijo', 'race_no', 'tansyo', 'fukusyo_1', 'fukusyo_2', 'rentan_2', 'renpuku_2', 'rentan_3', 'renpuku_3'])
    #debug abc=1
    with open(file_dir + file_name_race_result, 'r') as f:
        for line in f:
            '''
            set race_result variable
                date
                kaijo
                race_no
                distance
                weather
                wind_direction
                wind_volume
                wave
                rank
                entry_order
                toroku_no
                motor_no
                boat_no
                tenji
                
            set race_result_odds variable
                tansyo
                fukusyo_1
                fukusyo_2
                rentan_2
                renpuku_2
                rentan_3
                renpuku_3
            ''' 
            #debug print(str(abc) + ' ' + str(len(line)))
            #debug abc += 1
            if line.find('BGN') > -1:
                kaijo = line[0:2]
            
            if line[4:5]=='R':
                race_no = int(line[2:4])
                distance = int(line[line.find('H')+1:line.find('H')+5])
                weather = line[line.find('H')+7:line.find('H')+10].strip()
                wind_direction = line[line.find('H')+15:line.find('H')+18].strip()
                wind_volume = int(line[line.find('H')+19:line.find('H')+20])
                wave = int(line[line.find('H')+25:line.find('H')+28])
            
            if len(line) == 59 and line[2:3] == '0':
                rank = int(line[2:4])
                entry_order = int(line[6:7])
                toroku_no = int(line[8:12])
                motor_no = int(line[22:24])
                boat_no = int(line[27:29])
                tenji = float(line[30:35])
                
                #set race_result dataframe 
                race_result_tmp = pd.DataFrame([[race_date, kaijo, race_no, distance, weather, wind_direction, wind_volume, wave, rank, entry_order, toroku_no, motor_no, boat_no, tenji]], columns=['race_date', 'kaijo', 'race_no', 'distance', 'weather', 'wind_direction', 'wind_volume', 'wave', 'rank', 'entry_order', 'toroku_no', 'motor_no', 'boat_no', 'tenji'])
                race_result = race_result.append(race_result_tmp)
                
            if line.find('単勝') > -1:
                if line.find('特払') > -1:
                    tansyo = int(line[23:30])
                else:
                    if line[20:30].strip().isdigit() == True:
                        tansyo = int(line[20:30])
                    else:
                        tansyo = 100
            if line.find('複勝') > -1:
                if line[20:30].strip().isdigit() == True:
                    fukusyo_1 = int(line[20:30])
                else:
                    fukusyo_1 = 100
                if line[36:45].strip().isdigit() == True:
                    fukusyo_2 = int(line[36:45])
                else:
                    fukusyo_2 = 100
            if line.find('２連単') > -1 and line.find('人気') > -1:
                rentan_2 = int(line[20:30])
            if line.find('２連複') > -1 and line.find('人気') > -1:
                renpuku_2 = int(line[20:30])
            if line.find('３連単') > -1 and line.find('人気') > -1:
                rentan_3 = int(line[20:30])
            if line.find('３連複') > -1 and line.find('人気') > -1:
                renpuku_3 = int(line[20:30])
                
                #set race_result_odds dataframe
                race_result_odds_tmp = pd.DataFrame([[race_date, kaijo, race_no, tansyo, fukusyo_1, fukusyo_2, rentan_2, renpuku_2, rentan_3, renpuku_3]], columns=['race_date', 'kaijo', 'race_no', 'tansyo', 'fukusyo_1', 'fukusyo_2', 'rentan_2', 'renpuku_2', 'rentan_3', 'renpuku_3'])
                race_result_odds = race_result_odds.append(race_result_odds_tmp)
                
    return race_result, race_result_odds
 
#-----------------------------
#functino for postgresql
#-----------------------------    
def Pands2Postgre(race_result, race_result_odds):
    engine=create_engine(db_connect_info)

    race_result.to_sql("wk_race_result", engine, if_exists="replace", index=False)
    race_result_odds.to_sql("wk_race_result_odds", engine, if_exists="replace", index=False)
    
def ExecuteSql(sqltext):
    conn = psycopg2.connect("host=" + db_host + " port=" + db_port + " dbname=" + db_dbname + " user=" + db_user + " password=" + db_password)
    cur = conn.cursor()

    with open(sqltext,'r') as f:
        delete_race_result = f.read()
    
        cur.execute(delete_race_result)
        
        conn.commit()
        cur.close()
        conn.close()
    

#-----------------------------
#function for main
#-----------------------------
def main():
    
    #race result to pandas
    race_result, race_result_odds = SetRaceResult()
    
    #from pandas to postgresql (copy)
    Pands2Postgre(race_result, race_result_odds)
    
    #insert race_result
    ExecuteSql('../sql/delete_race_result.sql')
    ExecuteSql('../sql/insert_race_result.sql')
    
    #insert race_result_odds
    ExecuteSql('../sql/delete_race_result_odds.sql')
    ExecuteSql('../sql/insert_race_result_odds.sql')
    
    #clean file
    CleanFile()
    
    return race_result, race_result_odds

    
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
    



