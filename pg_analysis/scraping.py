# coding: UTF-8
import requests
from bs4 import BeautifulSoup
import lxml.html
from urllib.parse import urljoin
import re
import time
from pymongo import MongoClient
from prettytable import PrettyTable
from sklearn.externals import joblib
import configparser
import datetime
import pandas as pd

def scrape_list_page(url, base_url):
  r = requests.get(url)
  soup = BeautifulSoup(r.text, "html.parser")

  for ul in soup.find_all('ul', class_="textLinks3"):
    for a in ul.find_all('a'):        
      if a.get('href').find('racelist') > -1:
        url_racelist = urljoin(base_url, a.get('href'))
        yield url_racelist

def scrape_detail_page(list_page):
  #list_page='https://www.boatrace.jp/owpc/pc/race/racelist?rno=12&jcd=01&hd=20180428'
  #race_list
  r = requests.get(list_page)
  soup = BeautifulSoup(r.text, "html.parser")
  
  #odds
  odds_page = list_page.replace('racelist', 'oddstf')
  r_odds = requests.get(odds_page)
  soup_odds = BeautifulSoup(r_odds.text, "html.parser")
  
  #shime_time
  shime_time = []

  for td in soup.find_all('td'):
    if len(td.text) == 5 and td.text.find(':') > -1:
      shime_time.append(td.text)
  
  #race_date and race_no and kaijo
  race_date = list_page[list_page.find('hd=')+3:]
  race_no = list_page[list_page.find('rno=')+4:list_page.find('&jcd')]
  kaijo = list_page[list_page.find('jcd=')+4:list_page.find('&hd')]
  kaijo_nm = soup.find('img', height=45).get('alt')

  #toroku_no & player_class
  toroku_no=[]
  player_class=[]

  for a in soup.find_all('div', class_="is-fs11"):
    if len(a.text.strip().replace(' ','').replace('\r\n','')) == 7:
      toroku_no.append(re.sub("/.*", "", a.text.strip().replace(' ','').replace('\r\n','')))
      player_class.append(re.sub("^.*/", "", a.text.strip().replace(' ','').replace('\r\n','')))
      
  #odds
  tansyo_odds=[]
  fukusyo_odds_min=[]
  fukusyo_odds_max=[]
  
  i = 1

  if soup_odds.find('td', class_="oddsPoint is-miss is-fColor1") is None and soup_odds.find('td', class_="oddsPoint") is not None:
    for odds in soup_odds.find_all('td', class_="oddsPoint "):
      if i <= 6:
        tansyo_odds.append(odds.text)
      else: 
        fukusyo_odds_min.append(re.sub("-.*", "", odds.text))
        fukusyo_odds_max.append(re.sub("^.*-", "", odds.text))
      i += 1
  else:
    tansyo_odds = [0,0,0,0,0,0]
    fukusyo_odds_min = [0,0,0,0,0,0]
    fukusyo_odds_max = [0,0,0,0,0,0]

  #set race_info
  race_info ={
      'race_date' : race_date,
      'kaijo' : kaijo,
      'race_no' : race_no,
      'kaijo_nm' : kaijo_nm,
      'shime_time' : shime_time[int(race_no) - 1],
      'toroku_no_1' : toroku_no[0],
      'player_class_1' : player_class[0],
      'tansyo_odds_1' : tansyo_odds[0],
      'fukusyo_odds_min_1' : fukusyo_odds_min[0],
      'fukusyo_odds_max_1' : fukusyo_odds_max[0],
      'toroku_no_2' : toroku_no[1],
      'player_class_2' : player_class[1],
      'tansyo_odds_2' : tansyo_odds[1],
      'fukusyo_odds_min_2' : fukusyo_odds_min[1],
      'fukusyo_odds_max_2' : fukusyo_odds_max[1],
      'toroku_no_3' : toroku_no[2],
      'player_class_3' : player_class[2],
      'tansyo_odds_3' : tansyo_odds[2],
      'fukusyo_odds_min_3' : fukusyo_odds_min[2],
      'fukusyo_odds_max_3' : fukusyo_odds_max[2],
      'toroku_no_4' : toroku_no[3],
      'player_class_4' : player_class[3],
      'tansyo_odds_4' : tansyo_odds[3],
      'fukusyo_odds_min_4' : fukusyo_odds_min[3],
      'fukusyo_odds_max_4' : fukusyo_odds_max[3],
      'toroku_no_5' : toroku_no[4],
      'player_class_5' : player_class[4],
      'tansyo_odds_5' : tansyo_odds[4],
      'fukusyo_odds_min_5' : fukusyo_odds_min[4],
      'fukusyo_odds_max_5' : fukusyo_odds_max[4],
      'toroku_no_6' : toroku_no[5],
      'player_class_6' : player_class[5],
      'tansyo_odds_6' : tansyo_odds[5],
      'fukusyo_odds_min_6' : fukusyo_odds_min[5],
      'fukusyo_odds_max_6' : fukusyo_odds_max[5]
  }
  
  #print(shime_time)
  #print(race_no)
  #print(kaijo)
  #print(toroku_no)
  #print(player_class)
  #print(tansyo_odds)
  #print(fukusyo_odds_min)
  #print(fukusyo_odds_max)
  #print(race_info)
  return race_info

def scrape_result_page(list_page):
  #result
  result_page = list_page.replace('racelist', 'raceresult')
  r_result = requests.get(result_page)
  soup_result = BeautifulSoup(r_result.text, "html.parser")

  race_date = list_page[list_page.find('hd=')+3:]
  race_no = list_page[list_page.find('rno=')+4:list_page.find('&jcd')]
  kaijo = list_page[list_page.find('jcd=')+4:list_page.find('&hd')]
  kaijo_nm = soup.find('img', height=45).get('alt')

  #set rank
  rank_1 = 0
  rank_2 = 0
  rank_3 = 0

  i=0

  for rank in soup_result.find_all('td', class_="is-fBold"):
    if i == 1:
      rank_1 = rank.text
    elif i == 2:
      rank_2 = rank.text
    elif i == 3:
      rank_3 = rank.text
    i+=1

  #set odds result
  odds_result=[]
  
  for a in soup_result.find_all('span', class_="is-payout1"):
    odds_result.append(a.text.replace('¥', '').replace(',', ''))

  rentan_3 = odds_result[0]
  renpuku_3 = odds_result[2]
  rentan_2 = odds_result[4]
  renpuku_2 = odds_result[6]
  tansyo = odds_result[13]
  fukusyo_1 = odds_result[15]
  fukusyo_2 = odds_result[16]

  #set race_result
  race_result ={
      'race_date' : race_date,
      'kaijo' : kaijo,
      'race_no' : race_no,
      'kaijo_nm' : kaijo_nm,
      'rank_1' : rank_1,
      'rank_2' : rank_2,
      'rank_3' : rank_3,
      'rentan_3' : rentan_3,
      'renpuku_3' : renpuku_3,
      'rentan_2' : rentan_2,
      'renpuku_2' : renpuku_2,
      'tansyo' : tansyo,
      'fukusyo_1' : fukusyo_1,
      'fukusyo_2' : fukusyo_2
  }

  return race_result

def deserialize_model(race_info):
  #deserialize model
  rf_1 = joblib.load('rf_1.pkl') 
  rf_2 = joblib.load('rf_2.pkl') 
  rf_3 = joblib.load('rf_3.pkl') 
  
  dataframe_dummy_add = pd.DataFrame(columns=['kaijo', 'race_no', 'player_class_1', 'player_class_2',
       'player_class_3', 'player_class_4', 'player_class_5', 'player_class_6',
       'case', 'race_rank', 'race_rank_2', 'race_rank_3', 'player_class_1_A1',
       'player_class_1_A2', 'player_class_1_B1', 'player_class_1_B2',
       'player_class_2_A1', 'player_class_2_A2', 'player_class_2_B1',
       'player_class_2_B2', 'player_class_3_A1', 'player_class_3_A2',
       'player_class_3_B1', 'player_class_3_B2', 'player_class_4_A1',
       'player_class_4_A2', 'player_class_4_B1', 'player_class_4_B2',
       'player_class_5_A1', 'player_class_5_A2', 'player_class_5_B1',
       'player_class_5_B2', 'player_class_6_A1', 'player_class_6_A2',
       'player_class_6_B1', 'player_class_6_B2', 'kaijo_01', 'kaijo_02',
       'kaijo_03', 'kaijo_04', 'kaijo_05', 'kaijo_06', 'kaijo_07', 'kaijo_08',
       'kaijo_09', 'kaijo_10', 'kaijo_11', 'kaijo_12', 'kaijo_13', 'kaijo_14',
       'kaijo_15', 'kaijo_16', 'kaijo_17', 'kaijo_18', 'kaijo_19', 'kaijo_20',
       'kaijo_21', 'kaijo_22', 'kaijo_23', 'kaijo_24', 'race_no_1',
       'race_no_2', 'race_no_3', 'race_no_4', 'race_no_5', 'race_no_6',
       'race_no_7', 'race_no_8', 'race_no_9', 'race_no_10', 'race_no_11',
       'race_no_12'])
  
  col_list=['kaijo', 'race_no', 'player_class_1', 'player_class_2', 'player_class_3', 'player_class_4', 'player_class_5', 'player_class_6']
  df_tmp = pd.DataFrame(list(race_info.items()), index=race_info.keys()).T
  df = df_tmp.drop(0).loc[:,col_list]
  
  # dummy create
  df_kaijo = pd.get_dummies(df['kaijo'], prefix='kaijo')
  df_race_no = pd.get_dummies(df['race_no'], prefix='race_no')
  df_toroku_1 = pd.get_dummies(df['player_class_1'], prefix='player_class_1')
  df_toroku_2 = pd.get_dummies(df['player_class_2'], prefix='player_class_2')
  df_toroku_3 = pd.get_dummies(df['player_class_3'], prefix='player_class_3')
  df_toroku_4 = pd.get_dummies(df['player_class_4'], prefix='player_class_4')
  df_toroku_5 = pd.get_dummies(df['player_class_5'], prefix='player_class_5')
  df_toroku_6 = pd.get_dummies(df['player_class_6'], prefix='player_class_6')

  dataframe_dummy_add_tmp = df.join(df_toroku_1).join(df_toroku_2).join(df_toroku_3).join(df_toroku_4).join(df_toroku_5).join(df_toroku_6).join(df_kaijo).join(df_race_no)
  dataframe_dummy_add = dataframe_dummy_add.append(dataframe_dummy_add_tmp)
  dataframe_dummy_add = dataframe_dummy_add.fillna(0)

  dataframe_x = dataframe_dummy_add.drop(["kaijo", "race_no", "player_class_1", "player_class_2", "player_class_3", "player_class_4", "player_class_5", "player_class_6", "case", "race_rank", "race_rank_2", "race_rank_3"], axis=1)
  
  return rf_1.predict_proba(dataframe_x), rf_2.predict_proba(dataframe_x), rf_3.predict_proba(dataframe_x)


def main_predict():
  #config
  config = configparser.ConfigParser()
  config.read('../config/kyotei.config') 
  
  #url setting
  url = config.get('scrape', 'url')
  base_url = config.get('scrape', 'base_url')
  
  #mongodb setting
  mongo_host = config.get('mongo', 'mongo_host')
  mongo_port = config.get('mongo', 'mongo_port')

  client = MongoClient(mongo_host, 27017)
  collection = client.scraping.race_info
  
  #time
  #print((datetime.datetime.now()+datetime.timedelta(hours=9)).strftime("%Y/%m/%d %H:%M:%S"))
  
  try:
    url_list = scrape_list_page(url, base_url)
  
    for list_page in url_list:
      race_info = scrape_detail_page(list_page)
      
      rf_1, rf_2, rf_3 = deserialize_model(race_info)
      
      race_info.update(rank1_prob_1=float(rf_1[:,0].round(2)),rank1_prob_2=float(rf_1[:,1].round(2)),rank1_prob_3=float(rf_1[:,2].round(2)),rank1_prob_4=float(rf_1[:,3].round(2)),rank1_prob_5=float(rf_1[:,4].round(2)),rank1_prob_6=float(rf_1[:,5].round(2)),
                       rank2_prob_1=float(rf_2[:,0].round(2)),rank2_prob_2=float(rf_2[:,1].round(2)),rank2_prob_3=float(rf_2[:,2].round(2)),rank2_prob_4=float(rf_2[:,3].round(2)),rank2_prob_5=float(rf_2[:,4].round(2)),rank2_prob_6=float(rf_2[:,5].round(2)),
                       rank3_prob_1=float(rf_3[:,0].round(2)),rank3_prob_2=float(rf_3[:,1].round(2)),rank3_prob_3=float(rf_3[:,2].round(2)),rank3_prob_4=float(rf_3[:,3].round(2)),rank3_prob_5=float(rf_3[:,4].round(2)),rank3_prob_6=float(rf_3[:,5].round(2)),
                       rank_prob_12sum_1=float(rf_1[:,0].round(2))+float(rf_2[:,0].round(2)),rank_prob_12sum_2=float(rf_1[:,1].round(2))+float(rf_2[:,1].round(2)),rank_prob_12sum_3=float(rf_1[:,2].round(2))+float(rf_2[:,2].round(2)),rank_prob_12sum_4=float(rf_1[:,3].round(2))+float(rf_2[:,3].round(2)),rank_prob_12sum_5=float(rf_1[:,4].round(2))+float(rf_2[:,4].round(2)),rank_prob_12sum_6=float(rf_1[:,5].round(2))+float(rf_2[:,5].round(2)),
                       rank_prob_sum_1=float(rf_1[:,0].round(2))+float(rf_2[:,0].round(2))+float(rf_3[:,0].round(2)),rank_prob_sum_2=float(rf_1[:,1].round(2))+float(rf_2[:,1].round(2))+float(rf_3[:,1].round(2)),rank_prob_sum_3=float(rf_1[:,2].round(2))+float(rf_2[:,2].round(2))+float(rf_3[:,2].round(2)),rank_prob_sum_4=float(rf_1[:,3].round(2))+float(rf_2[:,3].round(2))+float(rf_3[:,3].round(2)),rank_prob_sum_5=float(rf_1[:,4].round(2))+float(rf_2[:,4].round(2))+float(rf_3[:,4].round(2)),rank_prob_sum_6=float(rf_1[:,5].round(2))+float(rf_2[:,5].round(2))+float(rf_3[:,5].round(2)),
                       tansyo_exp_1=float(race_info['tansyo_odds_1']) * 100 * float(rf_1[:,0].round(2)),tansyo_exp_2=float(race_info['tansyo_odds_2']) * 100 * float(rf_1[:,1].round(2)),tansyo_exp_3=float(race_info['tansyo_odds_3']) * 100 * float(rf_1[:,2].round(2)),tansyo_exp_4=float(race_info['tansyo_odds_4']) * 100 * float(rf_1[:,3].round(2)),tansyo_exp_5=float(race_info['tansyo_odds_5']) * 100 * float(rf_1[:,4].round(2)),tansyo_exp_6=float(race_info['tansyo_odds_6']) * 100 * float(rf_1[:,5].round(2)),
                       fukusyo_exp_1=(float(race_info['fukusyo_odds_min_1']) + float(race_info['fukusyo_odds_max_1'])) / 2 * 100 * (float(rf_1[:,0].round(2)) + float(rf_2[:,0].round(2))),fukusyo_exp_2=(float(race_info['fukusyo_odds_min_2']) + float(race_info['fukusyo_odds_max_2'])) / 2 * 100 * (float(rf_1[:,1].round(2)) + float(rf_2[:,1].round(2))),fukusyo_exp_3=(float(race_info['fukusyo_odds_min_3']) + float(race_info['fukusyo_odds_max_3'])) / 2 * 100 * (float(rf_1[:,2].round(2)) + float(rf_2[:,2].round(2))),fukusyo_exp_4=(float(race_info['fukusyo_odds_min_4']) + float(race_info['fukusyo_odds_max_4'])) / 2 * 100 * (float(rf_1[:,3].round(2)) + float(rf_2[:,3].round(2))),fukusyo_exp_5=(float(race_info['fukusyo_odds_min_5']) + float(race_info['fukusyo_odds_max_5'])) / 2 * 100 * (float(rf_1[:,4].round(2)) + float(rf_2[:,4].round(2))),fukusyo_exp_6=(float(race_info['fukusyo_odds_min_6']) + float(race_info['fukusyo_odds_max_6'])) / 2 * 100 * (float(rf_1[:,5].round(2)) + float(rf_2[:,5].round(2)))
                      )
      
      collection.delete_one({'race_date' : race_info['race_date'], 'kaijo' : race_info['kaijo'], 'race_no' : race_info['race_no']})
      collection.insert_one(race_info)
      
      time.sleep(1)
  except requests.exceptions.RequestException as ex:
    time.sleep(60)


if __name__ == '__main__':
  
  to_time = (datetime.datetime.now()+datetime.timedelta(hours=9)).strftime("%Y%m%d") + '21'
  
  while int(to_time) >= int((datetime.datetime.now()+datetime.timedelta(hours=9)).strftime("%Y%m%d%H")):
    
    main_predict()
    #main_predict_web()
    
    print((datetime.datetime.now()+datetime.timedelta(hours=9)).strftime("%Y/%m/%d %H:%M:%S"))
    
    time.sleep(180)

