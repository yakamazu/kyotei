import pandas as pd
import psycopg2
import configparser

#variable
config = configparser.ConfigParser()
config.read('../config/kyotei.config')

db_host = config.get('db', 'db_host')
db_port = config.get('db', 'db_port')
db_dbname = config.get('db', 'db_dbname')
db_user = config.get('db', 'db_user')
db_password = config.get('db', 'db_password')

# connection_info
connection_config = {
    'host': db_host,
    'port': db_port,
    'database': db_dbname,
    'user': db_user,
    'password': db_password
}

# connection
connection = psycopg2.connect(**connection_config)
dataframe = pd.read_sql(sql="""
                        select
                        a.kaijo
                        ,a.race_no
                        ,a.player_class_1
                        ,a.player_class_2
                        ,a.player_class_3
                        ,a.player_class_4
                        ,a.player_class_5
                        ,a.player_class_6
                        ,case when rank_1 = 1 then 0 else 1 end as case
                        ,case when rank_1 = 1 then 1
                              when rank_2 = 1 then 2
                              when rank_3 = 1 then 3
                              when rank_4 = 1 then 4
                              when rank_5 = 1 then 5
                              when rank_6 = 1 then 6
                              else 0 end as race_rank
                        ,case when rank_1 = 2 then 1
                              when rank_2 = 2 then 2
                              when rank_3 = 2 then 3
                              when rank_4 = 2 then 4
                              when rank_5 = 2 then 5
                              when rank_6 = 2 then 6
                              else 0 end as race_rank_2
                        ,case when rank_1 = 3 then 1
                              when rank_2 = 3 then 2
                              when rank_3 = 3 then 3
                              when rank_4 = 3 then 4
                              when rank_5 = 3 then 5
                              when rank_6 = 3 then 6
                              else 0 end as race_rank_3
                        from public.v_race_result_2 a 
                        ;""", con=connection)

# dummy create
#dataframe.T
df_kaijo = pd.get_dummies(dataframe['kaijo'], prefix='kaijo')
df_race_no = pd.get_dummies(dataframe['race_no'], prefix='race_no')
df_toroku_1 = pd.get_dummies(dataframe['player_class_1'], prefix='player_class_1')
df_toroku_2 = pd.get_dummies(dataframe['player_class_2'], prefix='player_class_2')
df_toroku_3 = pd.get_dummies(dataframe['player_class_3'], prefix='player_class_3')
df_toroku_4 = pd.get_dummies(dataframe['player_class_4'], prefix='player_class_4')
df_toroku_5 = pd.get_dummies(dataframe['player_class_5'], prefix='player_class_5')
df_toroku_6 = pd.get_dummies(dataframe['player_class_6'], prefix='player_class_6')

# dummy join
dataframe_dummy_add = dataframe.join(df_toroku_1).join(df_toroku_2).join(df_toroku_3).join(df_toroku_4).join(df_toroku_5).join(df_toroku_6).join(df_kaijo).join(df_race_no)

# create x and y
dataframe_x = dataframe_dummy_add.drop(["race_no", "kaijo", "player_class_1", "player_class_2", "player_class_3", "player_class_4", "player_class_5", "player_class_6", "case", "race_rank", "race_rank_2", "race_rank_3"], axis=1)
dataframe_y_1 = dataframe["race_rank"]
dataframe_y_2 = dataframe["race_rank_2"]
dataframe_y_3 = dataframe["race_rank_3"]
#dataframe_y = np.array([[dataframe["race_rank"]], [dataframe["race_rank_2"]], [dataframe["race_rank_3"]]]).T

#ロジスティック回帰
from sklearn.linear_model import LogisticRegression

#インスタンス作成
lr_1 = LogisticRegression(C=1000.0, random_state=0)
lr_2 = LogisticRegression(C=1000.0, random_state=0)
lr_3 = LogisticRegression(C=1000.0, random_state=0)

#トレーニングデータのモデル作成
lr_1.fit(dataframe_x, dataframe_y_1)
lr_2.fit(dataframe_x, dataframe_y_2)
lr_3.fit(dataframe_x, dataframe_y_3)

#ランダムフォレスト
from sklearn.ensemble import RandomForestClassifier

#インスタンス作成
rf_1 = RandomForestClassifier(criterion='entropy', n_estimators=10, random_state=1, n_jobs=2)
rf_2 = RandomForestClassifier(criterion='entropy', n_estimators=10, random_state=1, n_jobs=2)
rf_3 = RandomForestClassifier(criterion='entropy', n_estimators=10, random_state=1, n_jobs=2)

#ランダムフォレストのモデルにトレーニングデータを適合させる
rf_1.fit(dataframe_x, dataframe_y_1)
rf_2.fit(dataframe_x, dataframe_y_2)
rf_3.fit(dataframe_x, dataframe_y_3)

#シリアライズ
from sklearn.externals import joblib

joblib.dump(rf_1, 'rf_1.pkl')
joblib.dump(rf_2, 'rf_2.pkl')
joblib.dump(rf_3, 'rf_3.pkl')

