#!/home/codespace/.python/current/bin/python3 

from db_manager import DBManager
import re
import viz
with DBManager() as db:
   df=db.preprocess_df('data.txt', 1)
   df2=db.preprocess_df(df, 1)
   print(df2)
   
    
