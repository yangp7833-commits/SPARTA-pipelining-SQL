#!/home/codespace/.python/current/bin/python3 

from db_manager import DBManager
import re
import viz
with DBManager() as db:
   #db.insert_to_database('data.txt')
   db.delete_experiments(experiment_name='t')
   df=db.query('gene_results')
   print(df)
   
    
