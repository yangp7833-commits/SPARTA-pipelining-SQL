#!/home/codespace/.python/current/bin/python3 

from db_manager import DBManager
import re
import viz
with DBManager() as db:
   #db.delete(table='experimental_data', experiment_id__lt=2)
   db.insert_to_database('data.txt')
   
    
