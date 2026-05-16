#!/home/codespace/.python/current/bin/python3 

from db_manager import DBManager
import re
import viz
with DBManager() as db:
   df=db.query(table='gene_results', gene_symbol='TP5', chromosome='chr17', experimet_id=4)
   print(df)
   
    
