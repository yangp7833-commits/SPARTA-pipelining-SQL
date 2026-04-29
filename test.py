#!/home/codespace/.python/current/bin/python3 

from db_manager import DBManager
import parser
sql=DBManager()
sql.connect()
print(sql.query('gene_results', log2fc__gt=1.5))