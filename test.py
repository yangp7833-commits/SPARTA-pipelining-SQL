#!/home/codespace/.python/current/bin/python3 

from db_manager import DBManager
import parser
import re
import viz
df=parser.parse_csv_files('data.txt')
print(df)
with DBManager() as db:
   
    
    #db.insert_to_database(df, experiment_name='test', comparison_label='test')
    df=db.query(table='gene_results')
    viz.volcano_plot(df, plot_file='volcano_plot.png')
   
    
