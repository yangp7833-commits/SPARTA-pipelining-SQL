#!/home/codespace/.python/current/bin/python3 

from db_manager import DBManager
import parser
import re
with DBManager() as db:
    db.insert_to_database(parser.parse_csv_files('RNAseq_Data/2024-03-29/DEanalysis/all_genes_DGE'), tool='SPARTA', experiment_name='test', comparison_label='test')
    db.query(table='gene_results', save_path='test.csv')
    
