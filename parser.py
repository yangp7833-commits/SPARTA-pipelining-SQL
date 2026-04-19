import csv
import re
import sqlite3
import os
from contextlib import closing

class parser:

    def __init__(self, file_path=None):
        self.file_path = file_path

    def find_date(self):
        date_pattern = re.compile(r'(\d{4}-\d{2}-\d{2})')
        match = date_pattern.search(self.file_path)
        if match:
            return match.group(1)
        else:
            path=Path(self.file_path)
            return path.stat().st_mtime

    def parse_csv_files(self):
        clean_headers = {'log2FoldChange': ['log2fc', 'log2fold', 'log2foldchange', 'logfc'], 
                        'pvalue': ['p-value', 'pvalue'], 'padj':['padj', 'false discovery rate', 'fdr'], 
                        'Gene':['gene'], 'logCPM':['logcpm', 'basemean']}
        with open(self.file_path, 'r') as f:
            
            reader=csv.DictReader(f, delimiter='\t')
            JSON_headers=[]
            
            for header in reader.fieldnames:
                success=False
                for key, variants in clean_headers.items():
                    if header.lower().strip() in variants:
                        reader.fieldnames[reader.fieldnames.index(header)] = key
                        success=True
                        break
                if success==False:
                    print(f"Unrecognized header '{header}' converted to JSON format.")
                    JSON_headers.append(header)
            reader=csv.DictReader(f, delimiter='\t', fieldnames=reader.fieldnames)
                
            return list(reader), JSON_headers
    
    def export_data(self):
        reader, JSON_headers=self.parse_csv_files()
        date=self.find_date()
        
        return reader, date, JSON_headers, self.file_path
    
            
            
    

    
            
   


        