import csv
import re
import sqlite3
import os
from contextlib import closing
from pathlib import Path

class parser:

    # the file path is provided by the user, and is passed from the get_file.py to here
    def __init__(self, file_path=None):
        self.file_path = file_path

    # finds the date of the file path, is activated by the export_data function
    def find_date(self):
        date_pattern = re.compile(r'(\d{4}-\d{2}-\d{2})') 
        match = date_pattern.search(self.file_path) # first tries to find the date using regex on the file name
        if match:
            return match.group(1)
        else: # if the search doesn't work, then uses the path library to find the time, or else uses none
            path=Path(self.file_path)
            return path.stat().st_mtime if path.stat().st_mtime else None

    # function that parses the csv files
    def parse_csv_files(self):
        clean_headers = {'log2FoldChange': ['log2fc', 'log2fold', 'log2foldchange', 'logfc'], 
                        'pvalue': ['p-value', 'pvalue'], 'padj':['padj', 'false discovery rate', 'fdr'], 
                        'Gene':['gene'], 'logCPM':['logcpm', 'basemean']} # dictionary to clean headers
        with open(self.file_path, 'r') as f:
            
            reader=csv.DictReader(f, delimiter='\t') # reads the CSV the first time in order to find the headers
            JSON_headers=[] #JSON_headers are for extra columns that aren't in the SQL database
            
            for header in reader.fieldnames: # for each column in the file, clean it into the appropriate column
                success=False # we detect a match in the columns this way
                for key, variants in clean_headers.items(): 
                    if header.lower().strip() in variants: #checks if the header in the csv matches any of the SQL column variants
                        reader.fieldnames[reader.fieldnames.index(header)] = key # if so, matches it to our SQL database
                        success=True
                        break
                if success==False:
                    print(f"Unrecognized header '{header}' converted to JSON format.") # if none of them match, the header is converted to JSON in the extra info column
                    JSON_headers.append(header)
            reader=csv.DictReader(f, delimiter='\t', fieldnames=reader.fieldnames) # reads the new CSV
                
            return list(reader), JSON_headers # returns the iterable along with the JSON headers
    
    def export_data(self):
        reader, JSON_headers=self.parse_csv_files() # calls the parse_csv_files function and gets the main info
        date=self.find_date() # uses find_date function to find dates
        
        return reader, date, JSON_headers, self.file_path # exports these to the DB manager
    
            
            
    

    
            
   


        