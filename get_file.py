#!/home/codespace/.python/current/bin/python3
import os
import re
from parser import parser
import glob
from pathlib import Path
#from db_manager import DBmanager

class finder:

    def __init__(self, start_dir):
        self.start_dir=os.path.abspath(start_dir)


    
        
    def parse(self):
        parse=parser(self.start_dir) 
        info, date, JSON_headers, file_path= parse.export_data()
        return info, date, JSON_headers, file_path, "SPARTA"

            
    
                
        
    
    
        


    
    
        

    






