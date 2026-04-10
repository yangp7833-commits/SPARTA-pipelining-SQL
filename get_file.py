#!/home/codespace/.python/current/bin/python3
import os
import re
from parser import SPARTA_parser
import glob
from pathlib import Path
#from db_manager import DBmanager

class finder:

    def __init__(self, start_dir):
        self.start_dir=os.path.abspath(start_dir)


    
        
    def identify_tool(self):
        tool_patterns = {
            "RNAseq_data": "SPARTA",
            "salmon.merged.gene_counts.tsv": "NF-CORE",
            "res_ordered.csv": "DESEQ2",
            "gene_exp.diff": "CUFFDIFF",
            "quant.sf": "SALMON"
        }
        for pattern, tool in tool_patterns.items():
            if pattern.lower() in self.start_dir.lower():
                print(f"Identified tool: {tool}")
                self.tool=tool
        
    def parse(self):
        if self.tool == "SPARTA":
            info, date, JSON_headers, file_path = self.parse_SPARTA()
            return info, date, JSON_headers, file_path
        else:
            print("No parsing method available for the identified tool.")


    def parse_SPARTA(self):
        # Find all the DGE result files in any subfolder
        if not os.path.exists(self.start_dir):
            print(f"Directory '{self.start_dir}' does not exist.")
            return
        else:
            pattern = os.path.join(self.start_dir, '**', 'all_genes_DGE')
            dge_files = glob.glob(pattern, recursive=True)
        try:
            print(dge_files[0])
            for file_path in dge_files:
                print(f"Processing SPARTA run: {file_path}")  
                parser = SPARTA_parser(file_path) 
            
                info, date, JSON_headers, file_path = parser.export_data(file_path)
                return info, date, JSON_headers, file_path

            
    
                
        except IndexError:
            print("No DEanalysis file found in the directory.")
    
    
        


    
    
        

    






