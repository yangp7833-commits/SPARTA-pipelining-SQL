#!/home/codespace/.python/current/bin/python3
import csv
import re
import sqlite3
import os
from pathlib import Path
import pandas as pd
import json

# finds the date of the file path, is activated by the export_data function
def find_date(file_path):
    date_pattern = re.compile(r'(\d{4}-\d{2}-\d{2})') 
    match = date_pattern.search(file_path) # first tries to find the date using regex on the file name
    if match:
        return match.group(1)
    else: # if the search doesn't work, then uses the path library to find the time, or else uses none
        path=Path(file_path)
        return path.stat().st_mtime if path.stat().st_mtime else None

# parses the csv files, is activated by the export_data function
def parse_csv_files(file_path):
    file_path = os.path.abspath(file_path)

    # 1. Check if file exists and is not empty before processing
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        print(f"Error: File {file_path} is empty or missing.")
        return None

    clean_headers = {
        'log2FoldChange': ['log2fc', 'log2fold', 'log2foldchange', 'logfc'], 
        'pvalue': ['p-value', 'pvalue'], 
        'padj': ['padj', 'false discovery rate', 'fdr'], 
        'Gene': ['gene'], 
        'logCPM': ['logcpm', 'basemean']
    }

    try:
        # 2. Use engine='python' to help the sniffer avoid 'NoneType' errors
        df = pd.read_csv(file_path, sep=None, engine='python')
    except Exception as e:
        print(f"Failed to parse CSV: {e}")
        return None

    rename_dict = {original: standard for original in df.columns for standard, variants in clean_headers.items() if original.lower() in variants}

    

    # Identify extra columns
    extra_columns = [col for col in df.columns if col not in rename_dict]

    # Rename matching columns
    df.rename(columns=rename_dict, inplace=True)

    # Handle extra info
    if extra_columns:
        # Convert extra columns to a single JSON column
        df['extra_info'] = df[extra_columns].apply(json.dumps, axis=1)
        # Drop original extra columns
        df = df.drop(columns=extra_columns)
    else:
        df['extra_info'] = None

    return df

def export_data(file_path):
    df=parse_csv_files(file_path) # calls the parse_csv_files function and gets the main info
    date=find_date(file_path) # uses find_date function to find dates
    
    return df.to_dict('records'), date, file_path # exports these to the DB manager

parse_csv_files("RNAseq_Data/2024-03-29/DEanalysis/all_genes_DGE")
    
            
            
    

    
            
   


        