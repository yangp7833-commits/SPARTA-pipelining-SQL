#!/home/codespace/.python/current/bin/python3
import sqlite3
import re

class DBManager:
    def __init__(self, db_path='SQL.db'):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        

    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS experimental_data
                            (experiment_id INTEGER PRIMARY KEY AUTOINCREMENT, tool TEXT, date TEXT, file TEXT, experiment_name TEXT, comparison_label TEXT)''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS gene_results
                            (id INTEGER PRIMARY KEY AUTOINCREMENT,
                            experiment_id INTEGER,
                            gene_name TEXT,
                            log2fc REAL,
                            logCPM REAL,
                            pvalue REAL,
                            padj REAL,
                            other_info TEXT,
                            FOREIGN KEY (experiment_id) REFERENCES experimental_data(id) ON DELETE CASCADE)
                            ''')

        self.conn.commit()

    def create_experiment(self, tool, date, file_path):
        experiment_name=input('Enter a name for this experiment: ')
        comparison_label=input('Enter a comparison label for this experiment: ')
        self.cursor.execute("INSERT INTO experimental_data (tool, date, file, experiment_name, comparison_label) VALUES (?, ?, ?, ?, ?);", (tool, date, file_path, experiment_name, comparison_label))
        return self.cursor.lastrowid

    def insert_gene_results(self, info, JSON_headers, id):
        query = "INSERT INTO gene_results (experiment_id, gene_name, log2fc, logCPM, pvalue, padj, other_info) VALUES (?, ?, ?, ?, ?, ?, ?);"
        for row in info:
            self.cursor.execute(query, (id, row['Gene'], row['log2FoldChange'], row['logCPM'], row['pvalue'], row['padj'], str({h: row[h] for h in JSON_headers})))
        self.conn.commit()

    def close(self):
        if self.conn:
            self.conn.close()
            

    def list_experiments(self, query, values=None):
        if values is None:
            results=self.cursor.execute(query).fetchall()
        else: 
            results=self.cursor.execute(query, tuple(values)).fetchall()
        
        try:
            print("\n--- Registered Experiments ---")
            for result in results:
                print(f"Experiment ID: {result[0]} | tool: {result[1]} | date: {result[2]} | file: {result[3]} | experiment_name: {result[4]} | comparison_label: {result[5]}")
        except IndexError:
            print("No experiments found matching the query.")

        
    
    def list_gene_results(self, query, values=None):
        if values is None:
            results=self.cursor.execute(query).fetchall()
        else: 
            results=self.cursor.execute(query, tuple(values)).fetchall()
        
        try:
            print("\n--- Registered Gene Results ---")
            for result in results:
                print(f'Experiment ID: {result[1]} | gene_name: {result[2]} | log2fc: {result[3]} | logCPM: {result[4]} | pvalue: {result[5]} | padj: {result[6]}')
        except IndexError:
            print("No gene results found matching the query.")
                
        

      
    