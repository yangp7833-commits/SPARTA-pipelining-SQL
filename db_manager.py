#!/home/codespace/.python/current/bin/python3
import sqlite3

class DBManager:
    def __init__(self, db_path='SQL.db'):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        

    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS experimental_data
                            (id INTEGER PRIMARY KEY AUTOINCREMENT, Gene TEXT, file TEXT)''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS conditions
                            (experiment_id INTEGER,
                            sample_name TEXT,
                            condition_group TEXT,
                            FOREIGN KEY (experiment_id) REFERENCES experimental_data(id) ON DELETE CASCADE)''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS gene_results
                            (
                            experiment_id INTEGER,
                            gene_name TEXT,
                            log2fc REAL,
                            logCPM REAL,
                            pvalue REAL,
                            padj REAL,
                            other_info TEXT,
                            FOREIGN KEY (experiment_id) REFERENCES experimental_data(id) ON DELETE CASCADE)''')

        self.conn.commit()

    def create_expiriment(self, experiment_name, file_path):
        self.cursor.execute("INSERT INTO experimental_data (gene, file) VALUES (?, ?);", (experiment_name, file_path))
        return (experiment_name, self.cursor.lastrowid)

    def parse_experimental_data(self, info, file_path):
        ids={}
        for row in info:
            id=self.create_expiriment(row['Gene'], file_path)
            ids[id[0]]=id[1]
            self.conn.commit()
            self.ids=ids
    
    
    def insert_gene_results(self, info, JSON_headers):
        query = "INSERT INTO gene_results (experiment_id, gene_name, log2fc, logCPM, pvalue, padj, other_info) VALUES (?, ?, ?, ?, ?, ?, ?);"
        for row in info:
            if self.ids.get(row['Gene']):
                id=self.ids[row['Gene']]
            else:
                id=None
            self.cursor.execute(query, (id, row['Gene'], row['log2FoldChange'], row['logCPM'], row['pvalue'], row['padj'], str({h: row[h] for h in JSON_headers})))
            self.conn.commit()
    
    def close(self):
        if self.conn:
            self.conn.close()
    
    def view_data(self, table_name):
        if self.conn:
            
            self.cursor.execute(f"SELECT * FROM {table_name};")
            print(self.cursor.fetchall())
        else:
            print("No database connection.")
            return None
    
    def parse_differential_expression(self, info, date, JSON_headers, file_path):
        self.parse_experimental_data(info, file_path)
        self.insert_gene_results(info, JSON_headers)
        print(f"Finished parsing file: {file_path}")
