#!/home/codespace/.python/current/bin/python3
import duckdb
import re
from rich import print as rprint
from rich.text import Text
import pandas as pd
import os
import json
import urllib.request
from pathlib import Path
import hashlib
import sys

class DBManager:
    def __init__(self, db_path='SQL.duckdb'):
        self.db_path = db_path
        self.conn = None
        
    def __enter__(self):
        self.conn = duckdb.connect(self.db_path)

        self.conn.execute('''CREATE SEQUENCE IF NOT EXISTS seq_experiment_id START 1''')

        
        # Create tables
        self.conn.execute('''CREATE TABLE IF NOT EXISTS experimental_data
                            (experiment_id INTEGER PRIMARY KEY DEFAULT nextval('seq_experiment_id'), 
                             tool VARCHAR, date VARCHAR, file VARCHAR, 
                             experiment_name VARCHAR, comparison_label VARCHAR, data_signature VARCHAR)''')
        
        
        self.conn.execute('''CREATE SEQUENCE IF NOT EXISTS seq_gene_id START 1''')
        
        self.conn.execute('''CREATE TABLE IF NOT EXISTS gene_results
                            (id INTEGER PRIMARY KEY DEFAULT nextval('seq_gene_id'),
                            experiment_id INTEGER,
                            gene_symbol VARCHAR,
                            ensembl_id VARCHAR,
                            log2fc DOUBLE,
                            logCPM DOUBLE,
                            pvalue DOUBLE,
                            padj DOUBLE,
                            other_info VARCHAR,
                            FOREIGN KEY (experiment_id) REFERENCES experimental_data(experiment_id))
                            ''')
        
        self.conn.execute('''CREATE TABLE IF NOT EXISTS genes
                            (
                            symbol VARCHAR, 
                            id VARCHAR,
                            ensembl_id VARCHAR,
                            alias_symbol VARCHAR[],
                            prev_symbol VARCHAR[], species VARCHAR,
                            PRIMARY KEY (id, species) 

                            )''')
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_ref_symbol ON genes (symbol)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS index_ref_ensembl_id ON genes (ensembl_id)")

        
        
        
        # Create indexes
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_gene_name_lookup ON gene_results(gene_symbol, experiment_id)')
        
        # Get table and column information
        self.tables = self.conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()
        self.gene_columns = self.conn.execute("SELECT * FROM information_schema.columns WHERE table_name='gene_results'").fetchall()
        self.experiment_columns = self.conn.execute("SELECT * FROM information_schema.columns WHERE table_name='experimental_data'").fetchall()
        
        self.insertion_columns = {
            'experiment_id': ['experiment_id', 'exp_id', 'id'],
            'gene_name': ['gene_name', 'gene', 'genename', 'symbol', 'gene_symbol', 'ensembl_id', 'ensemblid', 'ensembl_gene_id'],
            'log2fc': ['log2fc', 'log2foldchange', 'log2fold'],
            'logCPM': ['logcpm', 'basemean', 'logcpm'],
            'pvalue': ['pvalue', 'p-value', 'p_value'],
            'padj': ['padj', 'fdr', 'false_discovery_rate'],
            'other_info': ['other_info', 'extra_info', 'json_info'],
            
        }
        self.query_columns={'tool': ['tool'],
            'date': ['date'],
            'file': ['file', 'file_path', 'filepath'],
            'experiment_name': ['experiment_name', 'exp_name'],
            'comparison_label': ['comparison_label', 'label'],
            'ensembl_id': ['ensembl_id', 'ensemblid', 'ensembl_gene_id'],
            'prev_symbol': ['prev_symbol', 'previous_symbol'],
            'alias_symbol': ['alias_symbol', 'alias_symbols'],
            'species': ['species'],
            'id': ['id', 'gene_id'],'experiment_id': ['experiment_id', 'exp_id', 'id'],
            'log2fc': ['log2fc', 'log2foldchange', 'log2fold'],
            'logCPM': ['logcpm', 'basemean', 'logcpm'],
            'pvalue': ['pvalue', 'p-value', 'p_value'],
            'padj': ['padj', 'fdr', 'false_discovery_rate'],
            'other_info': ['other_info', 'extra_info', 'json_info'],
            }
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        if self.conn:
            self.conn.close()
    
    def connect(self):
        self.__enter__()

    def find_date_and_file(self, info):
        if os.path.isfile(os.path.abspath(info)):
            date_pattern = re.compile(r'(\d{4}-\d{2}-\d{2})') 
            match = date_pattern.search(info) # first tries to find the date using regex on the file name
            file_path=os.path.abspath(info)
            if match:
                return pd.to_datetime(match.group(1), format='%Y-%m-%d'), file_path
            else: # if the search doesn't work, then uses the path library to find the time, or else uses none
                path=Path(file_path)
                if path.stat().st_mtime:
                    return pd.to_datetime(path.stat().st_mtime, unit='s').strftime('%Y-%m-%d'), file_path
        else:
            file_path='dataframe'
            return pd.datetime.now().strftime('%Y-%m-%d'), file_path
    
    def get_data_signature(self, df):
        if isinstance(df, pd.DataFrame):
            pass
        elif isinstance(df, list) and isinstance(df[0], dict):
            df=pd.DataFrame(df)
        elif os.path.isfile(df):
            df=self.preprocess_df(df, 0)
        sample_data = df[['gene_name', 'pvalue', 'log2fc']].head(100).to_string()
    
        # Generate a unique string (hash) from that data
        return hashlib.md5(sample_data.encode()).hexdigest()

    def preprocess_df(self, info, id):

        if isinstance(info, list):
            if len(info) == 0:
                return
            if isinstance(info[0], dict):
                info = pd.DataFrame(info)
            elif isinstance(info[0], pd.Series):
                info = pd.DataFrame(info)
            else:
                raise ValueError(f"Info list contains unsupported element type {type(info[0])}.")
        elif isinstance(info, pd.DataFrame):
            info = info.copy()
        elif os.path.isfile(os.path.abspath(info)):
            info = pd.read_csv(os.path.abspath(info), sep=None, engine='python')
        else:
            raise ValueError(f'Info is in an unexpected format {type(info)} please provide a pandas DataFrame, a list of dictionaries, or a file path to a CSV or TSV file.')
        
        rename_map = {}
        flat_map = {v.lower(): k for k, variants in self.insertion_columns.items() for v in variants}
        rename_dict = {col: flat_map[col.lower()] for col in info.columns if col.lower() in flat_map}
        info.rename(columns=rename_dict, inplace=True)
        
        
        
        
        
        expected_cols = ["gene_name", "log2fc", "logCPM", "pvalue", "padj", "other_info"]
        for col in expected_cols:
            if col not in info.columns:
                info[col] = None
        
        extra_columns = [col for col in info.columns if col not in expected_cols and col != 'experiment_id']
        if len(extra_columns)>0:
            extra_info=info[extra_columns].to_dict(orient='records')
            info['other_info'] = [json.dumps(r) for r in extra_info]
            info.drop(columns=extra_columns, inplace=True)
        else:
            info['other_info'] = None
        
        
        
        info['experiment_id'] = id
        
        insert_df = info[['experiment_id', 'gene_name', 'log2fc', 'logCPM', 'pvalue', 'padj', 'other_info']]
        
        return insert_df

    




    def initalize_gene_table(self, species):

        if species=='human':
            temp_file='human_genes.tsv'
            url='https://storage.googleapis.com/public-download-files/hgnc/tsv/tsv/hgnc_complete_set.txt'
            urllib.request.urlretrieve(url, temp_file)
            
            self.conn.execute('''INSERT INTO genes (symbol, id, ensembl_id, alias_symbol, prev_symbol, species)
            SELECT symbol, CAST(regexp_replace(hgnc_id, '^hgnc:', '', 'i') AS INTEGER) AS id, 
            ensembl_gene_id, string_split(UPPER(alias_symbol), '|') as alias_symbol, string_split(UPPER(prev_symbol), '|') as prev_symbol, 'human' as species FROM read_csv_auto(?)''', (temp_file,))
            os.remove(temp_file)

    def create_experiment(self, tool, date, file_path, data_signature, experiment_name=None, comparison_label=None):
        if experiment_name is None:
            experiment_name = input('Enter a name for this experiment: ')
        if comparison_label is None:
            comparison_label = input('Enter a comparison label for this experiment: ')

        duplicate_ids=results = self.conn.execute(
            "SELECT experiment_id FROM experimental_data WHERE data_signature = ?", 
            (data_signature,)
            ).fetchall()
        if len(duplicate_ids)>0:
            print(f'Warning: this data is identical to the data from experiment id:{duplicate_ids[0][0]}')
            override=input('Would you still like to proceed? y/n')
            if override=='y' or override=='Y':
                print('warning overwritten')
                pass
            else:
                print('insert canceled')
                sys.exit()


        
        result = self.conn.execute(
            "INSERT INTO experimental_data (tool, date, file, experiment_name, comparison_label, data_signature) VALUES (?, ?, ?, ?, ?, ?) RETURNING experiment_id",
            (tool, date, file_path, experiment_name, comparison_label, data_signature)
        ).fetchall()
        self.conn.commit()
        
        return result[0][0]

    def insert_gene_results(self, insert_df):
        
        self.conn.execute(f'''
        INSERT INTO gene_results 
        (experiment_id, gene_symbol, ensembl_id, log2fc, logCPM, pvalue, padj, other_info) 
    
        SELECT DISTINCT ON (df.experiment_id, df.gene_name)
        df.experiment_id, 
        -- Use the official symbol if found, otherwise keep the input name
        COALESCE(ref.symbol, df.gene_name) as gene_symbol, 
        
        -- Use official Ensembl if found, or input if it looks like an ENSG ID
        COALESCE(ref.ensembl_id, CASE WHEN df.gene_name LIKE 'ENSG%' THEN df.gene_name END) as ensembl_id,
        
        df.log2fc, df.logCPM, df.pvalue, df.padj, df.other_info
        FROM insert_df df
        LEFT JOIN genes ref ON (
        UPPER(TRIM(split_part(df.gene_name, '.', 1))) = UPPER(ref.ensembl_id) OR    
        UPPER(df.gene_name) = UPPER(ref.symbol) OR 
        list_contains(ref.alias_symbol, UPPER(df.gene_name)) OR
        list_contains(ref.prev_symbol, UPPER(df.gene_name))
        )
        ORDER BY df.experiment_id, df.gene_name, ref.symbol NULLS LAST
        ''')

        self.conn.commit()
        
    def insert_to_database(self, info, tool=None, date=None, file_path=None, experiment_name=None, comparison_label=None):
        
        date, file_path = self.find_date_and_file(info)
        if date:
            date = pd.to_datetime(date).strftime('%Y-%m-%d')
        data_signature=self.get_data_signature(info)
        id = self.create_experiment(tool, date, file_path, data_signature, experiment_name, comparison_label)
        df = self.preprocess_df(info, id)
        self.insert_gene_results(df)
        

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            
    def query(self, table, save_path=None, **filters):
        # Check if the table exists in the database
        table_names = [t[0] for t in self.tables]
        
        if table not in table_names:
            rprint(f"[red]Error: Table '{table}' does not exist in the database.")
            rprint(f"[yellow]Available tables:")
            for available_table in table_names:
                rprint(f"  - {available_table}")
            return
        
        # Get actual columns for the table
        if table == 'gene_results':
            actual_columns = [col[3] for col in self.gene_columns]
        elif table == 'experimental_data':
            actual_columns = [col[3] for col in self.experiment_columns]
        else:
            cols = self.conn.execute(f"SELECT * FROM information_schema.columns WHERE table_name='{table}'").fetchall()
            actual_columns = [col[3] for col in cols]
        
        normalized_filters = {}
        for filter_key, filter_value in filters.items():
            filter_column = filter_key.split('__')[0]
            filter_operator_suffix = '__' + filter_key.split('__')[1] if '__' in filter_key else ''
            
            # Map the given filter column to actual column name
            actual_column = None
            for standard_col, variants in self.query_columns.items():
                if filter_column.lower() in variants and standard_col in actual_columns:
                    actual_column = standard_col
                    break
            
            if not actual_column:
                rprint(f"[red]Error: Column '{filter_column}' does not exist in table '{table}'.")
                rprint(f"[yellow]Available columns: {', '.join(actual_columns)}")
                return
            
            normalized_filters[actual_column + filter_operator_suffix] = filter_value
        
        # Construct the query based on filters    
        query = f'SELECT * FROM {table} WHERE 1=1'
        params = []
        
        for column, value in normalized_filters.items():
            column_parts = column.split('__')
            if len(column_parts) == 1:
                query += f' AND {column_parts[0]} = ?'
            elif len(column_parts) == 2 and column_parts[1] in ['gt', 'lt', 'gte', 'lte', 'ne']:
                operator = {
                    'gt': '>',
                    'lt': '<',
                    'gte': '>=',
                    'lte': '<=',
                    'ne': '!='
                }[column_parts[1]]
                query += f' AND {column_parts[0]} {operator} ?'
            else:
                raise ValueError(f'Invalid filter: {column}')
            params.append(value)
        
        # Execute query and convert to DataFrame
        df = self.conn.execute(query, params).df()
        if 'id' in df.columns:
            df = df.set_index('id')
        elif not save_path:
            df=df.set_index('experiment_id')

        if save_path:
            folder = os.path.dirname(save_path)
            if folder and not os.path.exists(folder):
                os.makedirs(folder)
            
            if save_path.endswith('.csv'):
                df.to_csv(save_path, index=False)
            elif save_path.endswith('.xlsx'):
                try:
                    import openpyxl
                    df.to_excel(save_path, index=False)
                except ImportError:
                    print("openpyxl library is required to save as Excel. Please install it using 'pip install openpyxl'.")
            elif save_path.endswith('.json'):
                df.to_json(save_path, orient='records')
            else:
                df.to_csv(save_path, index=False)
            
            print(f"Successfully saved results to {save_path}")

        print(f'found {len(df)} results matching the query.')
        

        return df
       
   
    
    def delete(self, table, **filters):
        table_names = [t[0] for t in self.tables]
        
        if table not in table_names:
            rprint(f"[red]Error: Table '{table}' does not exist in the database.")
            rprint(f"[yellow]Available tables:")
            for available_table in table_names:
                rprint(f"  - {available_table}")
            return
        
        if table == 'gene_results':
            actual_columns = [col[3] for col in self.gene_columns]
        elif table == 'experimental_data':
            actual_columns = [col[3] for col in self.experiment_columns]
        else:
            cols = self.conn.execute(f"SELECT * FROM information_schema.columns WHERE table_name='{table}'").fetchall()
            actual_columns = [col[3] for col in cols]
        
        normalized_filters = {}
        if len(filters) == 0:
            raise ValueError("No filters provided for deletion. Please provide at least one filter to specify which records to delete.")
        
        for filter_key, filter_value in filters.items():
            filter_column = filter_key.split('__')[0]
            filter_operator_suffix = '__' + filter_key.split('__')[1] if '__' in filter_key else ''
            
            actual_column = None
            for standard_col, variants in self.query_columns.items():
                if filter_column.lower() in variants and standard_col in actual_columns:
                    actual_column = standard_col
                    break
            
            if not actual_column:
                rprint(f"[red]Error: Column '{filter_column}' does not exist in table '{table}'.")
                rprint(f"[yellow]Available columns: {', '.join(actual_columns)}")
                return
            
            normalized_filters[actual_column + filter_operator_suffix] = filter_value
        
        query = f'DELETE FROM {table} WHERE 1=1'
        params = []
        
        for column, value in normalized_filters.items():
            column_parts = column.split('__')
            if len(column_parts) == 1:
                query += f' AND {column_parts[0]} = ?'
            elif len(column_parts) == 2 and column_parts[1] in ['gt', 'lt', 'gte', 'lte', 'ne']:
                operator = {
                    'gt': '>',
                    'lt': '<',
                    'gte': '>=',
                    'lte': '<=',
                    'ne': '!='
                }[column_parts[1]]
                query += f' AND {column_parts[0]} {operator} ?'
            else:
                raise ValueError(f'Invalid filter: {column}')
            params.append(value)
        
        self.conn.execute(query, params)
        print(f"Successfully deleted records from {table} matching the specified filters.")
    
    def execute_raw(self, query):
        self.conn.execute(query)

    

        