#!/home/codespace/.python/current/bin/python3
import duckdb
import re
from rich.console import Console
from rich import print as rprint
from rich.table import Table
from rich.text import Text
import pandas as pd
import os
import json
import urllib.request

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
                             experiment_name VARCHAR, comparison_label VARCHAR)''')
        
        
        self.conn.execute('''CREATE SEQUENCE IF NOT EXISTS seq_gene_id START 1''')
        
        self.conn.execute('''CREATE TABLE IF NOT EXISTS gene_results
                            (id INTEGER PRIMARY KEY DEFAULT nextval('seq_gene_id'),
                            experiment_id INTEGER,
                            gene_name VARCHAR,
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
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_gene_experiment_id ON gene_results(experiment_id)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_gene_padj ON gene_results(padj ASC)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_gene_name ON gene_results(gene_name)')
        
        # Get table and column information
        self.tables = self.conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()
        self.gene_columns = self.conn.execute("SELECT * FROM information_schema.columns WHERE table_name='gene_results'").fetchall()
        self.experiment_columns = self.conn.execute("SELECT * FROM information_schema.columns WHERE table_name='experimental_data'").fetchall()
        
        self.column_variants = {
            'experiment_id': ['experiment_id', 'exp_id', 'id'],
            'gene_name': ['gene_name', 'gene', 'genename'],
            'log2fc': ['log2fc', 'log2foldchange', 'log2fold'],
            'logCPM': ['logcpm', 'basemean', 'logcpm'],
            'pvalue': ['pvalue', 'p-value', 'p_value'],
            'padj': ['padj', 'fdr', 'false_discovery_rate'],
            'other_info': ['other_info', 'extra_info', 'json_info'],
            'tool': ['tool'],
            'date': ['date'],
            'file': ['file', 'file_path', 'filepath'],
            'experiment_name': ['experiment_name', 'exp_name'],
            'comparison_label': ['comparison_label', 'label'],
            'ensembl_id': ['ensembl_id', 'ensemblid', 'ensembl_gene_id'],
            'symbol': ['symbol', 'gene_symbol'],
            'prev_symbol': ['prev_symbol', 'previous_symbol'],
            'alias_symbol': ['alias_symbol', 'alias_symbols'],
            'species': ['species'],
            'id': ['id', 'gene_id']
        }
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        if self.conn:
            self.conn.close()
    
    def connect(self):
        self.__enter__()

    def initalize_gene_table(self, species):
        if species=='human':
            temp_file='human_genes.tsv'
            url='https://storage.googleapis.com/public-download-files/hgnc/tsv/tsv/hgnc_complete_set.txt'
            urllib.request.urlretrieve(url, temp_file)
            
            self.conn.execute('''INSERT INTO genes (symbol, id, ensembl_id, alias_symbol, prev_symbol, species)
            SELECT symbol, CAST(regexp_replace(hgnc_id, '^hgnc:', '', 'i') AS INTEGER) AS id, 
            ensembl_gene_id, string_split(UPPER(alias_symbol), '|') as alias_symbol, string_split(UPPER(prev_symbol), '|') as prev_symbol, 'human' as species FROM read_csv_auto(?)''', (temp_file,))
            os.remove(temp_file)

    def create_experiment(self, tool, date, file_path, experiment_name=None, comparison_label=None):
        if experiment_name is None:
            experiment_name = input('Enter a name for this experiment: ')
        if comparison_label is None:
            comparison_label = input('Enter a comparison label for this experiment: ')
        
        result = self.conn.execute(
            "INSERT INTO experimental_data (tool, date, file, experiment_name, comparison_label) VALUES (?, ?, ?, ?, ?) RETURNING experiment_id",
            (tool, date, file_path, experiment_name, comparison_label)
        ).fetchall()
        self.conn.commit()
        
        return result[0][0]

    def insert_gene_results(self, info, id):
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
        else:
            raise ValueError(f"Info is in an unexpected format {type(info)}. Expected a pandas DataFrame or a list of dictionaries.")
        
        rename_map = {}
        flat_map = {v.lower(): k for k, variants in self.column_variants.items() for v in variants}
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
        print(insert_df['gene_name'])
        
        self.conn.execute(
            f'''INSERT INTO gene_results (experiment_id, gene_name, log2fc, logCPM, pvalue, padj, other_info) SELECT insert_df.experiment_id, COALESCE(ref.symbol, insert_df.gene_name), insert_df.log2fc, insert_df.logCPM, insert_df.pvalue, insert_df.padj, insert_df.other_info FROM insert_df
            LEFT JOIN genes ref ON (
        UPPER(TRIM(split_part(insert_df.gene_name, '.', 1))) = UPPER(TRIM(ref.ensembl_id)) OR    
        UPPER(insert_df.gene_name) = UPPER(ref.symbol) OR 
        list_contains(ref.alias_symbol, UPPER(insert_df.gene_name)) OR
        list_contains(ref.prev_symbol, UPPER(insert_df.gene_name))
            )''')
        unmapped_count = self.conn.execute(f"""
        SELECT COUNT(*) 
        FROM insert_df i
        LEFT JOIN genes r ON UPPER(TRIM(i.gene_name)) = UPPER(TRIM(r.symbol))
        WHERE r.symbol IS NULL
         """).fetchone()[0]

        if unmapped_count > 0:
            print(f"""{unmapped_count} genes could not be mapped to official symbols. If you want automatic mapping, please ensure your genes are in the reference table by running the initalize_gene_table(species) function with the appropriate species.""") 
        self.conn.commit()
        
    def insert_to_database(self, info, tool=None, date=None, file_path=None, experiment_name=None, comparison_label=None):
        id = self.create_experiment(tool, date, file_path, experiment_name, comparison_label)
        self.insert_gene_results(info, id)
        

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
            for standard_col, variants in self.column_variants.items():
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
        df = df.set_index('id' if 'id' in df.columns else 'experiment_id')

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
            for standard_col, variants in self.column_variants.items():
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
        