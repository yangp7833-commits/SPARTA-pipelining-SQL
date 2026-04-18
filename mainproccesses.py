#!/home/codespace/.python/current/bin/python3

from get_file import finder
from db_manager import DBManager
from parser import parser

def clean_query(query, table, sort_by=None, limit=None):
    columns= {'experiment_id': ['experiment_id', 'experiment id', 'experiment_number', 'experiment number'], 
    'tool': ['tool', 'software', 'pipeline', 'program'], 'date': ['date', 'time'], 'file': ['file', 'filepath', 'file path', 'file_location', 'file location', 'file_name', 'file name'], 
    'experiment_name': ['experiment_name', 'experiment name'], 'comparison_label': ['comparison_label', 'comparison label', 'comparison name', 'comparison_name'], 
    'log2FoldChange': ['log2fc', 'log2fold', 'log2foldchange', 'logfc'], 
    'pvalue': ['p-value', 'pvalue'], 'padj':['padj', 'false discovery rate', 'fdr'], 'Gene':['gene'], 'logCPM':['logcpm', 'basemean']} #dictionary of possible column names and their variants.
    # Takes a user query and turns it into a SQL query by splitting individual queries by
    #commas, then splitting each query into column, operator, and value. 
    #Construct the SQL query with parameterized values to prevent SQL injection.
    if sort_by is None:
        sort_by=""
    elif sort_by.lower() in columns['log2FoldChange']:
        sort_by='ORDER BY log2fc DESC'
    elif sort_by.lower() in columns['pvalue']:
        sort_by='ORDER BY pvalue ASC'

    if query is None:
        query= f"SELECT * FROM {table}"+ (f' {sort_by}' if sort_by else "") + (f' LIMIT {limit}' if limit else "")
        return query, None
    else:
        query=query.split(',')
        where_clauses = []
        values = []
        for line in query: # Each query is split into column, operator, and value. This is later passed on as parameters.
            parts = line.strip().split(" ") # Strip removes accidental spaces
            if len(parts) == 3:          # Ensure the query is in the format "column operator value"
                col, op, val = parts
                for cols, aliases in columns.items(): #cleans the headers using the dic
                        if col.lower() in aliases:
                            col=cols
                            break
                where_clauses.append(f"{col} {op} ?")
                values.append(val)
            else:
                print('''Invalid filter format: try using column operator value,
                separated by spaces, and separate multiple queries with commas''')
                return

                    
        query = f"SELECT * FROM {table} WHERE " + " AND ".join(where_clauses)+ (f' {sort_by}' if sort_by else "") + (f' LIMIT {limit}' if limit else '')
        return query, values
        
    
# Master function to parse and store data. Identifies data, parses the arguments, and stores it in the database.
def parse_and_store(file_path):
    f=finder(file_path)
    info, date, JSON_headers, file_path, tool = f.parse()
    sql=DBManager()
    sql.connect()

    id=sql.create_experiment(tool, date, file_path)
    sql.insert_gene_results(info, JSON_headers, id)
    sql.close()

# Function to view data in the experiments table.
def view_experiments(query=None, limit=None): 
    cleaned_query, values=clean_query(query, 'experimental_data', limit)
    sql=DBManager()
    sql.connect()
    try:
        sql.list_experiments(cleaned_query, values)
    except Exception as e:
        print(f"Error executing query: {e}")
    sql.close()


# Function to view data in the gene_results table.
def view_gene_results(query=None, sort_by=None, limit=None):
    cleaned_query, values=clean_query(query, 'gene_results', sort_by, limit)
    print(cleaned_query, values)
    sql=DBManager()
    sql.connect()
    try:
        sql.list_gene_results(cleaned_query, values)
    except Exception as e:
        print(f"Error executing query: {e}")
    sql.close()

parse_and_store('/workspaces/SPARTA-pipelining-SQL/RNAseq_Data/2024-03-29/DEanalysis/all_genes_DGE')

view_gene_results(query=None, sort_by='log2FoldChange')



