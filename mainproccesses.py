#!/home/codespace/.python/current/bin/python3

from get_file import finder
from db_manager import DBManager
from parser import parser
import sys
from rich import print as rprint

# cleans the query provided by the user and constructs one in SQL format to be passed on to the DBmanager
def clean_query(query, table, sort_by=None, limit=None):
    columns= {'experiment_id': ['experiment_id', 'experiment id', 'experiment_number', 'experimentnumber', 'experimental_id', 'experimentalid'], 
    'tool': ['tool', 'software', 'pipeline', 'program'], 'date': ['date', 'time'], 'file': ['file', 'filepath', 'filepath', 'file_location', 'filelocation', 'file_name', 'filename'], 
    'experiment_name': ['experiment_name', 'experimentname'], 'comparison_label': ['comparison_label', 'comparisonlabel', 'comparisonname', 'comparison_name'], 
    'log2fc': ['log2fc', 'log2fold', 'log2foldchange', 'logfc'], 
    'pvalue': ['p-value', 'pvalue'], 'padj':['padj', 'false discovery rate', 'fdr'], 'Gene':['gene'], 'logCPM':['logcpm', 'basemean']} #dictionary of possible column names and their variants.
    
    
    # Takes a user query and turns it into a SQL query by splitting individual queries by
    #commas, then splitting each query into column, operator, and value. 
    #Construct the SQL query with parameterized values to prevent SQL injection.
    if sort_by is None: # sort_by can either be by descending order of log2fc or by pvalue ascending. the default is by pvalue ascending
        sort_by=""
    elif sort_by.lower() in columns['log2fc']:
        sort_by='ORDER BY log2fc DESC'
    elif sort_by.lower() in columns['pvalue']:
        sort_by='ORDER BY pvalue ASC'
    elif sort_by.lower() in columns['padj']:
        sort_by='ORDER BY padj ASC'
    else:
        rprint(f"[red]Invalid sort_by value: {sort_by}. Valid options are log2FoldChange, pvalue, or padj.")
        sys.exit()

    if query is None: # if no query is provided, then uses the default select * query, along with the limit and sorting provided by the user
        query= f"SELECT * FROM {table}"+ (f' {sort_by}') + (f' LIMIT {limit}' if limit is not None else "")
        return query, None
    else: # if a query is provided, cleans it using split and lists
        query=query.split(',') # if the user used proper formatting, individual filters are seperated by commas, which are split by the script
        where_clauses = [] # a list of the filters, all having the column, operator, and a ? 
        values = [] # the values in the query, which will be used in the following function
        for line in query: 
            parts = line.strip().split(" ") # Strip removes accidental spaces
            if len(parts) == 3:          # Ensure the query is in the format "column operator value"
                col, op, val = parts
                for cols, aliases in columns.items(): #cleans the headers using the dic
                        if col.lower() in aliases:
                            col=cols
                            break
                where_clauses.append(f"{col} {op} ?") # appends the cleaned column and the operator along with a question mark
                values.append(val) # the isolated value is also appended to the values list
            else: # if the query is not in 3 parts, then the user used the wrong formatting
                print('''Invalid filter format: try using column operator value,
                separated by spaces, and separate multiple queries with commas''')
                sys.exit()

        # constructs the final query
        query = f"SELECT * FROM {table} WHERE " + " AND ".join(where_clauses)+ (f' {sort_by}') + (f' LIMIT {limit}' if limit is not None else '') # constructs the actual query
        return query, values # returns the query and the values to the db_manager

def clean_deletion(query, table):
    columns= {'experiment_id': ['experiment_id', 'experiment id', 'experiment_number', 'experiment number', 'experimental_id', 'experimental id'], 
     'tool': ['tool', 'software', 'pipeline', 'program'], 'date': ['date', 'time'], 'file': ['file', 'filepath', 'file path', 'file_location', 'file location', 'file_name', 'file name'], 
     'experiment_name': ['experiment_name', 'experiment name'], 'comparison_label': ['comparison_label', 'comparison label', 'comparison name', 'comparison_name'], 
     'log2fc': ['log2fc', 'log2fold', 'log2foldchange', 'logfc'], 
     'pvalue': ['p-value', 'pvalue'], 'padj':['padj', 'false discovery rate', 'fdr'], 'Gene':['gene'], 'logCPM':['logcpm', 'basemean']} #dictionary of possible column names and their variants.
    if query is None:
        rprint(f"[red]No query provided for deletion. Please provide a query to specify which rows to delete.")
        
    else:
        query=query.split(',') # if the user used proper formatting, individual filters are seperated by commas, which are split by the script
        where_clauses = [] # a list of the filters, all having the column, operator, and a ? 
        values = [] # the values in the query, which will be used in the following function
        for line in query: 
            parts = line.strip().split(" ") # Strip removes accidental spaces
            if len(parts) == 3:          # Ensure the query is in the format "column operator value"
                col, op, val = parts
                for cols, aliases in columns.items(): #cleans the headers using the dic
                        if col.lower() in aliases:
                            parts[0]=cols
                            break
                
                where_clauses.append(f"{col} {op} ?") # appends the cleaned column and the operator along with a question mark
                values.append(val) # the isolated value is also appended to the values list
            else: # if the query is not in 3 parts, then the user used the wrong formatting
                rprint("""[red]Invalid filter format: try using column operator value,
                separated by spaces, and separate multiple queries with commas""")
                

        # constructs the final query
        query = f"DELETE FROM {table} WHERE " + " AND ".join(where_clauses) # constructs the actual query
        return query, values # returns the query and the values to the db_manager
        
    
# Master function to parse and store data. Identifies data, parses the arguments, and stores it in the database.
def parse_and_store(file_path):
    f=finder(file_path) # first creates a finder object with the provided file path
    try:
        info, date, JSON_headers, file_path, tool = f.parse() # the finder object passes the path to the parser object, which then uses the export function to get the data
    except Exception as e: # if error occurs, the function will stop with the error message
        rprint(f'[red]Error occurred while parsing file: {e}')
        sys.exit(1)
    sql=DBManager() # if the export works, then connect to database
    sql.connect()

    id=sql.create_experiment(tool, date, file_path) # we first insert the experiment using the tool, date, and file_path. the last row id is kept for the gene_results
    sql.insert_gene_results(info, JSON_headers, id) # we use the iterable object from the previous function as well as the id from the experiment data to insert gene_results
    sql.close() # finally, we close the connection

# Function to view data in the experiments table.
def view_experiments(query=None, limit=None, export=False): 
    cleaned_query, values=clean_query(query, 'experimental_data', None, limit) # constructs the query from data given by the user
    sql=DBManager() # connects to the database
    sql.connect()
    try:
        sql.list_experiments(cleaned_query, values, export) # executes the query and lists the experiments using the DBmanager function
    except Exception as e:
        rprint(f"[red]Error executing query: {e}")
    sql.close() # closes the connection


# Function to view data in the gene_results table.
def view_gene_results(query=None, sort_by=None, limit=None, export=False):
    cleaned_query, values=clean_query(query, 'gene_results', sort_by, limit) # first, we construct the query using the values provided by the user
    print(cleaned_query, values)
    sql=DBManager() # connects the DBmanager
    sql.connect()
    try: # tries to list the gene results
        sql.list_gene_results(cleaned_query, values, export) # the list_gene_results will execute the query with the values, and then construct the table
    except Exception as e: # if an error occurs, stop the function
        rprint(f"[red]Error executing query: {e}")
    sql.close() # closes the connection

def delete_experiments(query):
    cleaned_query, values=clean_deletion(query, 'experimental_data') # first, we construct the deletion query using the values provided by the user
    sql=DBManager() # connects the DBmanager
    sql.connect()
    try: # tries to delete the experiments
        sql.delete(cleaned_query, values) # the delete_experiments function will execute the deletion query with the values
    except Exception as e: # if an error occurs, stop the function
        rprint(f"[red]Error executing deletion query: {e}")
    sql.close() # closes the connection

def delete_gene_results(query):
    cleaned_query, values=clean_deletion(query, 'gene_results') # first, we construct the deletion query using the values provided by the user
    sql=DBManager() # connects the DBmanager
    sql.connect()
    try: # tries to delete the gene results
        sql.delete(cleaned_query, values) # the delete_gene_results function will execute the deletion query with the values
    except Exception as e: # if an error occurs, stop the function
        rprint(f"[red]Error executing deletion query: {e}")
    sql.close() # closes the connection








