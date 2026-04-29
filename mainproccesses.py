#!/home/codespace/.python/current/bin/python3

from get_file import finder
from db_manager import DBManager
from parser import parser
import sys
from rich import print as rprint


    
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








