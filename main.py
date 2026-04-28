#!/home/codespace/.python/current/bin/python3
import mainproccesses
import argparse
import sys
from rich import print as rprint
import os
def main():
    error_color="[red]"
    success_color="[green]"
    
    parser = argparse.ArgumentParser(description='Parse and store gene expression data.')
    parser.add_argument('--store', type=str, help='Command to store file in database')
    parser.add_argument('-q_genes','--query_gene_results', type=str, help='SQL-like query to filter gene results', nargs='?', const="Default")
    parser.add_argument('-q_exp','--query_experimental_data', nargs='?', type=str, help='SQL-like query to filter experimental data', const="Default")
    parser.add_argument('--sort_by', type=str, nargs='?', help='Column to sort results by (log2FoldChange or pvalue)', default='padj')
    parser.add_argument('--limit', type=int, help='Limit the number of results returned', default=1000)
    parser.add_argument('--export', help='Export query results to CSV instead of displaying them', nargs='?', const=True, default=False)
    parser.add_argument('-del_genes','--delete_gene_results', type=str, help='SQL-like query to filter which gene results to delete', nargs='?', const='default')
    parser.add_argument('-del_exp','--delete_experimental_data', type=str, help='SQL-like query to filter which experiments to delete', nargs='?', const='default')
    args = parser.parse_args()
    
    if args.store: # triggers the store function if flag is detected
        args.file_path=args.store # finds file path provided
        if args.file_path: 
            
            try:
                args.file_path = os.path.abspath(args.file_path.strip('"')) # attempts to find the absolute path of the file
            except Exception as e:
                rprint(f'{error_color}Error occurred while processing file path: {e}')
                sys.exit(1)
            if os.path.isfile(args.file_path): # if the file exists, then proccesses it
                rprint(f"{success_color}Processing file: {args.file_path}")
            else:
                rprint(f"{error_color}File not found: {args.file_path}")
                sys.exit(1)
            mainproccesses.parse_and_store(args.file_path)
            sys.exit()
        else:
            rprint(f"{error_color}No file path provided. Please provide a file path using --file_path.") # if no file path is provided, throws error
            sys.exit()
    
    elif args.query_gene_results or args.query_experimental_data: # if any querying command is ran, trigger
        if args.query_gene_results: # finds the query 
            if args.query_gene_results=='Default': # if the query was left blank, default value to none so that the default query happens
                args.query=None
            else:
                args.query=args.query_gene_results # passes the query to args.query
                args.query=args.query.strip('"')
        else:
            if args.query_experimental_data=='Default': # if the query was left blank, default value to none
                args.query=None
            else:
                args.query=args.query_experimental_data # passes the query to args.query
                args.query=args.query.strip('"')
        if args.sort_by: #checks for sort_by
             sort_by=args.sort_by
        
        if args.limit: #checks for the limit and makes sure that it is valid
            if (args.limit > 0) and isinstance(args.limit, int):
                limit=args.limit
            else:
                rprint(f"{error_color}Invalid limit value. Please provide a positive integer for limit.")
                sys.exit()
       
        
        if args.query_gene_results: # if the command was to query gene results, queries gene results
            try: # tries in case of error
                mainproccesses.view_gene_results(args.query, sort_by, limit, args.export) # passes the arguments on to the main function, which cleans the query and lists the results
            except Exception as e: # if something goes wrong in the mainprocceses function, throws error
                rprint(f'{error_color}Error occurred while listing gene results: {e}')
                sys.exit(1)
            sys.exit()
        else:
            try:
                
                mainproccesses.view_experiments(args.query, limit, args.export) # if the command was to query experimental data, queries experiments
            except Exception as e:
                rprint(f'{error_color}Error occurred while listing experimental data: {e}')
                sys.exit(1)
            sys.exit()
        
    if args.delete_gene_results:
        if args.delete_gene_results=='default':
            rprint(f"{error_color}No deletion query provided. Please provide a query to specify which gene results to delete.")
            sys.exit(1)
        try:
            mainproccesses.delete_gene_results(args.delete_gene_results)
        except Exception as e:
            rprint(f'{error_color}Error occurred while deleting gene results: {e}')
            sys.exit(1)
        sys.exit()
    if args.delete_experimental_data:
        if args.delete_experimental_data=='default':
            rprint(f"{error_color}No deletion query provided. Please provide a query to specify which experimental data to delete.")
            sys.exit(1)

        try:
            mainproccesses.delete_experiments(args.delete_experimental_data)
        except Exception as e:
            rprint(f'{error_color}Error occurred while deleting experiments: {e}')
            sys.exit(1)
        sys.exit()

if __name__ == "__main__":
    main()
