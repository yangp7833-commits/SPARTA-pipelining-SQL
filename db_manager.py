#!/home/codespace/.python/current/bin/python3
import sqlite3
import re
from rich.console import Console
from rich import print as rprint
from rich.table import Table
from rich.text import Text

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
                            FOREIGN KEY (experiment_id) REFERENCES experimental_data(experimental_id) ON DELETE CASCADE)
                            ''')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_gene_experiment_id ON gene_results(experiment_id)')

        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_gene_padj ON gene_results(padj ASC)')

       
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_gene_name ON gene_results(gene_name)')
        self.conn.commit()

    # done before the gene results are entered. uses the data from the export_data function to create a new experiment
    def create_experiment(self, tool, date, file_path):
        experiment_name=input('Enter a name for this experiment: ') # uses user input to get the experiment and comparison names
        comparison_label=input('Enter a comparison label for this experiment: ')
        if ' ' in experiment_name or ' ' in comparison_label: # if there are spaces, throws error as it can cause problems with querying
            experiment_name=experiment_name.replace(" ", "_")
            comparison_label=comparison_label.replace(" ", "_")
            rprint(f"[red]Spaces detected in experiment name or comparison label. Replacing spaces with underscores. Experiment name: {experiment_name}, Comparison label: {comparison_label}")
        self.cursor.execute("INSERT INTO experimental_data (tool, date, file, experiment_name, comparison_label) VALUES (?, ?, ?, ?, ?);", (tool, date, file_path, experiment_name, comparison_label))
        return self.cursor.lastrowid # returns the last row id so that the gene results from the file can all be inserted with the matching id

    # inserts all the gene results using the data from the export_data function
    def insert_gene_results(self, info, JSON_headers, id):
        query = "INSERT INTO gene_results (experiment_id, gene_name, log2fc, logCPM, pvalue, padj, other_info) VALUES (?, ?, ?, ?, ?, ?, ?);"
        for row in info:
            self.cursor.execute(query, (id, row['Gene'], row['log2FoldChange'], row['logCPM'], row['pvalue'], row['padj'], str({h: row[h] for h in JSON_headers}))) # the last row is done in dictionary format and turned into a string, resembling JSON format
        self.conn.commit()

    def close(self):
        if self.conn:
            self.conn.close()
            
    # executes the query provided from the clean_query function. values are set to none by default if no specefic query is mentioned
    def list_experiments(self, query, values=None, export=False):
        if values is None:
            results=self.cursor.execute(query).fetchall()
        else: 
            results=self.cursor.execute(query, tuple(values)).fetchall()
        
        if results[0][0] and export==False: # if there are results, pass the iterable object on to the visualize function. else, return no results
            rprint(f"[green]Found {len(results)} results matching the query.")
            self.visualize_experiments(results)
        elif results[0][0] and export==True: # if there are results and the user wanted to export, then we return the results instead of visualizing them
            rprint(f"[green]exporting {len(results)} results matching the query. Exporting to CSV...") # if there are results and the user wanted to export, then we return the results instead of visualizing them
            self.export_experiments(results)
        else:
            rprint(f"[red]No experiments found matching the query.")
        
    # executes the query provided from the clean_query function. values are set to none by default if no specefic query is mentioned
    def list_gene_results(self, query, values=None, export=False):
        if values is None:
            results=self.cursor.execute(query).fetchall()
        else: 
            results=self.cursor.execute(query, tuple(values)).fetchall()
        
        if results[0][0] and export==False: # if there are results, pass the iterable object on to the visualize function. else, return no results
            rprint(f"[green]Found {len(results)} results matching the query.")
            self.visualize_gene_results(results)
        elif results[0][0] and export==True: # if there are results and the user wanted to export, then we return the results instead of visualizing them
            rprint(f"[green]exporting {len(results)} results matching the query. Exporting to CSV...") # if there are results and the user wanted to export, then we return the results instead of visualizing them
            self.export_gene_results(results)
        else:
            rprint(f"[red]No gene results found matching the query.")
    
    def visualize_gene_results(self, results):
        #visualizes data using the rich library.
        console = Console()
        table = Table(show_header=True, header_style="bold black", expand=True,  show_lines=True, show_edge=False, title="Gene Results", title_style="bold white on black")
        table.add_column("Experiment ID", width=20)
        table.add_column("Gene Name", width=20)
        table.add_column("log2FoldChange", justify="right")
        table.add_column("logCPM", justify="right")
        table.add_column("p-value", justify="right")
        table.add_column("padj", justify="right")
        table.add_column("Other Info", justify="left")
        
        # Define styles for different value ranges
        neutral_color='dim black'
        significant_color='bold italic white on red3'
        insignificant_color='bold italic white on blue3'
        identifier_color='italic bold black'

        # sorts all results by value and assigns styles based on thresholds.

        # for every row in the results, deterimine the colors and insert the information
        for result in results:
            experiment_name_cell = Text(str(result[1]), style=neutral_color) # experiment name, gene name, and other info all are kept neutral as they are identifier columns
            gene_name_cell = Text(str(result[2]), style=neutral_color)
            other_info_cell = Text(str(result[7]), style=neutral_color)
            if result[3] > 1.0: # if the log2fc is more than 1, then it means an increase in gene expression, which is denoted as red
                lfc_style = significant_color 
            elif result[3] < -1.0: # if it is less than 1, then it is blue as the gene expression was supressed
                lfc_style = insignificant_color   
            else:
                lfc_style = neutral_color       # if the result shows no large difference, then it is left neutral
            log2fc_cell=Text(str(result[3]), style=lfc_style) # finalizes the cell to enter into the table

            if result[4] > 0: # if the logCPM is above 0, then the sample size is likely large enough to be significant, so it is bolded
                logcpm_style = identifier_color 
            else: # if not, the result could be noise due to small sample size, so the text is kept neutral
                logcpm_style = neutral_color
            logcpm_cell = Text(str(result[4]), style=logcpm_style) # finalizes logcpm and prepares for entry into the table

            if result[6] < 0.05: # if the false discovery rate is less than 0.05, then the results are statistically significant, so it is bolded
                padj_style = identifier_color
            else: # otherwise, the result could be noise, so it is kept neutral
                padj_style = neutral_color
            padj_cell = Text(str(result[6]), style=padj_style) # finalizes the cell for entry into the table

            if result[5] < 0.05: # same thing for p-value
                pvalue_style = identifier_color
            else:
                pvalue_style = neutral_color
            pvalue_cell = Text(str(result[5]), style=pvalue_style)

            table.add_row(experiment_name_cell, gene_name_cell, log2fc_cell, logcpm_cell, pvalue_cell, padj_cell, other_info_cell)

        console.print(table) # finally prints the table


    #visualizes experimental data
    def visualize_experiments(self, results):
        console = Console()
        table = Table(show_header=True, header_style="bold black", expand=True,  show_lines=True, show_edge=False, title="Experiments", title_style="bold white on black")
        table.add_column("Experiment ID", width=15)
        table.add_column("Tool", width=20)
        table.add_column("Date", width=20)
        table.add_column("File Path", width=40)
        table.add_column("Experiment Name", width=20)
        table.add_column("Comparison Label", width=20) # unlike gene_results, we don't need specefic coloring for experiments

        for result in results:
            table.add_row(str(result[0]), result[1], result[2], result[3], result[4], result[5])

        console.print(table)
    
    def export_gene_results(self, results):
        file_name=input("Enter the file path to export to (including .csv extension): ") # gets the file path from the user
        import csv
        with open(file_name, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Experiment ID', 'Gene Name', 'log2FoldChange', 'logCPM', 'p-value', 'padj', 'Other Info'])
            for result in results:
                writer.writerow(result)
        rprint(f"[green]Gene results successfully exported to {file_name}.")

    def export_experiments(self, results):
        file_name=input(f"Enter the file path to export to (including .csv extension): ") # gets the file path from the user
        import csv
        with open(file_name, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Experiment ID', 'Tool', 'Date', 'File Path', 'Experiment Name', 'Comparison Label'])
            for result in results:
                writer.writerow(result)
        rprint(f"[green]Experimental data successfully exported to {file_name}.")
    
    def delete(self, query, values):
        count=self.cursor.execute(query.replace('DELETE', 'SELECT COUNT(*)'), tuple(values)).fetchone()[0] # first, we execute a count query to see how many rows will be deleted with the provided query and values
        if count>1000: # if the deletion will affect more than 1000 rows, we ask the user to confirm before proceeding
            rprint(f"[red]Warning: This deletion will affect {count} rows. Are you sure you want to proceed? (yes/no)")
            confirmation=input().lower()
            if confirmation != 'yes' or confirmation=='':
                rprint(f"[green]Deletion cancelled.")
                return
        elif count==0: # if no rows will be affected, we can stop the function and return
            rprint(f"[red]No rows found matching the deletion query. Deletion cancelled.")
            return
        self.cursor.execute(query, tuple(values)) # executes the deletion query with the values provided by the user
        self.conn.commit()
        rprint(f"[green]Deletion successful. {count} rows deleted.")
    
                
        

      
    