#!/home/codespace/.python/current/bin/python3

from get_file import finder
from db_manager import DBManager

from parser import SPARTA_parser


f=finder('/workspaces/SPARTA-pipelining-SQL/RNAseq_Data')
f.identify_tool()
info, date, JSON_headers, file_path = f.parse()
sql=DBManager()
sql.connect()
sql.parse_differential_expression(info, date, JSON_headers, file_path)
