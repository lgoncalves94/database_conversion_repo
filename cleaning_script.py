import sqlite3 as s3
import pandas as pd
import numpy as np
import json as js
from utils import *
from logging_config import *
from sqlalchemy import create_engine

# Connect to database, getting tablenames & loading pandas dataframes into dictionary
con,curs = connect_s3_database('cademycode.db')
tables = get_tables_list(curs)
db = tables_to_dict(tables,curs)
con.close()

# Clean tables & get output
clean_dict(db)
get_output(tables,db)
print('Database cleaned, transformed and saved')



