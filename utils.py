import pandas as pd
import sqlite3 as s3
from logging_config import *
import pandas as pd
import matplotlib.pyplot as plt
import logging
import collections.abc as c
import typing as t
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import csv 
# 
#
def connect_s3_database(filename: str) -> t.Tuple[s3.Connection, s3.Cursor]:
    try:
        con = s3.connect(filename)
    except s3.ERROR as e:
        log.error(f'Could not connect to database:\n\n{e}')
    curs = con.cursor()
    return con,curs
#
# Make pandas dictionary {table_name : pd.DataFrame(table)}
def tables_to_dict(tables: list, cursor: s3.Cursor) -> dict:
    dict = {str(table): load_table_to_df(cursor,table) for table in tables}
    return dict
#
# Convert cursor data to pandas dataframe
def load_table_to_df(cursor: s3.Cursor,table_name: str) -> pd.DataFrame:
    try:
        cursor.execute(f'''SELECT * FROM {table_name}''')
    except Exception as e:
        log.error('Error loading table:\n\n{e}')
        raise RuntimeError(f"An error occurred while loading the table: {e}")
    rows, columns = cursor.fetchall(), [description[0] for description in cursor.description]
    return pd.DataFrame(rows,columns=columns)
#  
# Return list of all tablenames from cursor
def get_tables_list(cursor: s3.Cursor) -> list:
    try:
        cursor.execute('SELECT name FROM sqlite_master WHERE type= "table"')
    except sqlite3.ERROR as e:
        log.error('Error getting tablenames from master:\n\n{e}')
    return [table[0] for table in cursor.fetchall()]
# 
# General cleaning for all tables 
def clean_dict(df_dict: dict) -> None:
    for name,table in df_dict.items():
        table.drop_duplicates(inplace=True)
        log.info('Removed duplicates')

        if name == 'cademycode_students':
            if get_percentage(len(table[~table.job_id.isna()]),len(table)) <= 2.0:
                df_dict[name] = table[~table.job_id.isna()]
                log.info('Excluding rows with missing job_id_data under 2%')
                table['job_id'] = pd.to_numeric(table['job_id'], errors='coerce').astype('Int8')
                
                log.info('dob to datetime')
                table['dob'] = pd.to_datetime(table['dob']).dt.strftime('%Y-%m-%d')

                log.info('fillna in time_spent_hrs with median')
                table['time_spent_hrs'] = pd.to_numeric( table['time_spent_hrs'], errors= 'coerce')
                table['time_spent_hrs'] = table.time_spent_hrs.fillna(table.time_spent_hrs.median())
                table["current_career_path_id"] = pd.to_numeric(table.current_career_path_id, errors='coerce')
                table["current_career_path_id"] = table["current_career_path_id"].astype('int8')
                table['sex'] = pd.Categorical(table['sex']).codes
                log.info('Encoded sex  F >>0 ; M >>1 ; N >>2')
        elif name == 'cademycode_courses':
            if (table.career_path_id == 0).any() == False:
                new_row = pd.DataFrame({'career_path_id': [0],
                'career_path_name': ['no career path chosen'],
                'hours_to_complete': [0]})
                df_dict[name] = pd.concat([new_row,table],ignore_index=True)
                table.reset_index(drop=True,inplace= True)
                log.info('Added 0 to career_path_id for not chosen')
                df_dict['cademycode_students']['num_course_taken'] = df_dict['cademycode_students']['num_course_taken'].fillna(0)
                df_dict['cademycode_students']['num_course_taken'] = pd.to_numeric(df_dict['cademycode_students']['num_course_taken']).astype('int8')
#
# Helping Functions for the cleaning module
def get_percentage(part: int, whole: int) -> float:
    return (part / whole) * 100

def create_db() -> Engine:
    print('How do you want to call this database? ')
    db_name = input('>> ')
    try:
        engine = create_engine(f'sqlite:///{db_name}.db')
    except s3.ERROR as e:
        log.error('Error creating database:\n\n{e}')
    return engine

def populate_database(table_name: str, db_dict: dict , engine: Engine) -> None :
    db_dict[table_name].to_sql(str(table_name), con=engine, if_exists='replace', index=False)


def get_output(tables: list, db: dict) -> None: 
    while True:
        output = input('Enter "db" for database, "c" "for csv or "b"" for both \n>> ')

        match output:
            case 'db':
                print('Creating database')
                engine = create_db()
                for table in tables:
                    populate_database(table, db, engine)
                    print(f'database {engine} populated with {table}')
                break
            case 'c':
                for table in tables:
                    db[table].to_csv(f'{table}.csv', index=False)
                    print(f'{table} saved as csv')
                break
            case 'b':
                print('generating csv and db')
                engine = create_db()

                for table in tables:
                    print('Creating database tables and csv files')
                    populate_database(table, db, engine)
                    print(f'database {engine} populated with {table}')
                    db[table].to_csv(f'{table}.csv', index=False)
                    print(f'{table} saved as csv')
                break
            case _:
                print('please enter a valid input')









