import pandas as pd
import sys
import os
import json

#Collects data from all json file in the path and created DataFrame
def ProcessFiles(path):
    Files_df = pd.DataFrame([])
    for filename in os.listdir(path):
        filename_path = os.path.join(path, filename)
        if os.path.isfile(filename_path):
            with open(filename_path) as file_data:
                json_data = json.load(file_data)
                file_df = pd.DataFrame(json_data)
                file_df['table_name'] = filename.split('.')[0]
                Files_df = Files_df.append(file_df)
    
    return Files_df

#Format column based on the datatype
def format_column(row):
    if row['type'] in ('STRING'):
        format_col_value = f"""REPLACE (REPLACE (REPLACE ( [{row['name']}], CHAR(13), ''), CHAR(10), ''),'"', '""')"""
        final_col_value = r""" '"' + {} + '"' [{}]""".format(format_col_value, row['name'])
        return final_col_value
    elif row['type'] in ('TIMESTAMP'):
        format_col_value = f"FORMAT( " + '[' + row['name'] + ']' + ", 'yyyy-MM-dd HH:mm:ss.ffffff')"
        final_col_value = r""" {} [{}]""".format(format_col_value, row['name'])
        return final_col_value
    else:
        return '[' + row['name'] + ']'

#Generates CSV file
def create_csv():
    path = sys.argv[1]
    df_data = ProcessFiles(path)
    df_data['formated_column'] = df_data.apply(lambda row: format_column(row), axis=1)
    df_data['column_list'] = \
        df_data[['table_name','formated_column']].groupby(['table_name'])['formated_column'].transform(lambda x: ','.join(x))
    
    table_list = df_data[['table_name','column_list']].drop_duplicates()
    table_list.to_csv('transform_data.csv',index=False,doublequote=False, escapechar='\\')
    print(df_data)

#main script of the program
def _init():
    print("Initiated...!")
    create_csv()
    print("Completed...!")

#Initialize the Program
_init()