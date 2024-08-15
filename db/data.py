import os.path

import pandas as pd
from .wmisdb import WMISDB, DBError


class Data:
    def __init__(self):
        pass

    def truncate_table(self):
        rslt = False
        try:
            wmisdb = WMISDB()
            conn = wmisdb.connection
            cursor = conn.cursor()
            cmd = 'truncate table giscomparefields;'
            cursor.execute(cmd)
            conn.commit()
            wmisdb = None
            rslt = True
        except DBError as err:
            print(f'Error in truncate_table {err}')
            rslt = False
        except Exception as err:
            print(f'Unexpected Error: {err}')
            rslt = False
        return rslt


    def import_data(self, file: str):
        rslt = False
        df = None
        try:
            wmisdb = WMISDB()
            conn = wmisdb.connection
            cursor = conn.cursor()
            df = pd.read_excel(file)

            allcmds = ''
            params = []
            for index, row in df.iterrows():
                field_id = row['FIELD_ID']
                dil_1998 = str(row['DIL_1998'])
                if dil_1998  == 'nan':
                    dil_1998 = ''
                else:
                    if dil_1998[0].upper() == 'Y':
                        dil_1998 = 'YES'

                params.append((field_id, dil_1998))

            # 4000 records takes about 1 minute to insert.
            cursor.executemany("INSERT INTO giscomparefields (field_id, dil_1998) VALUES (?, ?)", params)
            conn.commit()
            params = None
            cursor = None
            conn = None
            wmisdb = None
            rslt = True
            df = None
        except DBError as err:
            print(f'Error in import_data {err}')
            rslt = False
        except Exception as err:
            print(f'Unexpected Error: {err}')
            rslt = False

        if df is not None:
            df = None

        return rslt


    def process_data(self):
        rslt = False
        try:
            wmisdb = WMISDB()
            conn = wmisdb.connection
            cursor = conn.cursor()
            cmd = "exec sp_CompareGIStoWMIS 'CompareFields'"
            cursor.execute(cmd)
            conn.commit()
            wmisdb = None
            rslt = True
        except DBError as err:
            print(f'Error in process_data {err}')
            rslt = False
        except Exception as err:
            print(f'Unexpected Error: {err}')
            rslt = False
        return rslt

    def load_part1(self):
        rslt = False
        data_rows = []
        try:
            wmisdb = WMISDB()
            conn = wmisdb.connection
            cursor = conn.cursor()
            # load cmd from sql-part1.sql
            sql_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),'sql-part1.sql')
            with open(sql_file, 'r') as file:
                cmd = file.read()
            cursor.execute(cmd)
            rows = cursor.fetchall()
            for row in rows:
                obj = { 'wmis_field_id': row[0], 'begin_date': row[1], 'acres': row[2], 'legal_desc': row[3],
                        'gis_field_id': row[4], 'prior_wmis_field': row[5], 'prior_legal_desc': row[6] }
                data_rows.append(obj)

            cursor = None
            conn = None
            wmisdb = None
            rslt = True
        except DBError as err:
            print(f'Error in load_data_part1 {err}')
            rslt = False
        except Exception as err:
            print(f'Unexpected Error: {err}')
            rslt = False
        return data_rows

    def load_part2(self):
        rslt = False
        data_rows = []
        try:
            wmisdb = WMISDB()
            conn = wmisdb.connection
            cursor = conn.cursor()
            # load cmd from sql-part2.sql
            sql_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),'sql-part2.sql')
            with open(sql_file, 'r') as file:
                cmd = file.read()
            cursor.execute(cmd)
            rows = cursor.fetchall()
            for row in rows:
                obj = { 'gis_field_id': row[0], 'current_wmis_field_id': row[1], 'error_message': row[2] }
                data_rows.append(obj)

            cursor = None
            conn = None
            wmisdb = None
            rslt = True
        except DBError as err:
            print(f'Error in load_data_part1 {err}')
            rslt = False
        except Exception as err:
            print(f'Unexpected Error: {err}')
            rslt = False
        return data_rows


