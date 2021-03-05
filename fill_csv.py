import psycopg2
import csv
import os


database_connection_data = {
    'database': 'postgres',
    'user': 'postgres',
    'password': '123',
    'host': 'localhost'
}


RESULT_FILE = 'result.csv'
QUERY = '''
    SELECT 
        zno_year, 
        ROUND(avg(engBall100)), 
        REGNAME
    FROM tbl_grade 
    WHERE physTestStatus = 'Зараховано' 
    GROUP BY zno_year, REGNAME
'''
COLUMNS = ['Year', '100-200' ,'Region']

connection = psycopg2.connect(**database_connection_data)

cursor = connection.cursor()

cursor.execute(QUERY)
result = cursor.fetchall()

with open(os.path.join('data', RESULT_FILE), 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile, dialect='excel')
    csv_writer.writerow(COLUMNS)
    csv_writer.writerows(result)
print(f'The result was written in {RESULT_FILE}.')
cursor.close()
connection.close()
