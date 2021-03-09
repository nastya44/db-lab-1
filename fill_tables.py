import os
import re
import csv
import time
import json
import psycopg2
import psycopg2.extras



database_connection_data = {
    'database': 'postgres',
    'user': 'postgres',
    'password': '123',
    'host': 'localhost'
}


RESULT_FILE = 'result.csv'
RES_QUERY = '''
    SELECT 
        zno_year, 
        ROUND(avg(engBall100)), 
        REGNAME
    FROM tbl_grade 
    WHERE physTestStatus = 'Зараховано' 
    GROUP BY zno_year, REGNAME
'''
RES_COLUMNS = ['Year', '100-200' ,'Region']

query_fill_data = (
    '''
        INSERT INTO tbl_grade (
            zno_year,
            OUTID,
            Birth,
            SEXTYPENAME,
            REGNAME,
            AREANAME,
            TERNAME,
            REGTYPENAME,
            TerTypeName,
            ClassProfileNAME,
            ClassLangName,
            EONAME,
            EOTYPENAME,
            EORegName,
            EOAreaName,
            EOTerName,
            EOParent,
            UkrTest,
            UkrTestStatus,
            UkrBall100,
            UkrBall12,
            UkrBall,
            UkrAdaptScale,
            UkrPTName,
            UkrPTRegName,
            UkrPTAreaName,
            UkrPTTerName,
            histTest,
            HistLang,
            histTestStatus,
            histBall100,
            histBall12,
            histBall,
            histPTName,
            histPTRegName,
            histPTAreaName,
            histPTTerName,
            mathTest,
            mathLang,
            mathTestStatus,
            mathBall100,
            mathBall12,
            mathBall,
            mathPTName,
            mathPTRegName,
            mathPTAreaName,
            mathPTTerName,
            physTest,
            physLang,
            physTestStatus,
            physBall100,
            physBall12,
            physBall,
            physPTName,
            physPTRegName,
            physPTAreaName,
            physPTTerName,
            chemTest,
            chemLang,
            chemTestStatus,
            chemBall100,
            chemBall12,
            chemBall,
            chemPTName,
            chemPTRegName,
            chemPTAreaName,
            chemPTTerName,
            bioTest,
            bioLang,
            bioTestStatus,
            bioBall100,
            bioBall12,
            bioBall,
            bioPTName,
            bioPTRegName,
            bioPTAreaName,
            bioPTTerName,
            geoTest,
            geoLang,
            geoTestStatus,
            geoBall100,
            geoBall12,
            geoBall,
            geoPTName,
            geoPTRegName,
            geoPTAreaName,
            geoPTTerName,
            engTest,
            engTestStatus,
            engBall100,
            engBall12,
            engDPALevel,
            engBall,
            engPTName,
            engPTRegName,
            engPTAreaName,
            engPTTerName,
            fraTest,
            fraTestStatus,
            fraBall100,
            fraBall12,
            fraDPALevel,
            fraBall,
            fraPTName,
            fraPTRegName,
            fraPTAreaName,
            fraPTTerName,
            deuTest,
            deuTestStatus,
            deuBall100,
            deuBall12,
            deuDPALevel,
            deuBall,
            deuPTName,
            deuPTRegName,
            deuPTAreaName,
            deuPTTerName,
            spaTest,
            spaTestStatus,
            spaBall100,
            spaBall12,
            spaDPALevel,
            spaBall,
            spaPTName,
            spaPTRegName,
            spaPTAreaName,
            spaPTTerName
        ) VALUES %s
    '''
)
query_fill_temp_data = (    '''
        UPDATE tbl_temp SET id = %s, done = %s WHERE year = %s
    ''')

COLUMNS = [1, 18, 19, 20, 21, 29, 30, 31, 39, 40, 41, 49, 50, 51, 59, 60, 61, 69, 70, 71, 79, 80, 81, 88, 89, 91, 98, 99, 101, 108, 109, 111, 118, 119, 121]
connection = psycopg2.connect(**database_connection_data)
cursor = connection.cursor()

executions_time = dict()
file_names = ['Odata2019File.csv','Odata2020File.csv']
years = ['2019','2020']
years_count = 0
print('Start')
for file_name in file_names:
    year = years[years_count]
    with open(os.path.join(file_name), encoding='cp1251') as file:
        csv_reader = csv.reader(file, delimiter=';')
        next(csv_reader)
        start_time = time.time()
        iD = 0
        size = list()

        cursor.execute('SELECT id, done FROM tbl_temp WHERE year = ' + year)
        RESULT = cursor.fetchone()

        if RESULT is None:
            cursor.execute('INSERT INTO tbl_temp (year, id, done) VALUES (%s, %s, %s)', [year ,iD, False])
        else:
            if RESULT[-1]:
                continue
            for STR in csv_reader:
                iD += 1
                if iD >= RESULT[0]:
                    break

        for STR in csv_reader:
            for i in range(len(STR)):
                if STR[i] == 'null':
                    STR[i] = None
                else:
                    if i in COLUMNS:
                        STR[i] = STR[i].replace(',', '.')
            iD += 1
            size.append([year] + STR)
            if not iD % 200:
                psycopg2.extras.execute_values(cursor, query_fill_data, size)
                cursor.execute(query_fill_temp_data, [iD, False,year])
                size = list()
                connection.commit()
        if size:
            psycopg2.extras.execute_values(cursor, query_fill_data, size)
            size = list()
        cursor.execute(query_fill_temp_data, [iD, False,year])
        connection.commit()
        exec_time = time.time() - start_time
        print(f'File {file_name} was read. Exec time was {exec_time} sec.')
        executions_time[file_name] = exec_time
        years_count = years_count + 1

print('Done')
cursor.execute(RES_QUERY)
result = cursor.fetchall()

if executions_time:
    with open(os.path.join('time.txt'), 'w') as file:
        file.write(json.dumps(executions_time))
    with open(os.path.join(RESULT_FILE), 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile, dialect='excel')
        csv_writer.writerow(RES_COLUMNS)
        csv_writer.writerows(result)
    print(f'The result was written in {RESULT_FILE}.')


cursor.close()
connection.close()
