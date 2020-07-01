#!/usr/bin/python3
# coding: utf-8

from kafka import KafkaConsumer
from json import loads
from clickhouse_driver import Client
import csv
from csv import DictReader
from datetime import datetime
import time


client = Client(host='localhost', port=9000)
client.execute('CREATE DATABASE IF NOT EXISTS satellite')
client.execute('USE satellite')
client.execute("DROP TABLE IF EXISTS satellite.space")
client.execute("DROP TABLE IF EXISTS satellite.list")

#Создание и заполнение таблицы со списком спутников
client.execute(
    '''CREATE TABLE IF NOT EXISTS satellite.list (
     INTLDES String,
     NORAD_CAT_ID Int, 
     OBJECT_TYPE String,
     SATNAME String,
     COUNTRY String,
     LAUNCH date,
     SITE String,
     DECAY String,
     PERIOD Float32,
     INCLINATION Float32,
     APOGEE Int,
     PERIGEE Int,
     COMMENT String,
     COMMENTCODE String,
     RCSVALUE int,
     RCS_SIZE String,
     FILE Int,
     LAUNCH_YEAR Int,
     LAUNCH_NUM Int,
     LAUNCH_PIECE String,
     CURRENT String,
     OBJECT_NAME String,
     OBJECT_ID String,
     OBJECT_NUMBER Int
    ) ENGINE = MergeTree()
    PRIMARY KEY NORAD_CAT_ID 
    ORDER BY NORAD_CAT_ID;'''
)

schema = {
    'INTLDES': str,
    'NORAD_CAT_ID': int, 
    'OBJECT_TYPE': str,
    'SATNAME': str,
    'COUNTRY': str,
    'LAUNCH': lambda x: datetime.strptime(x, '%Y-%m-%d'),
    'SITE': str,
    'DECAY': str,
    'PERIOD': float,
    'INCLINATION': float,
    'APOGEE': int,
    'PERIGEE': int,
    'COMMENT': str,
    'COMMENTCODE': str,
    'RCSVALUE': int,
    'RCS_SIZE': str,
    'FILE': int,
    'LAUNCH_YEAR': int,
    'LAUNCH_NUM': int,
    'LAUNCH_PIECE': str,
    'CURRENT': str,
    'OBJECT_NAME': str,
    'OBJECT_ID': str,
    'OBJECT_NUMBER': int
}
bypass = lambda x: x

with open('../data_in/satellite_list.csv') as f:
    #print (f)
    gen = ({k: schema.get(k, bypass)(v) for k, v in row.items()} for row in csv.DictReader(f))
    print(gen)
    client.execute('INSERT INTO satellite.list VALUES', gen)

time.sleep(3)


d=client.execute( '''select * from satellite.list limit 5;''')
print ('--------------------Проверка загрузки данных - список спутников:')
print (d)

client.execute(
     '''CREATE TABLE IF NOT EXISTS satellite.space (
     SAT_ID Int,
     I Int,
     UTCDATETIME DateTime,
     LAT Float32,
     LON Float32
    ) ENGINE = MergeTree()
    PRIMARY KEY SAT_ID 
    ORDER BY SAT_ID;'''
)

#client.execute('SHOW TABLES')

consumer = KafkaConsumer(
    'satellite_msg',
    auto_offset_reset='earliest',
    #reset_offset_on_start=False, #new 
    enable_auto_commit=True,
    group_id='my-group-1',
    value_deserializer=lambda m: loads(m.decode('utf-8')),
    bootstrap_servers='localhost:9092')

# ГЛАВНАЯ вставка данных о координатах проекций
msg_counter = 0
for msg in consumer:
    msg_counter +=1
    #print (msg.value)
    print ('-------------------NEW_MSG----------------------------------------------')
    l = msg.value.split('\n')
    tuples_space = []
    for row in l:
        tuples_space.append('(' + row + ')')
    values_for_insert = ','.join(tuples_space)
    #print (values_for_insert)
    sql = '''INSERT INTO satellite.space VALUES {};'''.format(values_for_insert)
    print (sql)
    d=client.execute(sql)
    print ('------------------Count_msg = ' + str(msg_counter))
    print ('------------------sleep 1')
    time.sleep(1)



