#!/usr/bin/python3
# coding: utf-8

## Вариант создания набора координат с 10-мин интервалом за сутки для каждого спутника in-memory в текст

from kafka import KafkaProducer
from json import dumps
import time
from datetime import datetime, date, timedelta
import spacetrack.operators as op
from spacetrack import SpaceTrackClient
from pyorbital.orbital import Orbital
import configparser
import pandas as pd

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda m: dumps(m).encode('utf-8'))

 
config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8')
user=config.get('spacetrack', 'user')
password=config.get('spacetrack', 'password')

def get_spacetrack_tle (sat_id, start_date, end_date, username, password, latest=False):
    st = SpaceTrackClient(identity=username, password=password)
    if not latest:
        daterange = op.inclusive_range(start_date, end_date)
        data = st.tle(norad_cat_id=sat_id, orderby='epoch desc', limit=1, format='tle', epoch = daterange)
    else:
        data = st.tle_latest(norad_cat_id=sat_id, orderby='epoch desc', limit=1, format='tle')
 
    if not data:
        return 0, 0
 
    tle_1 = data[0:69]
    tle_2 = data[70:139]
    return tle_1, tle_2
 
# На вход будем требовать идентификатор спутника, день (в формате date (y,m,d))
# шаг в минутах для определения положения спутника, путь для результирующего файла
def create_orbital_track_for_day (sat_id, track_day, step_minutes, USERNAME, PASSWORD):
    print('------------Запуск в ' + str(datetime.utcnow()))
    # Для начала получаем TLE    
    # Если запрошенная дата наступит в будущем, то запрашиваем самые последний набор TLE 
    if track_day > date.today():
        tle_1, tle_2 = get_spacetrack_tle (sat_id, None, None, USERNAME, PASSWORD, True)
    # Иначе на конкретный период, формируя запрос для указанной даты и дня после неё
    else:
        tle_1, tle_2 = get_spacetrack_tle (sat_id, track_day, track_day + timedelta(days = 1), USERNAME, PASSWORD, False)
     
    # Если не получилось добыть    
    if not tle_1 or not tle_2:
        print ('Невозможно получить набор данных TLE для sat_id = ' + str(sat_id) + ' for track_day = ' + str(track_day))        
        return
 
    print (tle_1, tle_2)   
    # Создаём экземляр класса Orbital
    orb = Orbital("N", line1=tle_1, line2=tle_2)

    # Объявляем счётчики, i для идентификаторов, minutes для времени
    i = 0
    minutes = 0
    #set_row = 'sat_id,i,utcdatetime,lat,lon' + '\n'
    set_row = ''
    #df = pd.dataframe()
    # Простой способ пройти сутки - с заданным в минутах шагом дойти до 1440 минут.
    while minutes < 1440:
        # Рассчитаем час, минуту, секунду (для текущего шага)
        utc_hour = int(minutes // 60)
        utc_minutes = int((minutes - (utc_hour*60)) // 1)
        utc_seconds = int(round((minutes - (utc_hour*60) - utc_minutes)*60))
 
        # Сформируем строку для атрибута
        utc_string = str(utc_hour) + '-' + str(utc_minutes) + '-' + str(utc_seconds)
        # И переменную с временем текущего шага в формате datetime
        utc_time = datetime(track_day.year,track_day.month,track_day.day,utc_hour,utc_minutes,utc_seconds)
        # Считаем положение спутника
        lon, lat, alt = orb.get_lonlatalt(utc_time)
 
        # запишем
        if i == 0:
            set_row = str(sat_id) + ',' + str(i) + ',' + '\'' +  str(utc_time)  + '\'' + ',' + str(lat) + ',' + str(lon)
        else:
            set_row = set_row + '\n' + str(sat_id) + ',' + str(i) + ',' + '\'' + str(utc_time) + '\'' + ',' + str(lat) + ',' + str(lon)
        #writer.writerow(sat_id, track_day, i, utc_string, utc_time, lat, lon])
        # Не забываем про счётчики
        i += 1
        minutes += step_minutes
 
    try:
        if len(set_row) > 5:
            print(set_row)
            #отправить в кафку
            
            producer.send('satellite_msg', value=set_row)
            print('-------------------------send to kafka')
            print('-------------------------sleep 1')
            time.sleep(1)
        print('------------Финиш для ' + str(sat_id) + ' в ' + str(track_day) +'  ' + str(datetime.utcnow()) + '\n')
    except Exception as e:
        print ('Невозможно отправить данные в кафку, ошибка: ' + str(e))
    return


utc_time = datetime.utcnow()

# Получение списка низкоорбитальных ИСЗ (LEO)  15369 строк
sat_list = pd.read_csv('../data_in/satellite_list.csv', sep=",")
sat_list_id = sat_list['NORAD_CAT_ID']

for sat_id in sat_list_id:
    try:
        #path_csv = '../data_out/' + str(sat_id) + '_' + str(date(utc_time.year,utc_time.month,utc_time.day)) +  '.csv'
        print('Поиск данных для спутника ' + str(sat_id) + ' на дату ' + str(date(utc_time.year,utc_time.month,utc_time.day)))
        create_orbital_track_for_day(sat_id, date(utc_time.year,utc_time.month,utc_time.day), 10, user, password)
    except Exception as e:
        print ('Невозможно извлечь данные, ошибка: ' + str(e))
    continue
