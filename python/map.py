from clickhouse_driver import Client
from datetime import datetime
import time
import pandas as pd
import folium

client = Client(host='localhost', port=9000)

def get_map_sat_id(sat_id_i, sat_name_i, sat_period_i):
    utc_time_now = datetime.utcnow()
    utc_time_now_str = utc_time_now.strftime('%Y-%m-%d %H:%M:%S')
    #print(utc_time_now)
    #вытащим данные о движении для этого спутника
    sql = '''select * from satellite.space s where s.SAT_ID = {};'''.format(sat_id_i)
    data=client.execute(sql)
    df_i = pd.DataFrame(data, columns = ['sat_id', 'i', 'utctime', 'lat', 'lon'])
    #найдем ближайшую точку к текущему времени, чтобы отобразить на карте
    near_data_i = sorted(df_i['utctime'], key=lambda x: abs(x-utc_time_now))[:1]
    near_point = df_i[df_i['utctime'] == near_data_i[0]]
    #print('Ближайшая рассчитанная точка спутника к текущему времени: ' + str(near_point) + ' для ' + str(utc_time_now))
    # построим карту
    data = df_i
    lat = data['lat']
    lon = data['lon']
    time = data['utctime']

    loc = 'MAP for satellite "' + sat_name_i + '" on UTC ' + str(utc_time_now_str) + ' with period = ' + str(sat_period_i) + ' min'
    title_html = '''
             <h3 align="center" style="font-size:16px"><b>{}</b></h3>
             '''.format(loc)   

    map = folium.Map(zoom_start = 1, titles = title_html)

    for lat, lon, time in zip(lat, lon, time):
        folium.CircleMarker(location=[lat, lon], radius = 3, popup= str(time), min_width = 14, fill_color='blue' ''', color="gray" ''', fill_opacity = 0.8).add_to(map)

    folium.PolyLine(data[['lat', 'lon']], color="gray", weight=1, opacity=1, smooth_factor = 0.5).add_to(map)    
 
    folium.CircleMarker(
        location=near_point[['lat', 'lon']],
        radius = 15,
        min_width = 14, 
        fill_color='',
        fill_opacity = 1,
        popup='<b>' + str(near_point.iloc[0]['utctime']) + '<b/>'
    ).add_to(map)  
 
    map.get_root().html.add_child(folium.Element(title_html))  
    map.save('../image_out/' + sat_name_i + ".html")
    display(map)
    return

#найдем спутники, для которых есть данные и построим их графики:
sat_list_with_data=client.execute( '''select distinct(l.NORAD_CAT_ID), l.SATNAME, l.PERIOD
 from satellite.space s left join satellite.list l on l.NORAD_CAT_ID = s.SAT_ID;''')
for x in sat_list_with_data:
    #print(x[0],  x[1], x[2])
    sat_id_i, sat_name_i, sat_period_i = x[0], x[1], x[2]
    get_map_sat_id(sat_id_i, sat_name_i, sat_period_i)
    
    
    
    
    
    
    
    
    
    