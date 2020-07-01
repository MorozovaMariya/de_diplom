
Дипломный проект по курсу "Data Engineer" на тему

# "Потоковая обработка данных о движении ИСЗ"

* ИСЗ - искусственные спутники Земли

## Источники данных.

На сайте https://www.space-track.org/ представлены различные варианты датасетов с координатами передвижения спутников, например по видам орбит:
<li>Средняя околоземная орбита (MEO)</li>
<li>Низкая околоземная орбита (LEO)</li>
<li>Высокоэллиптическая орбита (HEO)</li>

<li>GEO: 0.99 <= Mean Motion <= 1.01 and Eccentricity < 0.01</li>
<li>MEO: 600 minutes <= Period <= 800 minutes and Eccentricity < 0.25</li>
<li>LEO: Mean Motion > 11.25 and Eccentricity < 0.25</li>
<li>HEO: Eccentricity > 0.25</li>



## Архитектурная схема.



## Развертывание и запуск проекта.

1. Склонировать проект:

  ```git clone https://github.com/MorozovaMariya/de_diplom.git```
  
  
Состав проекта:
---------------------------
```data_in  
data_out
python
image_out
docker-compose.yml```
  
satellite_list.csv - Список спутников, находящихся на LEO-орбите. Список взят с сайта 
  
  
2. Перейти в каталог проекта

  ```cd /de_diplom/```
  
3. запустить docker-контейнеры (для Kafka и Clickhouse):

  ```sudo docker-compose up```
  
2. Для запуска питоновских скриптов необходим установленный Python 3.5.2
  Список необходимых библиотек:
```spacetrack
pyorbital
configparser
pandas
kafka
json
clickhouse_driver
datetime
time
folium
csv
```

Скрипт **load_data_and_send_kafka.py** запускает стриминговую загрузку данных по API с сайта https://www.space-track.org/ Для подключения по API на сайт в файл config.ini добавить свои логин и пароль (зарегистрироваться на сайте https://www.space-track.org/) Здесб же рассчитывааются координаты в 10-минутном интервале за текущий день. Спутники движутся по UTC-времени, поэтому учет движения идет также по UTC.

Скрипт **load_data_and_send_kafka.py** запускает стриминговую загрузку данных по API с сайта https://www.space-track.org/ Для подключения по API на сайт в файл config.ini добавить свои логин и пароль (зарегистрироваться на сайте https://www.space-track.org/)




