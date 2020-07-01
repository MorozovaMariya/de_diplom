
Дипломный проект на тему

# "Потоковая обработка данных о движении ИСЗ"

* ИСЗ - искусственные спутники Земли


## Развертывание и запуск проекта.

1. Склонировать проект:

  ```git clone https://github.com/MorozovaMariya/de_diplom.git```
  
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




