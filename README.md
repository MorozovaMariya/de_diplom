
Дипломный проект по курсу "Data Engineer" на тему

# "Потоковая обработка данных о движении ИСЗ"

![МКС](https://github.com/MorozovaMariya/de_diplom/blob/master/images_sat/mks.jpg)

* ИСЗ - искусственные спутники Земли

## Источники данных

На сайте https://www.space-track.org/ представлены различные варианты датасетов с координатами передвижения спутников, например по видам орбит:
<li>Средняя околоземная орбита (MEO) (600 minutes <= Period <= 800 minutes and Eccentricity < 0.25)</li>
<li>Низкая околоземная орбита (LEO) ( Mean Motion > 11.25 and Eccentricity < 0.25)</li>
<li>Высокоэллиптическая орбита (HEO) (Eccentricity > 0.25) </li>

Есть еще специальные орбиты захоронения - для вывода на них спутников, которые уже не могут быть использованы.

Для данного примера был заранее загружен список LEO-спутников https://www.space-track.org/basicspacedata/query/class/satcat/PERIOD/<128/DECAY/null-val/CURRENT/Y/format/csv

Низкоорбитальные ИСЗ (LEO)
Низкоорбитальными ИСЗ (НОС (рус.), рис. 8, а) обычно считаются спутники с высотами от 160 км до 2000 км над поверхностью Земли. Такие орбиты (и спутники) в англоязычной литературе называют LEO (от англ. "Low Earth Orbit"). Орбиты LEO подвержены максимальным возмущениям со стороны гравитационного поля Земли и её верхней атмосферы. Угловая скорость спутников LEO максимальна - от 0,2°/с до 2,8°/с, периоды обращения от 87,6 минут до 127 минут.


TLE (аббр. от англ. two-line element set, двухстрочный набор элементов) — двухстрочный формат данных, представляющий собой набор элементов орбиты для спутника Земли.

Формат TLE был определен группировкой NORAD и, соответственно, используется в NORAD, NASA и других системах, которые используют данные группировки NORAD для определения положения интересующих космических объектов.

Модель SGP4/SDP4/SDP8 может использовать формат TLE для вычисления точного положения спутника в определенное время.

Орбитальные элементы определяются для многих тысяч космических объектов из базы данных NORAD и свободно распространяются для дальнейшего использования в Интернете. TLE всегда состоит из двух строк форматированного текста. Кроме того, им может предшествовать строка с названием объекта.

Ниже приведен пример TLE для одного из модулей Международной космической станции (в каталоге она называется ISS (ZARYA).

**ISS (ZARYA)**            
<li>1 25544U 98067A   20178.51707362  .00000595  00000-0  18688-4 0  9993</li>
<li>2 25544  51.6458 302.2859 0002546  86.3240   3.0384 15.49461304233463</li>

Масса Международной космической станции составляет более 400 тонн. Станция находится на высоте 350 км над Землей, а это значит, что спутник относится к низкоорбитальной группе. МКС совершает один оборот вокруг Земли каждые 90 минут. В результате космонавты на борту Международной космической станции наблюдают своеобразный восход и закат Солнца 16 раз в сутки и 5 840 раз в год. Доставка груза на МКС стоит $5 000-$10 000 за каждые 500 грамм.
МКС — это самый дорогой проект за всю историю человечества. Совместными усилиями США, Канады, Японии, России, Бельгии, Бразилии, Германии, Дании, Испании, Италии, Нидерландов, Норвегии, Франции, Швейцарии и Швеции в строительство и содержание станции вложено $150 000 000 000. Рекордное количество модулей - шестнадцать. Для управления с Земли необходимы 3 млн строк программного кода. Летает со скоростью около 8 км/сек.



## Архитектурная схема

![Архитектура](https://github.com/MorozovaMariya/de_diplom/blob/master/images_sat/architectura.png)


## Развертывание и запуск проекта

1. Склонировать репозиторий:

  ```git clone https://github.com/MorozovaMariya/de_diplom.git```
   
Состав проекта:
---------------------------
```
data_in  
python
image_out
image_sat
docker-compose.yml

  
satellite_list.csv - Список спутников, находящихся на LEO-орбите. 
  
  
2. Перейти в каталог проекта

  cd /de_diplom/
  
3. запустить docker-контейнеры (для Kafka и Clickhouse):

  sudo docker-compose up
  
2. Для запуска питоновских скриптов необходим установленный Python 3.5.2, а также предустановленные библиотеки:
spacetrack
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

Скрипт **load_data_and_send_kafka.py** запускает стриминговую загрузку данных по API с сайта https://www.space-track.org/ Для подключения по API на сайт в файл config.ini добавить свои логин и пароль (зарегистрироваться на сайте https://www.space-track.org/) Здесь же рассчитывааются координаты в 10-минутном интервале за текущий день. Спутники движутся по UTC-времени, поэтому учет идет также по UTC. Данные, рассчитыванные по каждому спутнику, отправляются отдельными сообщениями в очередь кафки.

Скрипт **concumer.py** создает необходимые таблицы в бд cickhouse, вычитывает собщения из кафки и складывает в таблицы.

Скрипт **map.py** строит карты движения на те спутники, по которым уже есть данные в базе, а также показывает на карте голубым кругом, где сейчас находится спутник. Построенные карты сохраняются в каталог image_out в виде HTML-файлов.

Пример карты:

![Карта](https://github.com/MorozovaMariya/de_diplom/blob/master/images_sat/map.png)


