# FMS Delta

[![CodeFactor](https://www.codefactor.io/repository/github/solesensei/fmsdelta/badge)](https://www.codefactor.io/repository/github/solesensei/fmsdelta) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) <a href="https://www.buymeacoffee.com/solesensei"><img src="https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png" height="20px"></a>

Скрипт скачивает реестр недействительных паспортов с ФМС МВД и вычисляет новые добавленные и удаленные паспорта

Задача выполнена в рамках проекта __GlowByte__

## Инструменты и методы
Использовался __Python 3.7__

### Система разработки:
- __OS:__ Windows 10 x64
- __RAM:__ 8GB
- __Core:__ i3

### Методы вычисления `delta`

|      Метод      	| Подход                                                                                                                 	|  Память (в среднем)  	| Время __(small delta)__ 	| Время __(big delta)__ 	| Время __(just increment)__ 	|
|:---------------:	|:------------------------------------------------------------------------------------------------------------------------	|:--------------------:	|:-----------------------:	|:-----------------------:	|:--------------------------:	|
|  __`onepass`__  	|                                            Подсчет одним проходом, __самый быстрый__ метод, зависит от размера дельты. 	| 700 МБ   `<=ram_use` 	|          4 мин          	|       20-40 мин       	|       -       	            |
|   __`stable`__  	| Попарное пересечение блоков строк двух файлов, долгий метод, __стабильный__, не зависит от размера дельты.             	|   2 ГБ  `<=ram_use` 	|          15 мин         	|         35 мин        	|         20 мин        	    |
| __`flow`__ 	    | Быстрый __инкрементный__ метод, не зависит от размера дельты. Плавный сдвиг блока в новом файле с пробегами по бэкапу. 	|   2 ГБ   `<=ram_use`  |          6 мин          	|         8 мин         	|         8 мин         	    |

## Предварительная настройка
Возможна динамическая настройка скрипта через изменение параметров, приведенных ниже, 
или использование скомпилированных `exe` файлов. 

При первичном запуске сменить `pure_start = 1`. 
```py
# ------------------------------ Динамические переменные ------------------------------ # 

# Ссылка на реестр недействительных паспортов с сайта МВД
fms_url = 'http://guvm.mvd.ru/upload/expired-passports/list_of_expired_passports.csv.bz2'
# Флаг запуска. Поставить 1 при первичном запуске. Скачивание + парсинг. Без дельты.
pure_start = 0
# Флаг завершения. По умолчанию очищает директорию от временных файлов и старых бэкапов.
clean_finish = 1
# Требуется ли загрузка в Кронос Синопсис
cronos = 1
# Формат файлов
fformat = '.txt'
# Вид бэкап файлов. Сейчас: list_of_expired_passports_date.txt, delta_date.txt
postfix = datetime.today().strftime('%Y%m%d')  # _date.fformat
# Выбор функции вычисления дельты. Стабильная - медленная, включать при больших дельта
delta_method = 'flow'  # 'onepass' / 'stable' / 'flow'
delta_type = 'plus'  # 'plus' / 'minus' / 'all' 
# Количество используемой оперативной памяти. Связано с размером блока паспортов.
ram_use = '2GB' # [MB|GB] exm: '2GB 700MB' 

# ------------------------------------------------------------------------------------- #
```

## Запуск
Для запуска скрипта необходимо выполнить любую из следующих команд. (Подробнее по методам см. [таблицу](#предварительная-настройка))

Первый запуск (инициализация)
```bash
# using executable
delta.exe --pure
# using python3
python FMSDelta.py --pure
```

Выбор метода подсчета дельты (`onepass`, `stable`, `flow`) с типами дельты (`plus`, `minus`, `all`)
```bash
# using executable
delta.exe -m onepass -t all
delta.exe -m stable -t minus
delta.exe -m flow -t plus
# same with python3
python FMSDelta.py -m [method] -r [type]
```

Дополнительные команды в контекстном меню
```bash
# using executable
delta.exe --help
# using python3
python FMSDelta.py --help
```

### Сбор executable файла
Для сборки __executable__ файла использовалась утилита `pyinstaller`. 
```bash
# using pyinstaller and python3.6
pip install pyinstaller
pyinstaller --onefile FMSDelta.py -n [name]
```

## Результат
### Структура проекта
```py
-backup/ # директория с файлами бэкапов, не больше трех штук, автоматическое удаление
-delta/ # директория с файлами посчитанных дельт, неограниченное количество
-log/ # директория лог файлов, неограниченное количество
-cronos/ # директория с файлами в формате для загрузки в Cинопсис, неограниченное количество
FMSDelta.py # скрипт
delta.exe # executable файл
brokenData.txt # текстовый файл, полученный в результате парсинга реестра, содержит битые данные
README.md # этот текстовый документ
```
