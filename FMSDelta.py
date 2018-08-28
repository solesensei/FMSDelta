# ----------------------------------------------------------------- #
# Скрипт скачивает реестр недействительных паспортов с ФМС МВД и    #
# вычисляет новые добавленные в базу паспорта                       #
# ----------------------------------------------------------------- #
# GlowByte                                                          #
# Автор: Гончаренко Дмитрий                                         #
# Версия: v0.3                                                      #
# ----------------------------------------------------------------- #

import sys
import csv
import time
from datetime import datetime
import requests  # pip3 install request
import bz2
import os


# ------------------------------ Динамические переменные ------------------------------ # 

# Ссылка на реестр недействительных паспортов с сайта МВД
fms_url = 'http://guvm.mvd.ru/upload/expired-passports/list_of_expired_passports.csv.bz2'
# Флаг запуска. Поставить 1 при первичном запуске. Скачивание + парсинг. Без дельты.
pure_start = 0
# Вид бэкап файлов. Сейчас: list_of_expired_passports_date.csv
# Выполнить pure_start = 1 после изменения. 
postfix = '_' + datetime.today().strftime('%Y%m%d') # _date
# Размер блока чтения (в строках). Больше значение - Больше расход RAM
blocksize = 15 * 10 ** 6 

# ------------------------------------------------------------------------------------- #


# Проверяет состоит ли строка только из цифр
def isInteger(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


# Скачивание файла по ссылке
def downloadFile(url):
    filename = url.split('/')[-1]
    print('Downloading:', filename)
    r = requests.get(url, stream=True)
    with open(filename, 'wb') as f:
        size = 0
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                if size % 10240 == 0:
                    print('Downloaded:', str(size // 1024) + 'MB', end='\r')
                f.write(chunk)
                f.flush()
                size += 1
    print('Downloaded:', filename)
    return filename


# Разархивирование bz2
def decompressFile(filename):
    print('Extracting:', filename)
    with open(filename[:-4], 'wb') as csvfile, open(filename, 'rb') as zipfile:
        z = bz2.BZ2Decompressor()
        for block in iter(lambda: zipfile.read(200 * 1024), b''):
            csvfile.write(z.decompress(block))
    print('Extracted', filename[:-4])
    return filename[:-4]


# Удаление всех данных кроме вида: 1234,123456 (считаются ошибочными)
def parseCSV(filename):
    print('Parsing:', filename)
    pfilename = filename[:-4] + postfix + '.csv'
    with open(filename, 'r', newline='', encoding='utf8') as csvIN, \
        open(pfilename, 'w', newline='') as csvOUT:
        readCSV = csv.reader(csvIN, delimiter=',')
        writeCSV = csv.writer(csvOUT, delimiter=',')
        writeCSV.writerow(next(readCSV))
        num = 0
        for line in readCSV:
            if len(line[0]) == 4 and len(line[1]) == 6 and isInteger(line[0]+line[1]):
                writeCSV.writerow({line[0]+line[1]})
                num += 1
        print('Parsed', num, 'passports!')
        print('File:', pfilename)
        return num, pfilename
    print('Parser ended!')    

# Поиск в директории ./backup самого последнего файла по postfix дате
def getBackFile(filename):
    f = []
    for root, dirs, files in os.walk('./backup'):  
        f.extend(files)
        break
    
# Вычисление дельты
# fileOld - предыдущая версия
# fileNew - новая версия
# N - количество людей в новой базе
def calcDelta(fileOld, fileNew, N):
    print('Comparing:', fileOld, fileNew)
    with open(fileNew, 'r', newline='', encoding='utf8') as csvNEW, \
        open('delta_' + datetime.today().strftime('%Y%m%d') + '.csv', 'w', newline='') as csvDELTA:
        readNEW = csv.reader(csvNEW, delimiter=',')
        writeDelta = csv.writer(csvDELTA, delimiter=',')
        writeDelta.writerow(next(readNEW))
        part = N if N <= blocksize else blocksize
        parts = N // part
        print(part, parts)
        setOld = set()
        setNew = set()
        for i in range(0, parts):
            print('Part:', i+1)
            for k, line in enumerate(readNEW):
                setNew.add(int(line[0]))
                if k == part-1: break
            with open(fileOld, 'r', newline='', encoding='utf8') as csvOLD:
                readOLD = csv.reader(csvOLD, delimiter=',')
                next(readOLD)
                for k, line in enumerate(readOLD):
                    setOld.add(int(line[0]))
                    if k % part == 0 and k > 0:
                        setNew.difference_update(setOld)
                        setOld.clear()
                        print('Checked:', k)
                        if len(setNew) == 0: break
                if len(setNew) > 0 and len(setOld)> 0:
                    setNew.difference_update(setOld) # проверяем оставшиеся записи
                    print('Checked:', k)
            for line in setNew:
                print('{:010d}'.format(line), file=csvDELTA)
            print(len(setNew))
            setOld.clear()
            setNew.clear()
    print('Finished!')


def main():
    print('Starts passports parser!')
    t0 = time.time()
    # При первичном запуске создать папку backup
    if pure_start:
        if not os.path.isdir('./backup'):
            os.mkdir('./backup/')
    
    # Скачиваем реестр недействительных паспортов
    compressfile = downloadFile(fms_url)
    # Распаковываем архив в текущую директорию
    file = decompressFile(compressfile)
    # Подчищаем файл от битых данных
    num_passports, parsed_file = parseCSV(file)
    # Если запуск первый, то сохранить только бэкап
    if not pure_start:
        # Получение имени предыдущей версии реестра для вычисления дельты
        back_file = getBackFile(file)
        # Сравнение старой и новой версии баз, выделение дельты (инкрементальной, но можно и любой другой)
        calcDelta(back_file, parsed_file, num_passports)
    # Переименовываем файл
    os.rename(parsed_file, 'backup/parsed_file')

    t1 = time.time()
    print('Parser ended!')
    print('Time: ', '{:g}'.format((t1 - t0) // 60), 'm ', '{:.0f}'.format((t1 - t0) % 60), 's', sep='')


if __name__ == '__main__':
    main()
