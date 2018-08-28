# ----------------------------------------------------------------- #
# Скрипт скачивает реестр недействительных паспортов с ФМС МВД и    #
# вычисляет новые добавленные в базу паспорта                       #
# ----------------------------------------------------------------- #
# GlowByte                                                          #
# Автор: Годлин Артем                                               #
# Версия: v0.2                                                      #
# ----------------------------------------------------------------- #

import sys
import csv
import time
from datetime import datetime
import requests  # pip3 install request
import bz2
import os

fms_url = 'http://guvm.mvd.ru/upload/expired-passports/list_of_expired_passports.csv.bz2'


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
    with open(filename[:-4], 'wb') as csvfile, open(filename, "rb") as zipfile:
        z = bz2.BZ2Decompressor()
        for block in iter(lambda: zipfile.read(200 * 1024), b''):
            csvfile.write(z.decompress(block))
    print('Extracted', filename[:-4])
    return filename[:-4]


# Удаление всех данных кроме вида: 1234,123456 (считаются ошибочными)
def parseCSV(filename):
    print('Parsing:', filename)
    with open(filename, 'r', newline='', encoding='utf8') as csvIN, open('parsed_' + filename, 'w',
                                                                         newline='') as csvOUT:
        readCSV = csv.reader(csvIN, delimiter=',')
        writeCSV = csv.writer(csvOUT, delimiter=',')
        writeCSV.writerow(next(readCSV))
        num = 0
        for line in readCSV:
            if len(line[0]) == 4 and len(line[1]) == 6 and isInteger(line[0]+line[1]):
                writeCSV.writerow({line[0]+line[1]})
                num += 1
        print('Parsed', num, 'passports!')
        return num


# Вычисление дельты
# file1 - предыдущая версия
# file2 - новая версия
# N - количество людей в новой базе
# blocksize - размер блока чтения. Больше значение - Больше расход RAM
def calcDelta(file1, file2, N, blocksize):
    print('Comparing:', file1, file2)
    with open(file2, 'r', newline='', encoding='utf8') as csvNEW, open('delta_' + datetime.today().strftime("%Y%m%d") + '.csv', 'w', newline='') as csvDELTA:
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
            with open(file1, 'r', newline='', encoding='utf8') as csvOLD:
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

    # Скачиваем реестр недействительных паспортов
    compressfile = downloadFile(fms_url)
    # Распаковываем архив в текущую директорию
    file = decompressFile(compressfile)
    # file = 'list_of_expired_passports.csv'
    os.rename('parsed_'+file, 'parsed_'+file + datetime.today().strftime("%Y%m%d"))
    # Подчищаем файл от битых данных
    num_passports = parseCSV(file)
    print('Parser ended!')
    # Сравнение старой и новой версии баз, выделение дельты (инкрементальной, но можно и любой другой)
    # Пока сравнивается один и тот же файл, дописать тут выбор файла
    calcDelta('parsed_'+file, 'parsed_'+file + datetime.today().strftime("%Y%m%d"), num_passports, 15 * 10 ** 6)
    os.rename('parsed_'+file + datetime.today().strftime("%Y%m%d"), 'backup/parsed_'+file + datetime.today().strftime("%Y%m%d"))
    t1 = time.time()

    print('Time: ', '{:g}'.format((t1 - t0) // 60), 'm ', '{:.0f}'.format((t1 - t0) % 60), 's', sep='')


if __name__ == '__main__':
    main()
