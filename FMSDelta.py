# ----------------------------------------------------------------- #
# Скрипт скачивает реестр недействительных паспортов с ФМС МВД и    #
# вычисляет новые добавленные и удаленные паспорта                  #
# ----------------------------------------------------------------- #
# GlowByte                                                          #
# Автор: Гончаренко Дмитрий                                         #
# Версия: v1.5                                                      #
# ----------------------------------------------------------------- #

import sys
import time
from datetime import datetime
import requests  # pip3 install requests
import bz2
import os

# ------------------------------ Динамические переменные ------------------------------ #

# Ссылка на реестр недействительных паспортов с сайта МВД
fms_url = 'http://guvm.mvd.ru/upload/expired-passports/list_of_expired_passports.csv.bz2'
# Флаг запуска. Поставить 1 при первичном запуске. Скачивание + парсинг. Без дельты.
pure_start = 0
# Флаг завершения. По умолчанию очищает директорию от временных файлов.
clean_finish = 0
# Формат файлов
fformat = '.txt'
# Вид бэкап файлов. Сейчас: list_of_expired_passports_date.txt, delta_date.txt
postfix = datetime.today().strftime('%Y%m%d')  # _date.fformat
# Выбор функции вычисления дельты. Стабильная - медленная, включать при больших дельта
delta_type = 'fast'  # 'fast' / 'stable'
# Количество используемой оперативной памяти. Связано с размером блока паспортов.
ram_use = '2GB 500MB' # [MB|GB] exm: '2GB 700MB' 
# ОКАТО коды регионов
okato_codes = [1, 3, 4, 5, 7, 8, 11, 12, 14, 15, 17, 19, 20, 22, 24, 25, 26, 27, 28, 29, 32, 33, 34, 36, 37, 38, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99]

# ------------------------------------------------------------------------------------- #


# Проверяет состоит ли строка только из цифр
def isInteger(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


# Размер блока чтения (в строках). Больше значение - Больше расход RAM    
blocksize = 10 * 10 ** 6 # 1200MB
# Переводит RAM в размер блока в строках
def toBlock(ram):
    mblock = 600 # 5m ~ 600MB
    isGB = ram.find('GB')
    isMB = ram.find('MB')
    if isGB == isMB:
        print('Error in ram_use variable:', ram_use, 'Using default RAM')
        print('Example: \'1GB 200MB\'')
        logging('Using default RAM')
        ram = '1GB'
        isGB = 1
    print('RAM USING:', ram)
    logging('RAM USING: ' + ram)
    global blocksize
    sizeGB = 0
    sizeMB = 0
    if isGB != -1:
        partGB,ram = ram.split('GB')
        sizeGB = int(partGB)
    if isMB != -1:
        partMB,ram = ram.split('MB')
        sizeMB = int(partMB)
    size = sizeGB * 2**10 + sizeMB
    blocksize = int(size / mblock * (5 * 10 ** 6))
    print('Blocksize computed: ' +  str(blocksize // 10**6) + 'm passports!') 
    logging('Blocksize computed: ' +  str(blocksize // 10**6) + 'm passports!') 


# Запись в лог
def logging(text, noTime=0):
    with open('./log/log' + postfix, 'a') as log:
        if noTime:
            print(text, file=log)
        else:
            print(datetime.today().strftime('[%Y-%m-%d %H:%M:%S] ') + text, file=log)


# Скачивание файла по ссылке
def downloadFile(url):
    filename = url.split('/')[-1]
    print('Downloading:', filename)
    logging('Downloading: ' + filename)
    # Если файл уже существует - пропуск
    if os.path.exists(filename):
        print(filename, 'exists! Skipped!')
        logging(filename + ' exists! Skipped!')
        return filename

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
    print('Downloaded:', filename, str(size // 1024) + 'MB')
    logging('Downloaded: ' + filename + ' ' + str(size // 1024) + 'MB')
    return filename


# Разархивирование bz2
def decompressFile(filename='list_of_expired_passports.csv.bz2'):
    print('Extracting:', filename)
    logging('Extracting: ' + filename)
    # Если файл уже существует - пропуск
    if os.path.exists(filename[:-len(fformat)]):
        print(filename[:-len(fformat)], 'exists! Skipped!')
        logging(filename[:-len(fformat)] + ' exists! Skipped!')
        return filename[:-len(fformat)]

    with open(filename[:-len(fformat)], 'wb') as csvfile, open(filename, 'rb') as zipfile:
        z = bz2.BZ2Decompressor()
        for block in iter(lambda: zipfile.read(512 * 1024), b''):
            csvfile.write(z.decompress(block))
    print('Extracted', filename[:-len(fformat)])
    logging('Extracted ' + filename[:-len(fformat)])
    return filename[:-len(fformat)]


# Удаление всех данных кроме вида: 1234,123456 (считаются ошибочными)
def parseCSV(filename='list_of_expired_passports.csv'):
    print('Parsing:', filename)
    logging('Parsing ' + filename)
    pfilename = filename[:-len(fformat)] + postfix
    num = 0
    err = 0
    # Если файл уже существует - пропуск
    if os.path.exists(pfilename):
        with open(pfilename, 'r') as pfile:
            num = sum(1 for i in pfile)
        print(pfilename, 'exists!', num, 'passports! Skipped!')
        logging(pfilename + ' exists! ' + str(num) + ' passports! Skipped!')
        return num, pfilename
    
    with open(filename, 'r', encoding='utf8') as csvIN, \
            open(pfilename, 'w') as txtOUT, \
            open('brokenData.txt', 'w') as txtBroke:
        csvIN.readline()
        for line in csvIN:
            a, b = line.replace('\n', '').split(',')
            if len(a) == 4 and len(b) == 6 and (a+b).isdigit():
                txtOUT.write(a + b + '\n')
                num += 1
                if num % 10**5 == 0:
                    print('Passports:', num, end='\r')
            else:
                err += 1
                txtBroke.write(a + ',' + b + '\n')
        print('Parsed', num, 'passports!')
        print('File:', pfilename)
        print('Broken Data: brokenData.txt (' + str(err) + ')')
        logging('Parsed ' + str(num) + ' passports!\nFile: ' +
                pfilename + '\nBroken Data: brokenData.txt (' + str(err) + ')')
        return num, pfilename


# Поиск в директории ./backup самого последнего файла по postfix дате
def getBackFile(filename='list_of_expired_passports.csv'):
    print('Getting backup file to compare')
    logging('Getting backup file to compare')
    n = len(postfix) - 1
    flen = len(fformat) 
    f = []
    for root, dirs, files in os.walk('./backup'):
        f.extend(files)
        break
    if len(f) == 0:
        print('No backup files! Set \'pure_start = 1\' Abort.')
        logging('No backup files! Set \'pure_start = 1\' Abort.')
        exit()
    last = 0  # последний бэкап
    first = 0  # первый бэкап
    for file in f:
        print(file)
        if not isInteger(file[-n:-flen]):
            print('Postfix error: not a number! Abort.', file[-n:-flen])
            logging('Postfix error: not a number! Abort. ' + file[-n:-flen])
            exit()
        if last < int(file[-n:-flen]):
            last = int(file[-n:-flen])
            first = last if first == 0 else first
        if first > int(file[-n:-flen]):
            first = int(file[-n:-flen])
    print('Got first backup:', first)
    print('Got last backup:', last)
    logging('Got first backup: ' + str(first) +
            ' Got last backup: ' + str(last))
    return (filename[:-flen] + '_' + str(first) + fformat), (filename[:-flen] + '_' + str(last) + fformat)

# Для calcDeltaFast. Если дельта > 1гб сравнить до конца файла.
def calcSkip(file, stack, N, start_from, t):
    print('Delta ' + t + ' is too big, comparing to the end of file')
    logging('Delta ' + t + ' is too big, comparing to the end of file')
    n = 0
    with open(file, 'r') as txt:
        # for n in range(0, start_from):
            # txt.readline()
        # print('Starting from', start_from)
        for line in txt:
            n += 1
            elem = int(line)
            if elem in stack:
                stack.remove(elem)
            if n % 10**5 == 0:
                print(N - n, end='\r')

    print('Calclulated part of delta!')
    logging('Calclulated part of delta!')
    return stack

# Вычисление дельты (быстрая версия, 1 прогон) ~ 5 мин
# fileOld - предыдущая версия
# fileNew - новая версия
# N - количество людей в новой базе
def calcDeltaFast(fileOld, fileNew, N):
    print('Comparing:', fileOld, fileNew)
    logging('Comparing: ' + fileOld + ' ' + fileNew)
    with open('./backup/' + fileOld, 'r') as fold:
        print('Counting passports in', fileOld)
        O = sum(1 for i in fold)
        O -= 1
    print('Counted! (' + str(O) + ')')
    logging('Counted passports in ' + fileOld + ' (' + str(O) + ')')
    less_num = N if N < O else O
    # Вычисление
    print('Calculating delta')
    stackMinus = set()
    stackPlus = set()
    # for code in okato_codes:
    with open('deltaPlus' + postfix, 'w') as deltaPlus, open('deltaMinus' + postfix, 'w') as deltaMinus:
        k = 0
        with open(fileNew, 'r') as txtNEW, open('./backup/' + fileOld, 'r') as txtOLD:
            for lineO, lineN in zip(txtOLD, txtNEW):
                elemO = int(lineO)
                elemN = int(lineN)
                k += 1
                if k % 100000 == 0:
                    print(less_num - k, end='\r')
                if elemO != elemN:
                    stackMinus.add(elemO)
                    stackPlus.add(elemN)
                if k % (2 * 10 ** 6) == 0:
                    ins_ = stackMinus.intersection(stackPlus)
                    stackMinus.difference_update(ins_)
                    stackPlus.difference_update(ins_)
                    ins_.clear()
                    # Защита от переполнения RAM
                    if len(stackPlus) > blocksize:
                        stackPlus = calcSkip('./backup/' + fileOld, stackPlus, O,  k, 'plus')
                        for element in stackPlus:
                            print(element, end='\n', file=deltaPlus)
                        stackPlus.clear()
                    if len(stackMinus) > blocksize:
                        stackMinus = calcSkip(fileNew, stackMinus, N, k, 'minus')
                        for element in stackMinus:
                            print(element, end='\n', file=deltaMinus)
                        stackMinus.clear()

            for i in range(1, abs(N - O)):
                if i % (10 ** 6) == 0:
                    tmp_ = stackMinus.difference(stackPlus)
                    stackPlus.difference_update(stackMinus)
                    stackMinus = tmp_.copy()
                    tmp_.clear()
                if N > O:
                    elemN = int(txtNEW.readline())
                    stackPlus.add(elemN)
                else:
                    elemO = int(txtOLD.readline())
                    stackMinus.add(elemO)
                # Защита от переполнения RAM
                if len(stackPlus) > blocksize:
                    stackPlus = calcSkip('./backup/' + fileOld, stackPlus, O,  k, 'plus')
                    for element in stackPlus:
                        print(element, end='\n', file=deltaPlus)
                    stackPlus.clear()
                if len(stackMinus) > blocksize:
                    stackMinus = calcSkip(fileNew, stackMinus, N, k, 'minus')
                    for element in stackMinus:
                        print(element, end='\n', file=deltaMinus)
                    stackMinus.clear()

            tmp_ = stackMinus.difference(stackPlus)
            stackPlus.difference_update(stackMinus)
            stackMinus = tmp_.copy()
            tmp_.clear()

            print('Calculated! Writing delta to files.')
            logging('Calculated! Writing delta to files.')
            for element in stackPlus:
                print(element, end='\n', file=deltaPlus)
            for element in stackMinus:
                print(element, end='\n', file=deltaMinus)
            stackPlus.clear()
            stackMinus.clear()
    print('Compared!')
    logging('Compared!')


# Вычисление дельты (универсальная версия, если дельта > 1гб) ~ 40 мин
# fileOld - предыдущая версия
# fileNew - новая версия
# N - количество людей в новой базе
def calcDeltaStable(fileOld, fileNew, N):
    print('Comparing:', fileOld, fileNew)
    logging('Comparing: ' + fileOld + ' ' + fileNew)
    with open('deltaPlus' + postfix, 'w') as deltaPlus, \
            open('deltaMinus' + postfix, 'w') as deltaMinus:
        part = N if N <= blocksize else blocksize
        parts = N // part + 1
        n = 0
        setOld = set()
        setNew = set()
        print('Delta Plus computing')
        logging('Delta Plus computing')
        with open(fileNew, 'r') as txtNEW:
            for i in range(0, parts):
                for k, line in enumerate(txtNEW):
                    setNew.add(int(line))
                    if k == part - 1: break
                with open('./backup/' + fileOld, 'r') as txtOLD:
                    for n in range(0, i * part):
                        txtOLD.readline()
                    if n: print('Skipped', n)
                    for k, line in enumerate(txtOLD):
                        setOld.add(int(line))
                        if k % part == 0 and k > 0:
                            setNew.difference_update(setOld)
                            setOld.clear()
                            print('Checked:', k)
                            if len(setNew) == 0: break
                    if len(setNew) > 0:
                        print('Jump to start of file')
                        txtOLD.seek(0)
                        for k, line in enumerate(txtOLD):
                            setOld.add(int(line))
                            if k % part == 0 and k > 0:
                                setNew.difference_update(setOld)
                                setOld.clear()
                                print('Checked:', N - n + k)
                                if len(setNew) == 0 or k > n: break
                    if len(setNew) > 0 and len(setOld) > 0:
                        setNew.difference_update(setOld) # проверяем оставшиеся записи
                setOld.clear()
                for line in setNew:
                    print(line, end='\n', file=deltaPlus)
                setNew.clear()
        print('Delta Minus computing')
        logging('Delta Minus computing')
        n = 0
        with open('./backup/' + fileOld, 'r') as txtOLD:
            for i in range(0, parts):
                for k, line in enumerate(txtOLD):
                    setOld.add(int(line))
                    if k == part - 1: break
                with open(fileNew, 'r') as txtNEW:
                    for n in range(0, i * part):
                        txtNEW.readline()
                    if n: print('Skipped', n)
                    for k, line in enumerate(txtNEW):
                        setNew.add(int(line))
                        if k % part == 0 and k > 0:
                            setOld.difference_update(setNew)
                            setNew.clear()
                            print('Checked:', k)
                            if len(setOld) == 0: break
                    if len(setOld) > 0:
                        print('Jump to start of file')
                        txtNEW.seek(0)
                        for k, line in enumerate(txtNEW):
                            setNew.add(int(line))
                            if k % part == 0 and k > 0:
                                setOld.difference_update(setNew)
                                setNew.clear()
                                print('Checked:', N - n + k)
                                if len(setOld) == 0 or k > n: break
                    if len(setNew) > 0 and len(setOld) > 0:
                        setOld.difference_update(setNew)  # проверяем оставшиеся записи
                setNew.clear()
                for line in setOld:
                    print(line, end='\n', file=deltaMinus)
                setOld.clear()
    print('Compared!')
    logging('Compared!')


# Функция инициализации. При первичной настройке
def init():
    # Изменение постфикса
    global postfix
    postfix = '_' + postfix + fformat
    # При первичном запуске создать папку backup, delta, log
    if not os.path.isdir('./backup'):
        os.mkdir('./backup')
    if not os.path.isdir('./delta'):
        os.mkdir('./delta')
    if not os.path.isdir('./log'):
        os.mkdir('./log')
    if not os.path.exists('./log/log' + postfix):
        open('./log/log' + postfix, 'a').close()

    # Начальное логирование
    logging('# ----------------------------------------------------------------------------------------- #', 1)
    logging('New log starts: ' + datetime.today().strftime('%d/%m/%y %H:%M'), 1)
    logging('------------ Variables ------------', 1)
    logging('Start type: ' + ('pure' if pure_start else 'not pure'), 1)
    logging('Delta calculation type: ' + delta_type, 1)
    logging('Postfix style: ' + postfix, 1)
    logging('-----------------------------------', 1)
    if delta_type != 'stable' and delta_type != 'fast':
        print('delta_type error: \'stable\' or \'fast\' expected! Abort.')
        logging('delta_type error: \'stable\' or \'fast\' expected! Abort.')
        exit()
    toBlock(ram_use)    

# Функция завершения. Перенос файлов и очистка директории
def postprocessing(parsed_file, first_backup, file='list_of_expired_passports.csv', compressfile='list_of_expired_passports.csv.bz2'):
    print('Postprocessing')
    logging('Postprocessing', 1)

    # Переносим файлы в бэкап и дельту с заменой
    if os.path.exists('./backup/' + parsed_file) and os.path.exists(parsed_file):
        os.remove('./backup/' + parsed_file)
    if os.path.exists('./delta/deltaPlus' + postfix) and os.path.exists('deltaPlus' + postfix):
        os.remove('./delta/deltaPlus' + postfix)
    if os.path.exists('./delta/deltaMinus' + postfix) and os.path.exists('deltaMinus' + postfix):
        os.remove('./delta/deltaMinus' + postfix)
    if os.path.exists(parsed_file):
        os.rename(parsed_file, './backup/' + parsed_file)
    if os.path.exists('deltaPlus' + postfix):
        os.rename('deltaPlus' + postfix, './delta/deltaPlus' + postfix)
    if os.path.exists('deltaMinus' + postfix):
        os.rename('deltaMinus' + postfix, './delta/deltaMinus' + postfix)

    # Удаляем самый старый бэкап, если > 3
    f = []
    for root, dirs, files in os.walk('./backup'):
        f.extend(files)
        break
    if len(f) > 3 and os.path.exists('./backup/' + first_backup):
        os.remove('./backup/' + first_backup)
        print('./backup/' + first_backup + ' removed')
        logging('./backup/' + first_backup + ' removed', 1)

    # Очистка work directory
    if clean_finish:
        os.remove(compressfile)
        os.remove(file)
        print(compressfile + ' and ' + file + ' removed')
        logging(compressfile + ' and ' + file + ' removed', 1)
    logging('# ----------------------------------------------------------------------------------------- #', 1)


def main():
    print('Starts passports parser!')
    t0 = time.time()

    # Инициализация
    init()

    # Скачиваем реестр недействительных паспортов
    compressfile = downloadFile(fms_url)
    # Распаковываем архив в текущую директорию
    first_backup = file = decompressFile(compressfile)
    # Подчищаем файл от битых данных
    num_passports, parsed_file = parseCSV(file)
    # Если запуск первый, то сохранить только бэкап
    if not pure_start:
        # Получение имени предыдущей версии реестра для вычисления дельты
        first_backup, backup_file = getBackFile(file)
        # Сравнение старой и новой версии баз, выделение дельты
        if delta_type == 'fast':
            calcDeltaFast(backup_file, parsed_file, num_passports)
        else:  # stable
            calcDeltaStable(backup_file, parsed_file, num_passports)

    t1 = time.time()
    print('Parser ended!')
    print('Time: ', '{:g}'.format((t1 - t0) // 60), 'm ', '{:.0f}'.format((t1 - t0) % 60), 's', sep='')
    logging('---------------\nCompleted!', 1)
    logging('Time: ' + str('{:g}'.format((t1 - t0) // 60)) + 'm ' + str('{:.0f}'.format((t1 - t0) % 60)) + 's', 1)

    # Постобработка - завершение
    postprocessing(parsed_file, first_backup, file, compressfile)


if __name__ == '__main__':
    main()
