import dotenv
import requests
import os
from dotenv import load_dotenv
import random
import string
import schedule
import time
import datetime
import csv
import threading

result_name = 'result.csv'  # Имя файла с результатами
cwd_path = os.getcwd()  # Полный путь к текущему каталогу
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')  # Путь к файлу.env

id_to_get_list = {}  # словарь с timestamp по ключам id
api_url = "https://analytics.maximum-auto.ru/vacancy-test/api/v0.1/reports"  # основное тело для api запросов
headers = {"Authorization": "Bearer " + dotenv.get_key(dotenv_path, "TEST_TOKEN"), }  # заголовок запроса
if os.path.exists(dotenv_path):  # проверка наличия файла.env, загрузка при наличии файла
    load_dotenv(dotenv_path)


# Функция - генератор случайного id
def id_generator(size=64, chars=string.ascii_uppercase + string.digits + string.ascii_lowercase):
    return ''.join(random.choice(chars) for _ in range(size))


# Функция для отправки запроса на создания отчета с декоратором для запуска через schedule каждую минуту
# @schedule.repeat(schedule.every(1).minutes)
def post_reports():  # функция отправляет запрос на создание нового отчета на сервере. Должна вызываться каждую минуту
    temp_id = id_generator()  # создаём новый случайный ид в переменную
    id_to_post = {'id': temp_id}  # добавляем ид в словарь для запроса
    id_to_get_list[temp_id] = datetime.datetime.fromtimestamp(time.time()).strftime('%d-%m-%Y %H:%M:%S')
    try:
        with requests.session() as session:
            response = session.post(url=api_url, headers=headers,
                                    json=id_to_post)  # отправляем запрос на создание отчета на сервер
            match response.status_code:  # проверяем статус ответа и выводим исключения если статус не 201 (запрос на создание отчета успешно принят)
                case 401:
                    raise Exception("Неправильный токен")
                case 429:
                    raise Exception("Превышен лимит запросов")
                case 400:
                    raise Exception("Тело запроса не соответствует спецификации")
                case 409:
                    post_reports()  # вызываем функцию снова, если Отчет с таким id уже существует
                    raise Exception("Отчет с таким id уже существует")
                case 201:  # Если запрос принят, добавляем таймстамп в словарь по ключу id
                    id_to_get_list[temp_id] = datetime.datetime.fromtimestamp(time.time()).strftime('%d-%m-%Y %H:%M:%S')


    except:
        raise Exception(
            "Ошибка при отправке запроса на получение отчета из сервера")  # Если не удалось установить соединение с сервером, выводим исключение
        post_reports()  # И вызываем функцию снова


# Функция для отправки запроса на получение отчета с декоратором для запуска через schedule каждую минуту
# @schedule.repeat(schedule.every(1).seconds)
def get_reports():  # функция отправляет запрос на получение отчета из сервера. Должна вызываться каждую секунду
    double_id_list = id_to_get_list.copy()  # создаём копию словаря с id
    for time_id in double_id_list.items():  # перебираем словарь по ключам id
        id = time_id[0]  # получаем id из словаря
        time = time_id[1]  # получаем время из словаря
        try:
            with requests.session() as session:
                response = session.get(api_url + "/" + id,
                                       headers=headers)  # отправляем запрос на получение отчета из сервера
                match response.status_code:  # проверяем статус ответа и выводим исключения если статус не 200 (запрос на создание отчета успешно принят)
                    case 401:
                        raise Exception("Неправильный токен")
                    case 429:
                        raise Exception("Превышен лимит запросов")
                    case 400:
                        raise Exception("Тело запроса не соответствует спецификации")
                    case 202:  # Еще не готов, пропускаем эту иттерацию
                        continue
                    case 200:  # Успешный ответ
                        with open(result_name, 'a', newline='') as file:  # открываем файл
                            writer = csv.writer(file)
                            writer.writerow([time, response.json()['value']])  # записываем в csv
                        delete_reports(id, session)  # Удаляем отчет с таким id из сервера
                        id_to_get_list.pop(id)  # Удаляем элемент из словаря с таким id из списка
                    case 404:  # Не существует отчета с таким id
                        id_to_get_list.pop(id)  # Удаляем элемент из словаря с таким id из списка
        except:
            raise Exception(
                "Ошибка при отправке запроса на получение отчета из сервера")  # Если не удалось установить соединение с сервером, выводим исключение


# Функция для удаления отчета по  id с сервера
def delete_reports(id_to_delete, session):
    session.delete(api_url + "/" + id_to_delete, headers=headers)  # Запрос на удаление отчета с сервера


# Функция для запуска функций в отдельных потоках
def run_threading(func_to_run):
    job_thread = threading.Thread(target=func_to_run)
    job_thread.start()


def main():
    if not (os.path.isfile(result_name)):  # проверка наличия файла
        with open(result_name, 'w+', newline='') as file:  # Если файла нет создаем и закрываем файл
            file.close()  #
    schedule.every(1).minutes.do(run_threading, post_reports)  # Запускаем run_threading с аргументом post_reports
    # каждую минуту
    schedule.every(1).seconds.do(run_threading, get_reports)  # Запускаем run_threading с аргументом get_reports
    # каждую секунду


if __name__ == '__main__':
    main()
    while True:  # запускаем цикл для schedule
        schedule.run_pending()
        time.sleep(1)
