import psycopg2
import sys
from psycopg2 import Error
from settings import database_conf


try:
    connection = psycopg2.connect(**database_conf)
    cursor = connection.cursor()
    print('Подключение к базе данных установлено')
except (Exception, Error) as error:
    print('Ошибка при работе с PostgreSQL', error)

def main():
    if sys.argv[1] == '1':
        create_table()
    elif sys.argv[1] == '2':
        add_note(sys.argv[2], sys.argv[3], sys.argv[4])
    elif sys.argv[1] == '3':
        unique_output()
    elif sys.argv[1] == '4':
        auto_filling()
    elif sys.argv[1] == '5':
        filtered_output()

def create_table():
    try:
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS people(
            id SERIAL PRIMARY KEY,
            fio VARCHAR(50),
            birthdate DATE,
            gender CHAR(1)
        );
        ''')
        connection.commit()
        print(f"Таблица people успешно создана")
    except (Exception, Error) as err:
        print("Ошибка при работе с PostgreSQL", err)
    cursor.close()
    connection.close()
    print("Соединение с PostgreSQL закрыто")

def add_note(*args):
    try:
        cursor.execute('''
        INSERT INTO people(
            fio,
            birthdate,
            gender
        ) VALUES(%s, %s, %s);
        ''', args)
        connection.commit()
        print("Запись добавлена")
    except (Exception, Error) as err:
        print("Ошибка при работе с PostgreSQL", err)
    cursor.close()
    connection.close()
    print("Соединение с PostgreSQL закрыто")

def unique_output():
    pass

def auto_filling():
    pass

def filtered_output():
    pass

if __name__ == '__main__':
    if len(sys.argv) < 1:
        print('Некорректная команда, ознакомьтесь с руководством')
    else:
        main() 
