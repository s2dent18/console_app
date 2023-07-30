import psycopg2
import sys
import random
import time
from psycopg2 import Error
from faker import Faker
from transliterate import translit
from settings import database_conf
from base import second_name_base


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
    cursor.close()
    connection.close()
    print("Соединение с PostgreSQL закрыто")

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
        print(err)

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
        print(err)

def unique_output():
    try:
        cursor.execute('''
        SELECT DISTINCT ON (fio, birthdate) fio, TO_CHAR (birthdate, 'YYYY-MM-DD'), gender, EXTRACT (YEAR FROM AGE(birthdate))::INT FROM people ORDER BY fio;
        ''')
        result = cursor.fetchall()
        if result:
            for person in result:
                print(person)
        else:
            print('пусто')
    except (Exception, Error) as err:
        print(err)

def auto_filling():
    fake = Faker('ru_RU')
    hundred_data = []
    data = []
    lenght = 1000000
    start_time = time.time()
    print('Генерация 1000000 случайных записей ')
    printProgressBar(0, lenght, prefix = 'Прогресс:', suffix = 'Выполнено', length = 50)
    for _ in range(lenght):
        gender = random.choice(['M', 'F'])
        if gender == 'M':
            fio = " ".join([fake.last_name_male(), fake.first_name_male(), fake.middle_name_male()])
        else:
            fio = " ".join([fake.last_name_female(), fake.first_name_female(), fake.middle_name_female()])
        out = (translit(fio, language_code='ru', reversed=True), fake.date_of_birth(), gender)
        data.append(out)
        printProgressBar(_ + 1, lenght, prefix = 'Прогресс:', suffix = 'Выполнено', length = 50)
    print('')
    print('Выполняется загрузка в базу данных')
    cursor.executemany('''
    INSERT INTO people (fio, birthdate, gender) VALUES (%s, %s, %s);
    ''', data)
    for _ in range(100):
        fio = " ".join([random.choice(second_name_base), fake.first_name_male(), fake.middle_name_male()])
        out = (translit(fio, language_code='ru', reversed=True), fake.date_of_birth(), 'M')
        hundred_data.append(out)
    print("Сгенерировано 100 случайных записей мужского пола с фамилией, начинающейся с 'F'")
    cursor.executemany('''
    INSERT INTO people (fio, birthdate, gender) VALUES (%s, %s, %s);
    ''', hundred_data)
    end_time = time.time()
    connection.commit()
    print(f"Время выполнения запроса: {end_time - start_time:.2f} секунд")

def filtered_output():
    pass

def printProgressBar(iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)

if __name__ == '__main__':
    if len(sys.argv) < 1:
        print('Некорректная команда, ознакомьтесь с руководством')
    else:
        main() 
