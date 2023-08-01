import random
import re
import sys
import time
from datetime import date

import psycopg2
from faker import Faker
from progress.bar import Bar
from psycopg2 import Error
from transliterate import translit

from base import second_name_base
from settings import database_conf

# Подключение к базе данных
try:
    connection = psycopg2.connect(**database_conf)
    cursor = connection.cursor()
    print("Подключение к базе данных установлено")
except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", error)

def main():
    """
    Основная функция. Проверяет первый аргумент и выполняет соответствующую ему функцию.
    """
    if sys.argv[1] == "1":
        create_table()
    elif sys.argv[1] == "2":
        add_note(sys.argv[2].title(), sys.argv[3], sys.argv[4])
    elif sys.argv[1] == "3":
        unique_output()
    elif sys.argv[1] == "4":
        auto_filling()
    elif sys.argv[1] == "5":
        filtered_output()
    elif sys.argv[1] == "6":
        optimize_database()
    cursor.close()
    connection.close()
    print("Соединение с PostgreSQL закрыто")

def check_arg():
    """
    Функция проверки всех введенных аргументов.
    """
    if len(sys.argv) == 2 and sys.argv[1] in ["1", "3", "4", "5", "6"]:
        return True
    if len(sys.argv) == 5 and sys.argv[1] == "2":
        try:
            date.fromisoformat(str(sys.argv[3])) # Проверка на возможность конвертации введённой даты из Str в Date
        except (Exception, Error) as error:
                return False
        result = sys.argv[4] in ["M", "F"] and re.match(r'^[a-zA-Z\s]+\Z', sys.argv[2]) is not None # Одновременная проверка аргумента "гендер" и англ. языка в ФИО 
        return result
    return False

def create_table():
    """
    Функция создания таблицы в БД.
    """
    cursor.execute('''
    SELECT EXISTS (SELECT FROM pg_tables WHERE tablename  = "people" );
    ''')
    check = cursor.fetchone()
    if check[0]:
        print("База данных уже создана")
        return
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

def add_note(*args):
    """
    Функция создания записи в таблице.
    """
    cursor.execute('''
    INSERT INTO people(
        fio,
        birthdate,
        gender
    ) VALUES(%s, %s, %s);
    ''', args)
    connection.commit()
    print("Запись добавлена")

def unique_output():
    """
    Функция вывода всех строк с уникальным значением ФИО + дата, отсортированным по ФИО.
    """
    cursor.execute('''
    SELECT DISTINCT ON (fio, birthdate) fio, TO_CHAR (birthdate, "YYYY-MM-DD"), gender, EXTRACT (YEAR FROM AGE (birthdate))::INT FROM people ORDER BY fio;
    ''')
    result = cursor.fetchall()
    if result:
        for person in result:
            print(person)
    else:
        print("Пусто")

def auto_filling():
    """
    Функция генерации и автоматического заполнения строк в БД.
    Генерирует миллион случайных записей. Генерирует сто случайных записей с условиями:
    Пол мужской и ФИО начинается с "F".
    """
    fake = Faker("ru_RU")
    hundred_data = []
    data = []
    len = 100
    print("Генерация 1000000 случайных записей:")
    bar = Bar("Прогресс", max=len)
    for _ in range(len):
        gender = random.choice(["M", "F"])
        if gender == "M":
            fio = " ".join([fake.last_name_male(), fake.first_name_male(), fake.middle_name_male()])
        else:
            fio = " ".join([fake.last_name_female(), fake.first_name_female(), fake.middle_name_female()])
        out = (translit(fio, language_code='ru', reversed=True), fake.date_of_birth(), gender)
        data.append(out)
        bar.next()
    bar.finish()
    print("Выполняется загрузка в базу данных", end="\r")
    cursor.executemany('''
    INSERT INTO people (fio, birthdate, gender) VALUES (%s, %s, %s);
    ''', data)
    for _ in range(100):
        fio = " ".join([random.choice(second_name_base), fake.first_name_male(), fake.middle_name_male()])
        out = (translit(fio, language_code="ru", reversed=True), fake.date_of_birth(), "M")
        hundred_data.append(out)
    print("Сгенерировано 100 случайных записей мужского пола с фамилией, начинающейся с 'F'")
    cursor.executemany('''
    INSERT INTO people (fio, birthdate, gender) VALUES (%s, %s, %s);
    ''', hundred_data)
    connection.commit()

def filtered_output():
    """
    Функция вывода из таблицы строк по критериям:
    Пол мужской и ФИО начинается с "F".
    Производит замер времени выполнения.
    """
    start_time = time.time()
    cursor.execute('''
        SELECT fio, TO_CHAR (birthdate, "YYYY-MM-DD"), gender FROM people WHERE gender = "M" AND substr(fio, 1, 1) = "F"
        ''')
    result = cursor.fetchall()
    middle_time = time.time()
    if result:
        for person in result:
            print(person)
    else:
        print("Пусто")
    end_time = time.time()
    print(f"Время выполнения запроса к БД: {middle_time-start_time} секунд")
    print(f"Время обработки вывода: {end_time-middle_time} секунд")
    print(f"Общее время выполнения: {end_time-start_time} секунд")

def optimize_database():
    """
    Функция партицирующая таблицу на две подтаблицы:
    - таблица строк с мужским полом и ФИО, начинающимся на "F",
    - таблица со всеми остальными строками.
    Разделение позволяет ускорить работу с БД.
    """
    cursor.execute('''
    SELECT EXISTS (SELECT FROM pg_tables WHERE tablename  = "people_f" );
    ''')
    check = cursor.fetchone()
    if check[0] == True:
        print("База данных уже оптимизирована")
        return
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS people_optimized(
        id SERIAL PRIMARY KEY,
        fio VARCHAR(50),
        birthdate DATE,
        gender CHAR(1)
    );
    CREATE TABLE people_f () inherits (people_optimized);
    CREATE TABLE people_other () inherits (people_optimized);
    ALTER TABLE people_f
    ADD CONSTRAINT people_f_check_lit_key
    CHECK (gender = "M" AND substr(fio, 1, 1) = "F");
    ALTER TABLE people_other
    ADD CONSTRAINT people_other_check_lit_key
    CHECK (NOT (gender = "M" AND substr(fio, 1, 1) = "F"));
    INSERT INTO people_f SELECT * FROM people WHERE gender = "M" AND substr(fio, 1, 1) = "F";
    INSERT INTO people_other SELECT * FROM people WHERE NOT (gender = "M" AND substr(fio, 1, 1) = "F");
    ALTER TABLE people RENAME TO people_old;
    ALTER TABLE people_optimized RENAME TO people;
    SELECT pg_catalog.setval(pg_get_serial_sequence("people", "id"), (SELECT MAX(id) FROM people));
    DROP TABLE people_old;
    CREATE OR REPLACE FUNCTION people_partition_trigger()
    RETURNS TRIGGER AS $$
    BEGIN
        IF (NEW.gender = "M" AND substr(NEW.fio, 1, 1) = "F")
        THEN
            INSERT INTO people_f VALUES (NEW.*);
        ELSE
            INSERT INTO people_other VALUES (NEW.*);
        END IF;
    RETURN NULL;
    END;
    $$
    LANGUAGE plpgsql;
    CREATE TRIGGER people_partition_trigger
    BEFORE INSERT ON people
    FOR EACH ROW EXECUTE PROCEDURE people_partition_trigger();
    ''')
    connection.commit()

if __name__ == "__main__":
    if check_arg() == True:
        main()
    else:
        print("Некорректные аргументы, ознакомьтесь с руководством")
