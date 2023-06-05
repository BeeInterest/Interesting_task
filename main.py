from postgres_tools import PostgresTools
from sqlalchemy import create_engine
from math import sqrt

engine = create_engine("sqlite:///testdb.db", echo=True)
connection = engine.connect()

tool = SqlTools(engine, connection)

# Моделируем таблицу 1 в программе, либо создаем её в бд, и заполняем данными таблицу 1
tool.create_table1()
tool.add_data_table1()

# Моделируем таблицу 2 в программе, либо создаем её в бд, и заполняем данными таблицу 2
tool.create_table2()
tool.add_data_table2()

# Смотрим данные в таблицах
print(tool.get_data_from_table1())
print(tool.get_data_from_table2())

try:
    number = int(input("Введите число: "))
    side = int(input("Введите сторону квадрата: "))
    a = int(input("Введите сумму вклада: "))
    years = int(input("Введите срок вклада: "))

    def is_prime(number):
        num = int(sqrt(number))
        flag = True
        for i in range(1, num):
            if number % i == 0:
                flag = False
                break
        return flag


    print(f"Число является простым? {is_prime(number)}")


    def square(side):
        p = side * 4
        s = side * side
        d = sqrt(2) * side
        return "Периметр квадрата: {:} \nПлощадь квадрата: {:} \nДиагональ квадрата: {:.2f}".format(p, s, d)


    print(square(side))


    def bank(a, years):
        a *= 1.10
        years -= 1
        if years == 0:
            return "Счет пользователя стал: {:.2f}".format(a)
        else:
            return bank(a, years)


    print(bank(a, years))
except Exception:
    print("Получена ошибка при расчетах! Скорее всего вы не ввели число!")
