import csv
import random
from faker import Faker
import psycopg2
from psycopg2 import sql
from googletrans import Translator
from dotenv import load_dotenv
import os

USERS_COUNT = 10000
ANNOUNCEMENTS_COUNT = 7234
MESSAGES_COUNT = 50000
FAVORITES_COUNT = 6843
REVIEWS_COUNT = 5435
TRANSACTIONS_COUNT = 6125

load_dotenv()

fake = Faker('ru_RU')
translator = Translator()

def get_connection():
    """Создает и возвращает соединение с базой данных PostgreSQL."""
    try:
        return psycopg2.connect(
            # host='localhost',
            host=os.getenv('DB_HOST'),  # Получаем хост из переменных окружения
            dbname=os.getenv('DB_NAME'),  # Получаем имя базы данных из переменных окружения
            user=os.getenv('DB_USER'),  # Получаем имя пользователя из переменных окружения
            password=os.getenv('DB_PASSWORD'),  # Получаем пароль из переменных окружения
            options="-c client_encoding=utf8"  # Указание кодировки

        )
    except Exception as e:
        print(f"Ошибка подключения: {e}")
        raise
    
def truncate_tables():
    """Очищает все таблицы в базе данных."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    TRUNCATE TABLE "User", 
                                    "Announcement", 
                                    "Messages", 
                                    "Review", 
                                    "Transactions", 
                                    "Favorites" 
                    CASCADE;
                """)
                conn.commit()
                print("Все таблицы успешно очищены.")
    except Exception as e:
        print(f"Ошибка при очистке таблиц: {e}")


def create_tables():
    """Создает необходимые таблицы в базе данных."""
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS "User" (
                    UserID SERIAL PRIMARY KEY,
                    Name VARCHAR(100),
                    Email VARCHAR(100) UNIQUE,
                    Phone VARCHAR(20),  -- Увеличиваем размер до 20 символов
                    PasswordHash VARCHAR(255),
                    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS "Announcement" (
                    AnnouncementID SERIAL PRIMARY KEY,
                    UserID INT REFERENCES "User"(UserID),
                    Make VARCHAR(50),
                    Model VARCHAR(50),
                    Year INT,
                    Mileage INT,
                    Price DECIMAL(10, 2),
                    Description TEXT,
                    Condition VARCHAR(50),
                    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS "Messages" (
                    MessageID SERIAL PRIMARY KEY,
                    MessageText TEXT,
                    SenderID INT REFERENCES "User"(UserID),
                    ReceiverID INT REFERENCES "User"(UserID),
                    AnnouncementID INT REFERENCES "Announcement"(AnnouncementID)
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS "Review" (
                    ReviewID SERIAL PRIMARY KEY,
                    Rating INT CHECK (Rating BETWEEN 1 AND 5),
                    Comment TEXT,
                    UserID INT REFERENCES "User"(UserID),
                    AnnouncementID INT REFERENCES "Announcement"(AnnouncementID)
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS "Transactions" (
                    TransactionID SERIAL PRIMARY KEY,
                    Price DECIMAL(10, 2),
                    BuyerID INT REFERENCES "User"(UserID),
                    SellerID INT REFERENCES "User"(UserID),
                    AnnouncementID INT REFERENCES "Announcement"(AnnouncementID)
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS "Favorites" (
                    FavoriteID SERIAL PRIMARY KEY,
                    UserID INT REFERENCES "User"(UserID),
                    AnnouncementID INT REFERENCES "Announcement"(AnnouncementID)
                );
            """)
            conn.commit()

def generate_phone_number():
    """Генерирует номер телефона в формате +7-9XX-XXX-XX-XX."""
    operator_code = random.choice(['915', '925', '926', '999', '906', '910'])
    remaining_digits = ''.join(random.choices('0123456789', k=7))
    return f'+7-{operator_code}-{remaining_digits[:3]}-{remaining_digits[3:5]}-{remaining_digits[5:]}'

def fill_users():
    """Заполняет таблицу пользователей с случайными данными."""
    values_list = []
    existing_emails = set()  # Множество для хранения существующих email

    for _ in range(50):  # Количество пользователей
        name = fake.name()  # Генерация русского имени и фамилии
        email = fake.email()

        # Убедитесь, что email уникален
        while email in existing_emails:
            email = fake.email()  # Генерируем новый email, если он уже существует
        
        existing_emails.add(email)  # Добавляем новый email в множество
        phone = generate_phone_number()
        password_hash = fake.password()  # Здесь можно использовать хеширование пароля

        # Проверяем длину значений перед добавлением в список
        if len(name) <= 50 and len(email) <= 50 and len(phone) <= 20 and len(password_hash) <= 100:
            values_list.append((name, email, phone, password_hash))
        else:
            print(f"Пропущена запись из-за превышения длины: {name}, {email}, {phone}, {password_hash}")

    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                for user in values_list:
                    # Проверяем существует ли уже пользователь с таким email
                    cursor.execute("SELECT COUNT(*) FROM \"User\" WHERE Email = %s", (user[1],))
                    count = cursor.fetchone()[0]

                    if count == 0:  # Если пользователь не найден, вставляем его
                        insert_query = sql.SQL('INSERT INTO "User"(Name, Email, Phone, PasswordHash) VALUES (%s, %s, %s, %s)')
                        cursor.execute(insert_query, user)
                    else:
                        print(f"Пользователь с email {user[1]} уже существует. Пропускаем.")
                
                conn.commit()
                print("Таблица 'User' успешно заполнена.")
    except Exception as e:
        print(f"Ошибка при заполнении таблицы 'User': {e}")

# def fill_users():
#     """Заполняет таблицу пользователей с случайными данными."""
#     values_list = []
#     existing_emails = set()  # Множество для хранения существующих email

#     for _ in range(USERS_COUNT):  # Количество пользователей
#         name = fake.name()
#         email = fake.email()

#         # Убедитесь, что email уникален
#         while email in existing_emails:
#             email = fake.email()  # Генерируем новый email, если он уже существует
        
#         existing_emails.add(email)  # Добавляем новый email в множество
#         phone = generate_phone_number()
#         password_hash = fake.password()  # Здесь можно использовать хеширование пароля

#         values_list.append((name, email, phone, password_hash))

#     try:
#         with get_connection() as conn:
#             with conn.cursor() as cursor:
#                 for user in values_list:
#                     # Проверяем существует ли уже пользователь с таким email
#                     cursor.execute("SELECT COUNT(*) FROM \"User\" WHERE Email = %s", (user[1],))
#                     count = cursor.fetchone()[0]

#                     if count == 0:  # Если пользователь не найден, вставляем его
#                         insert_query = sql.SQL('INSERT INTO "User"(Name, Email, Phone, PasswordHash) VALUES (%s, %s, %s, %s)')
#                         cursor.execute(insert_query, user)
#                     else:
#                         print(f"Пользователь с email {user[1]} уже существует. Пропускаем.")
                
#                 conn.commit()
#                 print("Таблица 'User' успешно заполнена.")
#     except Exception as e:
#         print(f"Ошибка при заполнении таблицы 'User': {e}")

def fill_announcements():
    """Заполняет таблицу объявлений, используя данные из CarBrandsAndModels."""
    user_ids = []
    brand_models = []
    
    with get_connection() as conn:
        with conn.cursor() as cursor:
            # Получаем UserID из таблицы User
            cursor.execute("SELECT UserID FROM \"User\";")
            user_ids = [row[0] for row in cursor.fetchall()]
            
            # Получаем все марки и модели из таблицы CarBrandsAndModels
            cursor.execute("SELECT BrandName, ModelName FROM \"CarBrandsAndModels\";")
            brand_models = cursor.fetchall()  # Получаем все марки и модели

    if not user_ids or not brand_models:
        print("Нет пользователей или марок автомобилей для создания объявлений.")
        return

    values_list = []
    
    for _ in range(ANNOUNCEMENTS_COUNT):  # Количество объявлений
        user_id = random.choice(user_ids)
        brand_model = random.choice(brand_models)  # Выбираем случайную пару (BrandName, ModelName)
        make = brand_model[0]
        model_name = brand_model[1]

        year = random.randint(2000, 2023)
        mileage = random.randint(0, 200000)
        price = round(random.uniform(1000, 50000), 2) * 90
        description = fake.text(max_nb_chars=200)
        condition = random.choice(['New', 'Used', 'Refurbished'])
        
        values_list.append((user_id, make, model_name, year, mileage, price, description, condition))

    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                insert_query = sql.SQL('INSERT INTO "Announcement"(UserID, Make, Model, Year, Mileage, Price, Description, Condition) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)')
                cursor.executemany(insert_query ,values_list)
                conn.commit()
                print("Таблица 'Announcement' успешно заполнена.")
    except Exception as e:
        print(f"Ошибка при заполнении объявлений: {e}")

def fill_messages():
    """Заполняет таблицу сообщений."""
    user_ids = []
    announcement_ids = []
    
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT UserID FROM \"User\";")
            user_ids = [row[0] for row in cursor.fetchall()]
            
            cursor.execute("SELECT AnnouncementID FROM \"Announcement\";")
            announcement_ids = [row[0] for row in cursor.fetchall()]

    if not user_ids or not announcement_ids:
        print("Нет пользователей или объявлений для создания сообщений.")
        return

    values_list = []
    
    for _ in range(MESSAGES_COUNT):  # Количество сообщений
        sender_id = random.choice(user_ids)
        receiver_id = random.choice(user_ids)
        
        while receiver_id == sender_id:  # Убедимся в уникальности отправителя и получателя
            receiver_id = random.choice(user_ids)
        
        message_text = fake.text(max_nb_chars=100)
        announcement_id = random.choice(announcement_ids)

        values_list.append((message_text, sender_id, receiver_id, announcement_id))

    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                insert_query = sql.SQL('INSERT INTO "Messages"(MessageText, SenderID, ReceiverID, AnnouncementID) VALUES (%s ,%s ,%s ,%s)')
                cursor.executemany(insert_query ,values_list)
                conn.commit()
                print("Таблица 'Messages' успешно заполнена.")
    except Exception as e:
        print(f"Ошибка при заполнении сообщений: {e}")

def fill_reviews():
    """Заполняет таблицу отзывов."""
    user_ids = []
    announcement_ids = []
    
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT UserID FROM \"User\";")
            user_ids = [row[0] for row in cursor.fetchall()]
            
            cursor.execute("SELECT AnnouncementID FROM \"Announcement\";")
            announcement_ids = [row[0] for row in cursor.fetchall()]

    if not user_ids or not announcement_ids:
        print("Нет пользователей или объявлений для создания отзывов.")
        return

    values_list = []
    
    for _ in range(REVIEWS_COUNT):  # Количество отзывов
        user_id = random.choice(user_ids)
        announcement_id = random.choice(announcement_ids)
        
        rating = random.randint(1, 5)
        comment = fake.text(max_nb_chars=200)

        values_list.append((rating ,comment ,user_id ,announcement_id))

    try:
         with get_connection() as conn:
             with conn.cursor() as cursor:
                 insert_query= sql.SQL('INSERT INTO "Review"(Rating ,Comment ,UserID ,AnnouncementID) VALUES (%s ,%s ,%s ,%s)')
                 cursor.executemany(insert_query ,values_list)
                 conn.commit()
                 print("Таблица 'Reviews' успешно заполнена.")
    except Exception as e:
        print(f"Ошибка при заполнении отзывов: {e}")

def fill_transactions():
     """Заполняет таблицу транзакций."""
     user_ids= []
     announcement_ids= []
     
     with get_connection() as conn:
         with conn.cursor() as cursor:
             cursor.execute("SELECT UserID FROM \"User\";")
             user_ids= [row[0] for row in cursor.fetchall()]
             
             cursor.execute("SELECT AnnouncementID FROM \"Announcement\";")
             announcement_ids= [row[0] for row in cursor.fetchall()]

     if not user_ids or not announcement_ids:
         print("Нет пользователей или объявлений для создания транзакций.")
         return

     values_list= []
     
     for _ in range(TRANSACTIONS_COUNT):  # Количество транзакций
         buyer_id= random.choice(user_ids)
         seller_id= random.choice(user_ids)

         while seller_id == buyer_id:  # Убедимся в уникальности покупателя и продавца
             seller_id= random.choice(user_ids)

         announcement_id= random.choice(announcement_ids)
         price= round(random.uniform(1000 ,50000) ,2)

         values_list.append((price ,buyer_id ,seller_id ,announcement_id))

     # Вставка данных в таблицу Transactions
     try:
         with get_connection() as conn:
             with conn.cursor() as cursor:
                 insert_query= sql.SQL('INSERT INTO "Transactions"(Price ,BuyerID ,SellerID ,AnnouncementID) VALUES (%s ,%s ,%s,%s)')
                 cursor.executemany(insert_query ,values_list)
                 conn.commit()
                 print("Таблица 'Transactions' успешно заполнена.")
     except Exception as e:
         print(f"Ошибка при заполнении транзакций: {e}")

def fill_favorites():
     """Заполняет таблицу избранных объявлений."""
     user_ids= []
     announcement_ids= []

     with get_connection() as conn:
         with conn.cursor() as cursor:
             cursor.execute("SELECT UserID FROM \"User\";")
             user_ids= [row[0] for row in cursor.fetchall()]
             
             cursor.execute("SELECT AnnouncementID FROM \"Announcement\";")
             announcement_ids= [row[0] for row in cursor.fetchall()]

     if not user_ids or not announcement_ids:
          print("Нет пользователей или объявлений для создания избранных.")
          return

     values_list= []

     for _ in range(FAVORITES_COUNT):  # Количество избранных
          user_id= random.choice(user_ids)
          announcement_id= random.choice(announcement_ids)

          values_list.append((user_id ,announcement_id))

     # Вставка данных в таблицу Favorites
     try:
         with get_connection() as conn:
             with conn.cursor() as cursor:
                 insert_query= sql.SQL('INSERT INTO "Favorites"(UserID ,AnnouncementID) VALUES (%s ,%s)')
                 cursor.executemany(insert_query ,values_list)
                 conn.commit()
                 print("Таблица 'Favorites' успешно заполнена.")
     except Exception as e:
         print(f"Ошибка при заполнении избранных: {e}")

# def update_reviews_to_russian():
#     """Обновляет все отзывы на русский язык."""
#     try:
#         with get_connection() as conn:
#             with conn.cursor() as cursor:
#                 # Пример текста отзыва на русском языке
#                 new_comment = 'Это пример отзыва на русском языке.'
                
#                 # Обновляем все отзывы
#                 cursor.execute("UPDATE \"Review\" SET Comment = %s WHERE Comment IS NOT NULL;", (new_comment,))
#                 conn.commit()
#                 print("Все отзывы успешно обновлены на русский язык.")
#     except Exception as e:
#         print(f"Ошибка при обновлении отзывов: {e}")

# Вызов функции
# update_reviews_to_russian()

truncate_tables()

# Создание необходимых таблиц перед заполнением данными.
create_tables()

# Заполнение таблиц.
fill_users()
fill_announcements()
fill_messages()
fill_reviews()
fill_transactions()
fill_favorites()

print("Все таблицы успешно заполнены.")