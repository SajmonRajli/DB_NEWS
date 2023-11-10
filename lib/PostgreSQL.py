# -*- coding: utf-8 -*-
import configparser
import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from lib.DatabaseInterface import DatabaseInterface

# Считываем учетные данные
config = configparser.ConfigParser()
config.read("config_database.ini")
# config database
user=config['database']['user']
password=config['database']['password']
host=config['database']['host']
port=config['database']['port']
class Database(DatabaseInterface):
    def __init__(self, database: str):
        self.connect = psycopg2.connect(user=user,
                                    password=password,
                                    host=host,
                                    port=port,
									database=database)
        self.connect.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        # Курсор для выполнения операций с базой данных
        self.cursor = self.connect.cursor()

        # создание таблицы для хранения ключей
        text_SQL = f'''
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY ,
            name VARCHAR(255) NOT NULL UNIQUE,
            key VARCHAR(255) NOT NULL UNIQUE
        );'''
        self.cursor.execute(text_SQL)
        self.connect.commit()
        
    # добавляем пользователя и его ключ в таблицу users
    def add_user(self, user: str, key: str):
        try:
            text_SQL = f"INSERT INTO users (name, key) VALUES ('{user}', '{key}')"
            self.cursor.execute(text_SQL)
            self.connect.commit()
            return True
        except Exception as error:
            return error
    
    # возвращаем ключ для пользователя
    def check_user(self, user: str):
        try:
            text_SQL = f"SELECT key FROM users WHERE name = '{user}'"
            self.cursor.execute(text_SQL)
            key = self.cursor.fetchone()
            if key != None:
                return key[0]
            else: 
                return None
        except Exception as error:
            return error


    def create_table(self, table: str):
        # создаем таблицу
        # (надо подумать над длиной текста в varchar)
        try:
            text_SQL = f'''
            CREATE TABLE IF NOT EXISTS {table} (
                id SERIAL PRIMARY KEY ,
                resembling INTEGER[],
                source TEXT NOT NULL,
                public_date TIMESTAMP NOT NULL,
                channel_id VARCHAR(255),   
                channel_name VARCHAR(255) NOT NULL,
                channel_link VARCHAR(255) NOT NULL,
                channel_subs INTEGER default 0,
                message_id INTEGER,
                is_toggled BOOLEAN,
                message_text TEXT,
                source_text TEXT,
                views INTEGER,
                rel_size FLOAT,
                message_photo VARCHAR(255)[],
                sourse_photo VARCHAR(255)[],
                photo BYTEA,
                container_photo VARCHAR(255),
                message_video VARCHAR(255)[],
                sourse_video VARCHAR(255)[],
                video BYTEA,
                container_video VARCHAR(255),
                difference_time FLOAT,
                AllGroup VARCHAR(255)[],
                Allteg VARCHAR(255)[]
            );
            '''
            self.cursor.execute(text_SQL)

            text_SQL = f'''
            CREATE TABLE IF NOT EXISTS {table}_tags (
                id INTEGER PRIMARY KEY,
                id_parent INTEGER default 0,
                parent VARCHAR(255) default '-',
                name VARCHAR(255) NOT NULL,	
                searchObject VARCHAR(255) default '-',		
                value VARCHAR(255) default '-',	
                type VARCHAR(255) default '-'
            );
            '''
            self.cursor.execute(text_SQL)

            text_SQL = f'''
            CREATE TABLE IF NOT EXISTS {table}_sources (
                id SERIAL PRIMARY KEY ,
                link VARCHAR(255) NOT NULL,
                name VARCHAR(255) NOT NULL,
                type VARCHAR(255) NOT NULL

            );
            '''
            self.cursor.execute(text_SQL)

            text_SQL = f'''
            CREATE TABLE IF NOT EXISTS {table}_subscribers (
                id INTEGER PRIMARY KEY,
                name VARCHAR(255) NOT NULL
            )
            '''
            self.cursor.execute(text_SQL)

            self.connect.commit()
            return True
        except Exception as error:
            return error

    def delete_table(self, table: str):
        try:
            text_SQL = f"DROP TABLE {table}"
            self.cursor.execute(text_SQL)
            text_SQL = f"DROP TABLE {table}_tags"
            self.cursor.execute(text_SQL)
            text_SQL = f"DROP TABLE {table}_sources"
            self.cursor.execute(text_SQL)
            text_SQL = f"DROP TABLE {table}_subscribers"
            self.cursor.execute(text_SQL)
            self.connect.commit()
            return True
        except Exception as error:
            return error
    

    def add_data(self, table: str, data: list):
        try:
            text_SQL = ''
            for row in data:
                text_SQL += "INSERT INTO {} ({}) VALUES ({});".format(table, ', '.join(key for key in row.keys()), ', '.join(f"'{str(value)}'" for value in row.values()))
            # создаем запрос для ввода данных
            self.cursor.execute(text_SQL)
            self.connect.commit()
            return True
        except Exception as error:
            return error

    def get_data(self, table: str, where: list = None):
        try:
            text_SQL = f"SELECT * FROM {table}"
            if where:
                text_SQL += " WHERE " + " AND ".join(f"{key} = '{value}'" for key, value in where.items())
            self.cursor.execute(text_SQL)
            self.connect.commit()
            rows = self.cursor.fetchall()
            data = []
            for row in rows:
                data.append(dict(zip([desc[0] for desc in self.cursor.description], row)))
            return data
        except Exception as error:
            return error

    def upd_data(self, table: str, data: list):
        try:
            text_SQL = ''
            for row in data:
                dataset = ', '.join(f"{key} = '{value}'" for key, value in row.items())
                text_SQL += f"UPDATE {table} SET {dataset} WHERE id = {row['id']};"
            print(text_SQL)
            self.cursor.execute(text_SQL)
            self.connect.commit()
            return True
        except Exception as error:
            return error


    def del_data(self, table: str, data: list):
        try:
            ids = ", ".join(str(id) for id in data)
            text_SQL = f"DELETE FROM {table} WHERE id IN ({ids})"
            self.cursor.execute(text_SQL)
            self.connect.commit()
            return True
        except Exception as error:
            return error



    def get_data_by_date(self, table: str, date_start: str = None, date_end: str = None):
        try:
            text_SQL = f"SELECT * FROM {table}"
            if date_start and date_end:
                text_SQL += f" WHERE public_date BETWEEN '{date_start}' AND '{date_end}'"
            elif date_start:
                text_SQL += f" WHERE public_date >= '{date_start}'"
            elif date_end:
                text_SQL += f" WHERE public_date <= '{date_end}'"
            self.cursor.execute(text_SQL)
            self.connect.commit()
            rows = self.cursor.fetchall()
            data = []
            for row in rows:
                data.append(dict(zip([desc[0] for desc in self.cursor.description], row)))
            return data
        except Exception as error:
            return error

    def add_tags(self, table: str, tags: list):
        try:
            text_SQL = ''
            for tag in tags:
                text_SQL += "INSERT INTO {} ({}) VALUES ({});".format(f"{table}_tags", ', '.join(key for key in tag.keys()), ', '.join(f"'{str(value)}'" for value in tag.values()))
            # создаем запрос для ввода данных
            self.cursor.execute(text_SQL)
            self.connect.commit()
            return True
        except Exception as error:
            return error
        
    def get_tags(self, table: str):
        try:
            text_SQL = f"SELECT * FROM {table}_tags"
            self.cursor.execute(text_SQL)
            self.connect.commit()
            rows = self.cursor.fetchall()
            data = []
            for row in rows:
                data.append(dict(zip([desc[0] for desc in self.cursor.description], row)))
            return data
        except Exception as error:
            return error
        
    def upd_tags(self, table: str, tags: list):
        try:
            text_SQL = ''
            for tag in tags:
                dataset = ', '.join(f"{key} = '{value}'" for key, value in tag.items())
                text_SQL += f"UPDATE {table}_tags SET {dataset} WHERE id = {tag['id']};"
            self.cursor.execute(text_SQL)
            self.connect.commit()
            return True
        except Exception as error:
            return error
        
    def del_tags(self, table: str, tags: list):
        try:
            tags = ", ".join(str(tag) for tag in tags)
            text_SQL = f"DELETE FROM {table}_tags WHERE id IN ({tags})"
            self.cursor.execute(text_SQL)
            self.connect.commit()
            return True
        except Exception as error:
            return error

    def add_sources(self, table: str, sources: list):
        try:
            values = ''
            for source in sources:
                values += f"{source['link'], source['name'], source['type']},"
            values = values[:-1]
            # создаем запрос для ввода данных
            text_SQL = f"INSERT INTO {table}_sources (link, name, type) VALUES {values}"
            self.cursor.execute(text_SQL)
            self.connect.commit()
            return True
        except Exception as error:
            return error
        
    def upd_sources(self, table: str, sources: list):
        try:
            text_SQL = ''
            for source in sources:
                dataset = ', '.join(f"{key} = '{value}'" for key, value in source.items())
                text_SQL += f"UPDATE {table}_sources SET {dataset} WHERE id = {source['id']};"
            self.cursor.execute(text_SQL)
            self.connect.commit()
            return True
        except Exception as error:
            return error
        
    def get_sources(self, table: str):
        try:
            text_SQL = f"SELECT * FROM {table}_sources"
            self.cursor.execute(text_SQL)
            self.connect.commit()
            rows = self.cursor.fetchall()
            data = []
            for row in rows:
                data.append(dict(zip([desc[0] for desc in self.cursor.description], row)))
            return data
        except Exception as error:
            return error
    
    def del_sources(self, table: str, sources: list):
        try:
            sources = ", ".join(str(source) for source in sources)
            text_SQL = f"DELETE FROM {table}_sources WHERE id IN ({sources})"
            self.cursor.execute(text_SQL, sources)
            self.connect.commit()
            return True
        except Exception as error:
            return error
        
    def add_subscribers(self, table: str, subscribers: list):
        try:
            values = ''
            for subscriber in subscribers:
                values += f"{subscriber['id'], subscriber['name']},"
            values = values[:-1]
            # создаем запрос для ввода данных
            text_SQL = f"INSERT INTO {table}_subscribers (id, name) VALUES {values}"
            self.cursor.execute(text_SQL)
            self.connect.commit()
            return True
        except Exception as error:
            return error
            
    
    def upd_subscribers(self, table: str, subscribers: list):
        try:
            text_SQL = ''
            for subscriber in subscribers:
                dataset = ', '.join(f"{key} = '{value}'" for key, value in subscriber.items())
                text_SQL += f"UPDATE {table}_subscribers SET {dataset} WHERE id = {subscriber['id']};"
            self.cursor.execute(text_SQL)
            self.connect.commit()
            return True
        except Exception as error:
            return error
        

    def get_subscribers(self, table: str):
        try:
            text_SQL = f"SELECT * FROM {table}_subscribers"
            self.cursor.execute(text_SQL)
            self.connect.commit()
            rows = self.cursor.fetchall()
            data = []
            for row in rows:
                data.append(dict(zip([desc[0] for desc in self.cursor.description], row)))
            return data
        except Exception as error:
            return error
    
    def del_subscribers(self, table: str, subscribers: list):
        try:
            subscribers = ", ".join(str(subscriber) for subscriber in subscribers)
            text_SQL = f"DELETE FROM {table}_subscribers WHERE id IN ({subscribers})"
            self.cursor.execute(text_SQL, subscribers)
            self.connect.commit()
            return True
        except Exception as error:
            return error