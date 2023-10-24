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
        self.connection = psycopg2.connect(user=user,
                                    password=password,
                                    host=host,
                                    port=port,
									database=database)
        # Курсор для выполнения операций с базой данных
        self.cursor = self.connection.cursor()
        # запрос на создание базы данных если её нет
        sql_create_database = f'CREATE DATABASE IF NOT EXISTS {database}'
        self.cursor.execute(sql_create_database)

    def request_db(self, text_SQL: str):
        # выполняем запрос к бд и сохраняем изменения
        try:
            self.cursor.execute(text_SQL)
            self.conn.commit()
            return {"response": self.cursor.fetchall()}
        except Exception as e:
            return {"exception": e}

    def create(self, table: str, values: dict):
        # создаем таблицу
        text_SQL = f'''CREATE TABLE IF NOT EXISTS {table} (
            {', '.join(f'{key} {values}' for key, values in values.items()),}
            )'''
        return self.request_db(text_SQL)
    
    def set(self, table: str, values: dict):        
        # создаем запрос для ввода данных
        text_SQL = "INSERT OR IGNORE INTO {} ({}) VALUES ({})".format(
            table,
            ', '.join(values.keys()), 
            ', '.join(str(value) for value in values.values())
        )
        return self.request_db(text_SQL)

    def get(self, table: str, where: str = None):
        # создаем запрос для получения данных
        text_SQL = f"SELECT * FROM {table}"
        if where:
            text_SQL += f" WHERE {where}"
        return self.request_db(text_SQL)
    
    def update(self, table: str, values: dict, where: str = None):
        # создаем запрос для обновления данных
        text_SQL = f"""
        UPDATE {table} 
        SET {', '.join(f'{key} = {value}' for key, value in values.items())}
        """
        if where:
            text_SQL += f" WHERE {where}"
        return self.request_db(text_SQL)

    def delete(self, table: str, where: str):
        # создаем запрос для удаления данных
        text_SQL = f"DELETE FROM {table} WHERE {where}"
        
        return self.request_db(text_SQL)