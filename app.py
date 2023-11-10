# -*- coding: utf-8 -*-
import os
from datetime import datetime
from dotenv import load_dotenv
from fastapi import FastAPI
from lib.PostgreSQL import Database



load_dotenv()
API_KEY = os.getenv("API_KEY")

DB = Database('news')

app = FastAPI()



@app.get("/test")
async def test():
    return {"message": "Hello DataBase"}


@app.post("/user")
async def add_user(request: dict):
    """
    request 
    json_data = {
        "user": 'username',
    }
    Связка пользователя - ключ добавляется в таблицу users
    """
    user = request['user']
    key = request['key']
    data = DB.add_user(user, key)
    return {"response": str(data)}

# # Эту функцию не стоит подключать, только для проверки!!!
# @app.get("/user")
# async def check_user(request: dict):
#     # request 
#     # json_data = {
#     #     "user": 'username',
#     # }
#     # Возвращается ключ пользователя
#     user = request['user']
#     data = DB.check_user(user)
#     if data != None:
#         return {"response": data}
#     else:
#         return {"response": "error"}
    
@app.delete("/user")
async def delete_user(request: dict):
    pass


# создание таблицы
@app.post("/table")
async def create_table(request: dict):
    """
    request 
    json_data = {
        "user": 'username',
        "table": 'tablename'
        "key": 'user_token',
        }
    Если ключ совпал, то таблица создается "user_table"
    так же создается таблица с тегами  (ключ и данные в виде массива)
    так же создается таблица с источниками (название ссылка категория(тип/группа))
    так же создается таблица с пользователями в телеграме (юзер, айдишник)
    """
    user = request['user']
    table = request['table']
    key = request['key']
    
    key_baza = DB.check_user(user)
    if key == key_baza:
        data = DB.create_table(f"{user}_{table}")
        return {"response": str(data)}
    else:
        return {"response": "Ошибка авторизации"}
        


@app.delete("/table")
async def delete_table(request: dict):
    """
    request
    json_data = {
        "user": 'username',
        "table": 'tablename'
        "key": 'user_token',
        }
    Если ключ совпал, то таблицы к этой базе удаляются 
    """
    user = request['user']
    table = request['table']
    key = request['key']
        
    key_baza = DB.check_user(user)
    if key == key_baza:
        data = DB.delete_table(f"{user}_{table}")
        return {"response": str(data)}
    else:
        return {"response": "Ошибка авторизации"}


@app.post("/data")
async def add_data(request: dict):
    """
    request
    json_data = {
        "user": 'username',
        "table": 'tablename'
        "key": 'user_token',
        "data": [{},{}]
        }
    Список полей "data"       
        обязательные
            source TEXT NOT NULL,
            channel_name VARCHAR(255) NOT NULL,
            channel_link VARCHAR(255) NOT NULL, 
            public_date TIMESTAMP NOT NULL,    
        дополнительные 
            resembling INTEGER[],
            channel_id VARCHAR(255),   
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
    Если ключ совпал, то массив данных добавляется в базу
    """
    user = request['user']
    table = request['table']
    key = request['key']
    data = request['data']
    key_baza = DB.check_user(user)
    if key == key_baza:
        data = DB.add_data(f"{user}_{table}", data)
        return {"response": str(data)}
    else:
        return {"response": "Ошибка авторизации"}


@app.get('/data')
async def get_data(request: dict):
    """
    request
    json_data = {
        "user": 'username',
        "table": 'tablename'
        "key": 'user_token',
        "where": {"key": 'value', "key2": 'value2'} - опционально и не обязательно
        }
    Если ключ совпал, то возвращается список всех данных для пользователя и его таблицы
    """
    user = request['user']
    table = request['table']
    key = request['key']
    key_baza = DB.check_user(user)
    if key == key_baza:
        if 'where' in request:
            data = DB.get_data(f"{user}_{table}", request['where'])
        else:
            data = DB.get_data(f"{user}_{table}")
        return {"response": data}
    else:
        return {"response": "Ошибка авторизации"}
    
@app.patch("/data")
async def update_data(request: dict):
    """
    request
    json_data = {
        "user": 'username',
        "table": 'tablename'
        "key": 'user_token',
        "data": [{"id", ...},{"id", ...}]
        }
          
    """
    user = request['user']
    table = request['table']
    key = request['key']
    data = request['data']
    key_baza = DB.check_user(user)
    if key == key_baza:
        data = DB.upd_data(f"{user}_{table}", data)
        return {"response": str(data)}
    else:
        return {"response": "Ошибка авторизации"}
    
@app.delete("/data")
async def delete_data(request: dict):
    """
    request
    json_data = {
        "user": 'username',
        "table": 'tablename'
        "key": 'user_token',
        "data": [id1, id2, id3]
        }
    Если ключ совпал, то данные удаляются
    """
    user = request['user']
    table = request['table']
    key = request['key']
    data = request['data']
    key_baza = DB.check_user(user)
    if key == key_baza:
        data = DB.del_data(f"{user}_{table}", data)
        return {"response": str(data)}
    else:
        return {"response": "Ошибка авторизации"}
    
@app.get("/data_by_date")
async def get_data_by_date(request: dict):
    """
    request
    json_data = {
        "user": 'username',
        "table": 'tablename'
        "key": 'user_token',
        "date_start": '2023-01-01 12:53:00' - опционально и не обязательно
        "date_end": '2023-01-01 12:53:00' - опционально и не обязательно
        }
    Если ключ совпал, то возвращается список всех данных для пользователя и его таблицы в интервале даты
    """
    user = request['user']
    table = request['table']
    key = request['key']
    key_baza = DB.check_user(user)
    if key == key_baza:

        if 'date_start' in request:
            date_start = request['date_start']
        else: date_start = None
        if 'date_end' in request:
            date_end = request['date_end']
        else: date_end = None
    
        data = DB.get_data_by_date(f"{user}_{table}", date_start, date_end)
        return {"response": data}


@app.get("/tag")
async def get_tag(request: dict):
    """
    request
    json_data = {
        "user": 'username',
        "table": 'tablename'
        "key": 'user_token',
        }
    Если ключ совпал, то возвращается список всех тегов для пользователя и его таблицы
    """
    user = request['user']
    table = request['table']
    key = request['key']
    key_baza = DB.check_user(user)
    if key == key_baza:
        data = DB.get_tags(f"{user}_{table}")
        return {"response": data}
    else:
        return {"response": "Ошибка авторизации"}


@app.post("/tag")
async def create_tag(request: dict):
    """
    request
    json_data = {
        "user": 'username',
        "table": 'tablename',
        "key": 'user_token',
        "tags": [
            {'id': 'id',  'id_parent': 'id_parent', 'parent': 'parent', 'name': 'name', 'searchObject': 'searchObject', 'value': 'value', 'type': 'type'},
        ]
    }
    Если ключ совпал, то массив тегов добавляется в базу
    """
    
    user = request['user']
    table = request['table']
    key = request['key']
    tags = request['tags']
    key_baza = DB.check_user(user)
    if key == key_baza:
        data = DB.add_tags(f"{user}_{table}", tags)
        return {"response": str(data)}
    else:
        return {"response": "Ошибка авторизации"}

@app.patch("/tag")
async def update_tag(request: dict):
    """
    request
    json_data = {
        "user": 'username',
        "table": 'tablename',
        "key": 'user_token',
        "tags": [
            {'id': id,  'id_parent': 'id_parentnew', 'parent': 'parentnew', 'name': 'namenew', 'searchObject': 'searchObject', 'value': 'value', 'type': 'type'},
        ]
    }
    Если ключ совпал, то тег по указанному id изменяется
    """
    user = request['user']
    table = request['table']
    key = request['key']
    tags = request['tags']
    key_baza = DB.check_user(user)
    if key == key_baza:
        data = DB.upd_tags(f"{user}_{table}", tags)
        return {"response": str(data)}
    else:
        return {"response": "Ошибка авторизации"}

@app.delete("/tag")
async def delete_tag(request: dict):
    """
    request
    json_data = {
        "user": 'username',
        "table": 'tablename',
        "key": 'user_token',
        "tags": [id1, id2, id3]
    }
    Если ключ совпал, то тег удаляется
    """
    user = request['user']
    table = request['table']
    key = request['key']
    tags = request['tags']
    key_baza = DB.check_user(user)
    if key == key_baza:
        data = DB.del_tags(f"{user}_{table}", tags)
        return {"response": str(data)}
    else:
        return {"response": "Ошибка авторизации"}


@app.get("/source")
async def get_source(request: dict):
    """
    request
    json_data = {
        "user": 'username',
        "table": 'tablename',
        "key": 'user_token',
        }
    Если ключ совпал, то возвращается список всех источников для пользователя и его таблицы
    """
    user = request['user']
    table = request['table']
    key = request['key']
    key_baza = DB.check_user(user)
    if key == key_baza:
        data = DB.get_sources(f"{user}_{table}")
        return {"response": data}
    else:
        return {"response": "Ошибка авторизации"}


@app.post("/source")
async def create_source(request: dict):
    """
    request
    json_data = {
        "user": 'username',
        "table": 'tablename',
        "key": 'user_token',
        "sources": [
            {'link': 'link',  'name': 'name', 'type': 'type'},
        ]
    }
    Если ключ совпал, то массив источников добавляется в базу
    """
    user = request['user']
    table = request['table']
    key = request['key']
    sources = request['sources']
    key_baza = DB.check_user(user)
    if key == key_baza:
        data = DB.add_sources(f"{user}_{table}", sources)
        return {"response": str(data)}
    else:
        return {"response": "Ошибка авторизации"}

@app.patch("/source")
async def update_source(request: dict):
    """
    request
    json_data = {
        "user": 'username',
        "table": 'tablename',
        "key": 'user_token',
        "sources": [
            {'id': id, 'link': 'linknew',  'name': 'namenew', 'type': 'typenew'},
        ]
    }
    Если ключ совпал, то источник по указанному id изменяется
    """
    user = request['user']
    table = request['table']
    key = request['key']
    sources = request['sources']
    key_baza = DB.check_user(user)
    if key == key_baza:
        data = DB.upd_sources(f"{user}_{table}", sources)
        return {"response": str(data)}
    else:
        return {"response": "Ошибка авторизации"}


@app.delete("/source")
async def delete_source(request: dict):
    """
    request
    json_data = {
        "user": 'username',
        "table": 'tablename',
        "key": 'user_token',
        "sources": [id1, id2, id3]
    }
    Если ключ совпал, то источник с рассылки удаляется
    """
    user = request['user']
    table = request['table']
    key = request['key']
    sources = request['sources']
    key_baza = DB.check_user(user)
    if key == key_baza:
        data = DB.del_sources(f"{user}_{table}", sources)
        return {"response": str(data)}
    else:
        return {"response": "Ошибка авторизации"}


@app.get("/subscriber")
async def get_subscriber(request: dict):
    """
    request
    json_data = {
        "user": 'username',
        "table": 'tablename',
        "key": 'user_token',
        }
    Если ключ совпал, то возвращается список всех подписчиков для пользователя и его таблицы
    """
    user = request['user']
    table = request['table']
    key = request['key']
    key_baza = DB.check_user(user)
    if key == key_baza:
        data = DB.get_subscribers(f"{user}_{table}")
        return {"response": data}
    else:
        return {"response": "Ошибка авторизации"}

@app.post("/subscriber")
async def create_subscriber(request: dict):
    """
    request
    json_data = {
        "user": 'username',
        "table": 'tablename',
        "key": 'user_token',
        "subscribers": [
            {'id': id,  'name': 'name'},
        ]
    }
    Если ключ совпал, то массив подписчиков на рассылку добавляется в базу
    """
    user = request['user']
    table = request['table']
    key = request['key']
    subscribers = request['subscribers']
    key_baza = DB.check_user(user)
    if key == key_baza:
        data = DB.add_subscribers(f"{user}_{table}", subscribers)
        return {"response": str(data)}
    else:
        return {"response": "Ошибка авторизации"}

@app.patch("/subscriber")
async def update_subscriber(request: dict):
    """
    request
    json_data = {
        "user": 'username',
        "table": 'tablename',
        "key": 'user_token',
        "subscribers": [
            {'id': id,  'name': 'namenew'},
        ]
    }
    Если ключ совпал, то подписчик по указанному id изменяется
    """
    user = request['user']
    table = request['table']
    key = request['key']
    subscribers = request['subscribers']
    key_baza = DB.check_user(user)
    if key == key_baza:
        data = DB.upd_subscribers(f"{user}_{table}", subscribers)
        return {"response": str(data)}
    else:
        return {"response": "Ошибка авторизации"}

    

@app.delete("/subscriber")
async def delete_subscriber(request: dict):
    """
    request
    json_data = {
        "user": 'username',
        "table": 'tablename',
        "key": 'user_token',
        "subscribers": [id1, id2, id3]
    }
    Если ключ совпал, то подписчик с рассылки удаляется
    """
    user = request['user']
    table = request['table']
    key = request['key']
    subscribers = request['subscribers']
    key_baza = DB.check_user(user)
    if key == key_baza:
        data = DB.del_subscribers(f"{user}_{table}", subscribers)
        return {"response": str(data)}
    else:
        return {"response": "Ошибка авторизации"}
    






