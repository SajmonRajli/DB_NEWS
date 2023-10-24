# -*- coding: utf-8 -*-
import os
from dotenv import load_dotenv
from fastapi import FastAPI
from lib.PostgreSQL import Database

load_dotenv()
API_KEY = os.getenv("API_KEY")

DB = Database('news')

app = FastAPI()



@app.get("/test")
def test():
    return {"message": "Hello DataBase"}



