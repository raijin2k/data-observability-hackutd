# src/core/config.py
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    MONGO_URI = "mongodb://localhost:27017/"
    ES_HOST = "http://localhost:9200"
    REDIS_HOST = "localhost"
    REDIS_PORT = 6379
    TIMESCALE_CONFIG = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "password",
        "host": "localhost",
        "port": "5432"
    }