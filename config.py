#!/usr/bin/env python3
"""
    Developed by @edyatl <edyatl@yandex.ru> January 2024
    https://github.com/edyatl

"""
import os
from os import environ as env
from dotenv import load_dotenv


project_dotenv = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(project_dotenv):
    load_dotenv(project_dotenv)

class Configuration(object):
    DEBUG = False
    RETRIES = 10 
    DELAY = 6

    # DWH Credentials
    CLICKHOUSE_HOST = env.get('ENV_CLICKHOUSE_HOST')
    CLICKHOUSE_USER = env.get('ENV_CLICKHOUSE_USER')
    CLICKHOUSE_PASS = env.get('ENV_CLICKHOUSE_PASS')
    CLICKHOUSE_PORT = env.get('ENV_CLICKHOUSE_PORT')

    # URLs
    BASE_URL = "https://maintracking.net/cikpl9k.php"

    LOG_FILE = os.path.join(os.path.dirname(__file__), "clickhouse_event_monitor.log")
    JSON_FILE = os.path.join(os.path.dirname(__file__), "var_storage.json")
    DB_FILE = os.path.join(os.path.dirname(__file__), "db/cache.db")
    SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "db/schema.sql")

