import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    host = os.getenv("BDREAL_HOST")
    port = int(os.getenv("BDREAL_PORT", 3306))
    database = os.getenv("BDREAL_DATABASE")
    user = os.getenv("BDREAL_USER")
    password = os.getenv("BDREAL_PASSWORD")

    if not host:
        raise ValueError("BDREAL_HOST não foi carregado do arquivo .env")
    if not database:
        raise ValueError("BDREAL_DATABASE não foi carregado do arquivo .env")
    if not user:
        raise ValueError("BDREAL_USER não foi carregado do arquivo .env")
    if password is None:
        raise ValueError("BDREAL_PASSWORD não foi carregado do arquivo .env")

    return mysql.connector.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )