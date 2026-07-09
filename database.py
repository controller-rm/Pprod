import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()


# =========================================================
# ERP - SOFTDIB_REALFIX
# =========================================================
def get_connection_softdib():

    return mysql.connector.connect(
        host=os.getenv("BDREAL_HOST"),
        port=int(os.getenv("BDREAL_PORT", 3306)),
        database=os.getenv("BDREAL_DATABASE"),
        user=os.getenv("BDREAL_USER"),
        password=os.getenv("BDREAL_PASSWORD"),
    )


# =========================================================
# PAINEL - realfix
# =========================================================
def get_connection_realfix():

    return mysql.connector.connect(
        host=os.getenv("REAL_HOST"),
        port=int(os.getenv("REAL_PORT", 3306)),
        database=os.getenv("REAL_DATABASE"),
        user=os.getenv("REAL_USER"),
        password=os.getenv("REAL_PASSWORD"),
    )