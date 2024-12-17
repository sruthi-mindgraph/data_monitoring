from app.config import get_mysql_config
import pymysql

def execute_query(query: str):
    """
    Executes a given SQL query using MySQL connection.
    """
    mysql_config = get_mysql_config()
    connection = pymysql.connect(
        host=mysql_config["host"],
        port=mysql_config["port"],
        user=mysql_config["user"],
        password=mysql_config["password"]
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()
    finally:
        connection.close()

