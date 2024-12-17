from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

def get_mysql_config():
    return {
        "host": os.getenv("MYSQL_HOST"),
        "port": int(os.getenv("MYSQL_PORT", 3306)),  # Default to 3306 if not set
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
    }

def get_dynamic_table(api_key: str):
    """
    Fetches database and table configuration dynamically based on the API key.
    """
    try:
        return {
            "db1_db2": {
                "database_1": os.getenv("DIFFENJOBMETRICS_DATABASE"),
                "table_1": os.getenv("DIFFENJOBMETRICS_TABLE"),
                "database_2": os.getenv("EXTRACTION_INFO_DATABASE"),
                "table_2": os.getenv("EXTRACTION_INFO_TABLE")
            },
            "db1": {
                "database": os.getenv("DIFFENJOBMETRICS_DATABASE"),
                "table": os.getenv("DIFFENJOBMETRICS_TABLE")
            },
            "db2": {
                "database": os.getenv("EXTRACTION_INFO_DATABASE"),
                "table": os.getenv("EXTRACTION_INFO_TABLE")
            },
        }[api_key]
    except KeyError:
        raise ValueError(f"Invalid API key '{api_key}' in .env.")

