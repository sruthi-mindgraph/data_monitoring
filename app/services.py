from app.config import get_dynamic_table

def get_table_info(api_key: str):
    """
    Fetch database and table names dynamically based on the API key.
    """
    try:
        return get_dynamic_table(api_key)
    except ValueError as e:
        raise ValueError(str(e))
