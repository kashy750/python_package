import psycopg2
from pgcopy import CopyManager


def get_connection(db_url):
    connection = psycopg2.connect(db_url)
    return connection


def execute_query(db_connection, query, data):
    cursor = db_connection.cursor()
    cursor.execute(query, data)
    db_connection.commit()
    cursor.close()


def insert_rows(db_connection, table_name, rows):
    """
    Insert multiple rows of data.
    Args:
        db_connection: postgresql connection object
        table_name (str): Name of the table
        data (list(dict))
    """
    cursor = db_connection.cursor()

    first_row = rows[0]
    column_names = list(first_row)

    values = list(map(lambda row: tuple(row.values()), rows))

    try:
        copyManager = CopyManager(db_connection, table_name, column_names)
        copyManager.copy(values)
        db_connection.commit()
        cursor.close()

    except (Exception, psycopg2.Error) as error:
        cursor.close()
        raise error


def fetch_many(db_connection, table_name, limit=None):

    query = f'SELECT * FROM {table_name} LIMIT {limit}'
    cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(query)
    result = cursor.fetchmany(limit)
    cursor.close()

    return result


def is_connection_open(db_connection):
    if db_connection.closed == 0:
        return True

    return False

def close_connection(db_connection):
    db_connection.close();
