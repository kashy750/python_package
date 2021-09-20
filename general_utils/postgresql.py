import psycopg2


def get_connection(db_url):
    connection = psycopg2.connect(db_url)
    return connection


def execute_query(db_connection, query=""):
    cursor = db_connection.cursor()
    cursor.execute(query)
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

    first_row = rows
    column_names = first_row.keys()
    column_names_sub_query = ', '.join(column_names)

    for row in rows:
        try:
            values = row.values()
            row_values_sub_query = ', '.join(values)
            insert_statement = f'INSERT INTO {table_name} ({column_names_sub_query}) VALUES ({row_values_sub_query});'
            cursor.execute(insert_statement)

        except (Exception, psycopg2.Error) as error:
            print(error)

    db_connection.commit()


# FIXME: Finish implementation
def fetch_many(db_connection, table_name):

    query = f'SELECT * FROM {table_name}'
    cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(query)
    result = cursor.fetchmany()
    cursor.close()

    return result