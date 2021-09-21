import logging
import time

import postgresql


class DB():

    connectors = {"postgresql": postgresql.get_connection}
    inserters = {"postgresql": postgresql.insert_rows}
    fetchers = {"postgresql": postgresql.fetch_many}
    executers = {"postgresql": postgresql.execute_query}
    connection_status_providers = {"postgresql": postgresql.is_connection_open}

    def __init__(self, url, db="postgresql"):
        self.db = db
        self.url = url
        self.connection = self.connect()

    def connect(self):
        connector = self.connectors[self.db]
        return connector(self.url)

    def insert_many(self, table_name, data):
        try:
            inserter = self.inserters[self.db]
            inserter(self.connection, table_name, data)
        except Exception as e:
            logging.error(
                f'[G-utils]--- DB - {self.db} - Insert many failed with error:{e}'
            )

    def fetch_many(self, table_name):
        try:
            fetcher = self.fetchers[self.db]
            fetcher(self.connection, table_name)
        except Exception as e:
            logging.error(
                f'[G-utils]--- DB - {self.db} - Fetch many failed with error:{e}'
            )

    def execute_query(self, query, data=None):
        try:
            executer = self.executers[self.db]
            executer(self.connection, query, data)
        except Exception as e:
            logging.error(
                f'[G-utils]--- DB - {self.db} - Execute query failed with error:{e}'
            )

    def is_connection_open(self):
        connection_status_provider = self.connection_status_providers[self.db]
        return connection_status_provider(self.connection)

    def keep_connection_open(self):

        while True:
            # Wait 5 mins
            time.sleep(60 * 5)

            if not self.is_connection_open():
                try:
                    self.retry_to_connect()
                except:
                    return

    def retry_to_connect(self):
        retry_limit = 10
        for retry_count in range(retry_limit):
            try:
                logging.info(
                    f'[G-utils]--- DB - {self.db} - Retrying to connect ({retry_count}/{retry_limit})'
                )
                self.connection = self.connect()
                if self.is_connection_open():
                    return

                # Wait 5 mins
                time.sleep(60 * 5)

            except Exception as e:
                logging.error(
                    f'[G-utils]--- DB - {self.db} - Failed while retrying to connect, with error:{e}'
                )
                raise Exception('Retry Failed!')

        logging.error(f'[G-utils]--- DB - {self.db} - Reached retry limit')
        raise Exception('Reached retry limit')
