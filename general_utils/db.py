import postgresql

class DB():

    connectors = {
        "postgresql": postgresql.get_connection
    }

    def __init__(self, url="", db="postgresql"):
        self.connection = self.connect()
        self.db = db
        self.url = url
    
    def connect(self):
        connector = self.connectors[self.db]
        return connector(self.url)

    # TODO: Add a method to insert many rows of data

    # TODO: Add a method to fetch many rows of data
    