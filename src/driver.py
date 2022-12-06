from neo4j import GraphDatabase, basic_auth
from neo4jconfig import neo4j_config as cfg

uri = cfg["uri"]
user = cfg["user"]
password = cfg["password"]
db = cfg["db"]


class Driver:
    def __init__(self):
        self.__uri = uri
        self.__user = user
        self.__password = password
        self.__db = db

        self.driver = GraphDatabase.driver(
            self.__uri,
            auth=(self.__user, self.__password)
        )

    def close(self):
        if self.driver:
            self.driver.close()

