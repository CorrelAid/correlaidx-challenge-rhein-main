from neo4j import GraphDatabase
from neo4jconfig import neo4j_config as cfg

URI = cfg["uri"]
USER = cfg["user"]
PASSWORD = cfg["password"]
DB = cfg["db"]


class Driver:
    def __init__(self):
        self.__uri = URI
        self.__user = USER
        self.__password = PASSWORD
        self.__db = DB

        self.driver = GraphDatabase.driver(
            self.__uri,
            auth=(self.__user, self.__password)
        )

    def _do_cypher_tx(self, tx, cypher, **cypher_kwargs):
        result = tx.run(cypher, **cypher_kwargs)
        values = [record.values() for record in result]
        return values

    def do_cypher_tx(self, cypher, **cypher_kwargs):
        with self.driver.session() as session:
            values = session.read_transaction(
                self._do_cypher_tx,
                cypher,
                **cypher_kwargs
            )
        return values

    def close(self):
        if self.driver:
            self.driver.close()


driver = Driver()
