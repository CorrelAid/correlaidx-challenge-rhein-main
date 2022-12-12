from driver import Driver

dbcon = Driver()

def match_entity_nodes(tx, entity):
    result = tx.run(
        query=f"MATCH (n:{entity}) RETURN n LIMIT 25;"
    )
    records = list(result)
    summary = result.consume()
    return records, summary


with dbcon.driver.session() as session:
    records, summary = session.execute_read(match_entity_nodes, entity="Address")
    for person in records:
        print(person.data())