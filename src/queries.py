"""Cypher queries here."""
from itertools import product
from argparse import ArgumentParser

from driver import driver
from edits import edits1, edits2

def contains_lower_name(**params):
	""" String match in node name."""
	cypher = """
	MATCH (n)
	WHERE toLower(n.name) contains $name
	RETURN n
	"""
	name = params.get('name')
	if name:
		name = name.lower()
		values = driver.do_cypher_tx(cypher, name=name)
		return values
	raise ValueError("You shold pass the param 'name'")

def contains_lower_with_edits_name(**params):
	all_values = {}

	name = params.get('name')
	name_parts = name.split()
	edits = [list(edits1(part)) for part in name_parts]
	for edit in product(*edits):
		name = " ".join(edit)
		print(f"Looking for '{name}' on the database, wait a moment...")
		values = contains_lower_name(name=name)
		for value in values:
			element_id = value[0].element_id
			if not element_id in all_values:
				all_values[element_id] = value
	return all_values

def contains_lower_address(**params):
	""" String match in address name"""
	cypher = """
	MATCH (addr:Address)
	WHERE toLower(addr.name) contains $address
	RETURN addr
	"""
	address = params.get('address')
	if address:
		address = address.lower()
		values = driver.do_cypher_tx(cypher, address=address)
		return values
	raise ValueError("You should pass the param 'address'")


def _parse_args():
	parser = ArgumentParser()
	parser.add_argument("queryname", help="Which query to execute.")
	parser.add_argument("--name")
	parser.add_argument("--address")
	args = parser.parse_args()
	return vars(args)

if __name__ == "__main__":
	queryname2func_args = {
		'contains_lower_name': {
			'func': contains_lower_name,
			'args': ['name']
		},
		'contains_lower_address': {
			'func': contains_lower_address,
			'args': ['address']
		},
		'contains_lower_with_edits_name': {
			'func': contains_lower_with_edits_name,
			'args': ['name']
		}
	}

	args = _parse_args()
	queryname = args["queryname"]
	func_queryargs = queryname2func_args.get(queryname)
	func = func_queryargs.get("func")
	queryargs = func_queryargs.get("args")

	if func and queryargs:
		values = [args.get(queryarg) for queryarg in queryargs]
		queryargs = dict(zip(queryargs, values))
		values = func(**queryargs)
		print(values)
