"""Cypher queries here."""
import os
import csv
from itertools import product
from typing import Dict, List
from argparse import ArgumentParser

from driver import driver
from edits import edits1, edits2

STOP_TERMS = set(['gruppe', 'group', 'bank', 'gmbh', 'ag'])

def parse_query_result(query_res, search_term: str):
	parsed_query_res = {}

	if query_res:
		parsed_query_res['search_term'] = search_term
		parsed_query_res['id'] = str(query_res[0].element_id)
		parsed_query_res['labels'] = ', '.join(query_res[0].labels)
		parsed_query_res['properties'] = query_res[0]._properties

	return parsed_query_res

def out_format(values, search_term):
	parsed_query_res = []

	for query_res in values:
		parsed_res = parse_query_result(query_res, search_term)
		parsed_query_res.append(parsed_res)

	return parsed_query_res
	

def contains_lower_name(**params) -> List[Dict[str, str]]:
	""" String match in node name."""
	cypher = """
	MATCH (n)
	WHERE toLower(n.name) contains $name
	RETURN n
	"""
	name = params.get('name')
	if name:
		name = name.lower()
		if name in STOP_TERMS:
			return
		print(f"Looking for '{name}' on the database, wait a moment...")
		values = driver.do_cypher_tx(cypher, name=name)
		out_format_values = out_format(values, search_term=name) 
		return out_format_values
	raise ValueError("You shold pass the param 'name'")

def contains_lower_name_and_splits(**params) -> List[Dict[str, str]]:
	all_values = []

	name = params.get('name')
	name_parts = name.split()
	for part in set([name] + name_parts):
		if len(part) > 2:
			out_format_values = contains_lower_name(name=part)
			if out_format_values:
				all_values.extend(out_format_values)
	return all_values

def contains_lower_with_edits_name(**params) -> List[Dict[str, str]]:
	all_values = []

	name = params.get('name')
	outfile = params.get('outfile')
	name_parts = name.split()
	edits = [list(edits1(part)) for part in name_parts]
	for edit in product(*edits):
		name = " ".join(edit)
		out_format_values = contains_lower_name(name=name)
		if out_format_values:
			all_values.extend(out_format_values)
	return all_values

def contains_lower_address(**params) -> List[Dict[str, str]]:
	""" String match in address name"""
	cypher = """
	MATCH (addr:Address)
	WHERE toLower(addr.name) contains $address
	RETURN addr
	"""
	address = params.get('address')
	outfile = params.get('outfile')
	if address:
		address = address.lower()
		values = driver.do_cypher_tx(cypher, address=address)
		out_format_values = parse_query_result(values, search_term=address) 
		return out_format_values
	raise ValueError("You should pass the param 'address'")

def _queryobjtype2func_arg(query_object_type: str):
	dict_ = {
		'address': (contains_lower_address, 'address'),
		'name': (contains_lower_name, 'name'),
		'name_edits': (contains_lower_with_edits_name, 'name'),
		'name_splits': (contains_lower_name_and_splits, 'name')
	}
	func_arg = dict_.get(query_object_type)
	if not func_arg:
		raise ValueError("The query object type you passed is not supported.")

	func, arg = func_arg
	return func, arg

def contains_lower_from_txt(**params) -> List[Dict[str, str]]:
	all_values = []
	filepath = params.get('filepath')
	query_object_type = params.get('query_object_type')
	outfile = params.get('outfile')
	func, arg = _queryobjtype2func_arg(query_object_type)

	with open(outfile, 'w', newline='') as csvfile:
		fieldnames=['search_term', 'id', 'labels', 'properties']
		writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
		writer.writeheader()
		with open(filepath, 'r') as infile:
			for line in infile:
				kwargs = {arg: line.rstrip(), 'outfile': outfile}
				out_format_values = func(**kwargs)
				all_values.extend(out_format_values)
				for parsed_value in out_format_values:
					writer.writerow(parsed_value)

def _parse_args():
	parser = ArgumentParser()
	parser.add_argument("queryname", help="Which query to execute.")
	parser.add_argument("--name")
	parser.add_argument("--address")
	parser.add_argument("--query_object_type")
	parser.add_argument("--filepath")
	parser.add_argument("--outfile")
	args = parser.parse_args()
	return vars(args)

if __name__ == "__main__":
	queryname2func_args = {
		'contains_lower_name': {
			'func': contains_lower_name,
			'args': ['name', 'outfile']
		},
		'contains_lower_address': {
			'func': contains_lower_address,
			'args': ['address', 'outfile']
		},
		'contains_lower_with_edits_name': {
			'func': contains_lower_with_edits_name,
			'args': ['name', 'outfile']
		},
		'contains_lower_name_and_splits': {
			'func': contains_lower_name_and_splits,
			'args': ['name', 'outfile']
		},
		'contains_lower_from_txt': {
			'func': contains_lower_from_txt,
			'args': ['filepath', 'query_object_type', 'outfile']
		}
	}

	args = _parse_args()
	queryname = args["queryname"]
	func_queryargs = queryname2func_args.get(queryname)
	func = func_queryargs.get("func")
	queryargs = func_queryargs.get("args")

	if func and queryargs:
		arguments = [args.get(queryarg) for queryarg in queryargs]
		queryargs = dict(zip(queryargs, arguments))
		parsed_values = func(**queryargs)
