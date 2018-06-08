import psycopg2 as psql
import json
import argparse

# database abstraction structures

normal_cmds = set(["new", "remove", "child", "parent", "ancestors", 
				   "descendants", "ancestor", "read", "update"])

def new_user(args):
	return (args['emp'], args['data'], args['newpassword'])

insert_user = """INSERT INTO users VALUES (%s, %s, %s);"""
insert_relation = """INSERT INTO ancestry VALUES (%s, %s);"""
get_passwd = """SELECT password FROM users WHERE userId = %s;"""

# utility database

def check_passwd(conn, emp, passwd):
	with conn.cursor() as cur:
		cur.execute(get_passwd, (emp,))
		c_passwd = cur.fetchone()[0]
		return c_passwd == passwd

def print_ok():
	print(json.dumps({"status" : "OK"}))

def print_error(e):
	print(json.dumps({"status" : "ERROR", "debug" : str(e)}))
# utility tree

class Node:
	def __init__(self):
		self.desc = []
		self.anc = None


def tree_add(tree, anc, desc):
	if anc not in tree:
		tree[anc] = Node()
	tree[anc].desc.append(desc)
	if desc not in tree:
		tree[desc] = Node()
	tree[desc].anc = anc

def gen_tree(conn):
	tree = {}
	with conn.cursor() as cur:
		cur.execute("SELECT userId FROM users")
		usersIds = cur.fetchall()
		for userId in usersIds:
			tree[userId] = Node()
	with conn.cursor() as cur:
		cur.execute("SELECT * from ancestry")
		rows = cur.fetchall()
		for (anc, desc) in rows:
			tree_add(tree, anc, desc)
	return tree


def is_ancestor(tree, emp_a, emp_d):
	while emp_d is not None:
		if emp_d == emp_a:
			return True
		emp_d = tree[emp_d].anc
	return False


# normal commands

def new(conn, tree, admin, emp1, passwd, data, newpasswd, emp):
	if not check_passwd(conn, admin, passwd):
		raise Exception("Invalid admin password")
	if not is_ancestor(tree, admin, emp1):
		raise Exception("Admin is not ancestor of parent")
	with conn.cursor() as cur:
		cur.execute(insert_user, (emp, data, newpasswd))
		cur.execute(insert_relation, (emp1, emp))
	tree_add(tree, emp1, emp)

# application logic

def parse_args():
	parse = argparse.ArgumentParser(description="Database connection script.")
	parse.add_argument("file_name", type=str, help="Commands file.")
	parse.add_argument("--init", action="store_true", help="For first run.")
	return parse.parse_args()

def parse_json(file_name):
	commands = []
	with open(file_name, "r") as fh:
		for line in fh:
			commands.append(json.loads(line))
	return commands

def connect_bd(open_cmd):
	assert "open" in open_cmd
	args = open_cmd["open"]
	conn = psql.connect(dbname=args['database'], 
						user=args['login'], 
						password=args['password'],
						host='localhost')
	print_ok()
	return conn

def init_bd(conn):
	with open("db.sql") as fh:
		with conn.cursor() as cur:
			cur.execute(fh.read())

def create_root(conn, root_cmd):
	assert "root" in root_cmd
	args = root_cmd["root"]
	assert args['secret'] == "qwerty"
	with conn.cursor() as cur:
		cur.execute(insert_user, new_user(args))
	print_ok()

def run_commands(conn, tree, commands):
	for command in commands:
		for cmd, kwargs in command.items():
			try:
				if cmd not in normal_cmds:
					raise Exception("Invalid command: " + cmd)
				run_command(conn, tree, cmd, kwargs)
				conn.commit()
				print_ok()
			except (Exception, psql.DatabaseError) as e:
				# print(traceback.format_exc())
				print_error(e)

def run_command(conn, tree, cmd, kwargs):
	globals()[cmd](conn, tree, **kwargs)


def init_run(commands):
	assert len(commands) >= 2
	conn = connect_bd(commands[0])
	try:
		init_bd(conn)
		create_root(conn, commands[1])
		tree = gen_tree(conn)
		run_commands(conn, tree, commands[2:])
	except (Exception, psql.DatabaseError) as e:
		print("init run error: ", e)
	finally:
		conn.close()




def main():
	args = parse_args()
	commands = parse_json(args.file_name)

	if args.init:
		init_run(commands)
	else:
		normal_run(commands)


if __name__ == '__main__':
	main()
