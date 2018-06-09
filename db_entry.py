import psycopg2 as psql
import json
import argparse
import traceback
from Crypto.Cipher import AES
import base64

SECRET = "1234567890ABCDEF"
cipher = AES.new(SECRET, AES.MODE_ECB)

def encrypt(s):
	encrypted = base64.b64encode(cipher.encrypt(s + (16 -(len(s) % 16))*'_'))
	return encrypted.decode("utf-8")


# database abstraction structures

normal_cmds = set(["new", "remove", "child", "parent", "ancestors", 
				   "descendants", "ancestor", "read", "update"])

def new_user(args):
	return (args['emp'], args['data'], encrypt(args['newpassword']))

insert_user = """INSERT INTO users VALUES (%s, %s, %s);"""
insert_relation = """INSERT INTO ancestry VALUES (%s, %s);"""

remove_user_q = """DELETE FROM users WHERE userId = %s;"""
update_data = """UPDATE users SET data = %s WHERE userID = %s;"""

get_passwd = """SELECT password FROM users WHERE userId = %s;"""
get_data = """SELECT data FROM users WHERE userId = %s;"""

# utility database

def check_passwd(conn, emp, passwd):
	with conn.cursor() as cur:
		cur.execute(get_passwd, (emp,))
		c_passwd = cur.fetchone()[0]
		if c_passwd != encrypt(passwd):
			raise Exception("Invalid password")

def remove_user(conn, emp):
	with conn.cursor() as cur:
		cur.execute(remove_user_q, (emp,))

def check_admin(conn, tree, admin, passwd, emp):
	check_passwd(conn, admin, passwd)
	if not is_ancestor(tree, admin, emp):
		raise Exception("Admin is not ancestor of target")

def print_ok(res=None):
	if res:
		print(json.dumps({"status" : "OK", "data" : res}))
	else:
		print(json.dumps({"status" : "OK"}))

def print_error(e):
	print(json.dumps({"status" : "ERROR", "debug" : str(e)}))

# utility tree

class Node:
	def __init__(self):
		self.desc = set()
		self.anc = None


def tree_add(tree, anc, desc):
	if anc not in tree:
		tree[anc] = Node()
	tree[anc].desc.add(desc)
	if desc not in tree:
		tree[desc] = Node()
	tree[desc].anc = anc

def gen_tree(conn):
	tree = {}
	with conn.cursor() as cur:
		cur.execute("SELECT userId FROM users")
		usersIds = cur.fetchall()
		for userId in usersIds:
			userId = int(userId[0])
			tree[userId] = Node()
	with conn.cursor() as cur:
		cur.execute("SELECT * from ancestry")
		rows = cur.fetchall()
		for (anc, desc) in rows:
			anc, desc = int(anc), int(desc)
			tree_add(tree, anc, desc)
	return tree


def is_ancestor(tree, emp_a, emp_d):
	while emp_d is not None:
		if emp_d == emp_a:
			return True
		emp_d = tree[emp_d].anc
	return False

def print_tree(tree):
	for k, n in tree.items():
		print(k, n.anc, n.desc)

# normal commands

def new(conn, tree, admin, emp1, passwd, data, newpasswd, emp):
	check_admin(conn, tree, admin, passwd, emp1)
	with conn.cursor() as cur:
		cur.execute(insert_user, (emp, data, encrypt(newpasswd)))
		cur.execute(insert_relation, (emp1, emp))
	tree_add(tree, emp1, emp)

def remove(conn, tree, admin, passwd, emp):
	check_admin(conn, tree, admin, passwd, emp)
	if admin == emp:
		raise Exception("Can't remove admin")
	
	tree[tree[emp].anc].desc.discard(emp)

	targets = set([emp])
	while not targets == set():
		target = targets.pop()
		targets |= tree[target].desc
		remove_user(conn, target)
		del tree[target]

def child(conn, tree, admin, passwd, emp):
	check_passwd(conn, admin, passwd)
	return list(tree[emp].desc)
		
def parent(conn, tree, admin, passwd, emp):
	check_passwd(conn, admin, passwd)
	return [tree[emp].anc]

def ancestors(conn, tree, admin, passwd, emp):
	check_passwd(conn, admin, passwd)

	res = []
	anc = tree[emp].anc
	while anc is not None:
		res.append(anc)
		emp = anc
		anc = tree[emp].anc
	return res

def descendants(conn, tree, admin, passwd, emp):
	check_passwd(conn, admin, passwd)

	res = []
	queue = set([emp])
	while not queue == set():
		e = queue.pop()
		queue |= tree[e].desc
		res.extend(list(tree[e].desc))
	return res

def ancestor(conn, tree, admin, passwd, emp1, emp2):
	check_passwd(conn, admin, passwd)

	return [is_ancestor(tree, emp2, emp1)]

def read(conn, tree, admin, passwd, emp):
	check_passwd(conn, admin, passwd)

	if not is_ancestor(tree, admin, emp) and \
		not admin in tree[emp].desc:
		raise Exception("Invalid admin to read")

	with conn.cursor() as cur:
		cur.execute(get_data, (emp,))
		res = cur.fetchone()[0]
		return [res]

def update(conn, tree, admin, passwd, emp, newdata):
	check_passwd(conn, admin, passwd)

	if not is_ancestor(tree, admin, emp) and \
		not admin in tree[emp].desc:
		raise Exception("Invalid admin to update")

	with conn.cursor() as cur:
		cur.execute(update_data, (newdata, emp))




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
				res = run_command(conn, tree, cmd, kwargs)
				print_ok(res)
			except (Exception, psql.DatabaseError) as e:
				# print(traceback.format_exc())
				print_error(e)

def run_command(conn, tree, cmd, kwargs):
	return globals()[cmd](conn, tree, **kwargs)


def run(commands, init):
	s = 1 + init
	assert len(commands) >= s
	conn = connect_bd(commands[0])
	try:
		if init:
			try:
				init_bd(conn)
				create_root(conn, commands[1])
			except (Exception, psql.DatabaseError) as e:
				print_error(e)
		tree = gen_tree(conn)
		run_commands(conn, tree, commands[s:])
		conn.commit()
	except (Exception, psql.DatabaseError) as e:
		print("init run error: ", e)
	finally:
		conn.close()


def main():
	args = parse_args()
	commands = parse_json(args.file_name)
	run(commands, args.init)


if __name__ == '__main__':
	main()
