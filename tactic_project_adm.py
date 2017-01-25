#! /usr/bin/python3
# -*-coding: utf-8 -*-
# vim: set fileencoding=utf-8 :

"""
 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU Library Public License as published by
 the Free Software Foundation; either version 2, or (at your option)
 any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Library Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

__author__ = "Ralf Zerres"
__copyright__ = "Copyright 2017, Ralf Zerres"
__license__ = "GPL"
__version__ = "0.1"
__docformat__= 'reStructuredText'

__email__ = "ralf.zerres@networkx.de"
__status__ = "Production"

import argparse
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys

###
# Declarations
###
sthpw_tables = [ 'file', 'snapshot', 'task', 'note', 'wdg_settings', 'pref_setting', 'project' ]
tactic_projects = 'p50_sfr'
#tactic_projects = 'dw_00_test,dw_00_test2,dw_25_jojo,dw_40_as_moderation,dw_dev,p36_as_rolli_stella,p40_as_moderation,p42_mullewapp2,44_sorgenfresser,p47_molly,p50_sfr,p45_s71_rtl_fussballer,p46_as-fahrschule2'

###
# Function Definitions
###
def parse_args():
		"""Argument parser function using argparse

		We could define argument parsing within the main structure of our code, but
		prefere to make it a function.

		:returns: args

 		:Example:

		>>> import argparse
		>>> parser = argparse.ArgumentParser(description='Process arguments.')
		>>> parser.add_argument('--dryrun', dest='dryrun', action='store_true')
		_StoreTrueAction(option_strings=['--dryrun'], dest='dryrun', nargs=0, const=True, default=False, type=None, choices=None, help=None, metavar=None)

		.. note::

		use arguments in your calling function line this
		args = parse_args()
		print("[parse_args] args: %s" % vars(args))
		print("argument dryrun: %s" % args.dryrun)

		"""

		global args
		global projects

		parser = argparse.ArgumentParser(description='Process arguments.')
		parser.add_argument('--mode', dest='mode', choices=['delete', 'list', 'info'],
							default=list, required=True,
							help='running given mode')
		parser.add_argument('--host',
							nargs='?', const='host', default='localhost',
							help="hostname running the database [default: localhost]")
		parser.add_argument('--port',
							nargs='?', const='port', default='5432',
							help='portname the database is listening to [default: 5432]')
		parser.add_argument('--user',
							nargs='?', const='user', default='postgres',
							help='database-user [default: postgres]')
		parser.add_argument('--database',
							nargs='?', dest='dbname', const='dbname', default='sthpw',
							help='the tactic database-name')
		parser.add_argument('--projects', dest='projects', nargs='?',
							default=tactic_projects,
							help='list of project-names')
		parser.add_argument('-v', '--verbose', action='count', default=0,
							help='verbosity level')
		parser.add_argument('--version', action='version', version=('%(prog)s ' + __version__))
		parser.add_argument('--dryrun', dest='dryrun', action='store_true',
							help='dryrun, no real action')

		args = parser.parse_args()

		# convert input string projects to a list of tuples
		if len(args.projects) != 0:
				projects=args.projects.split(',')
		if (args.verbose) >= 3:
				print("[parse_args] args: %s" % vars(args))
				print("[parse_args] projects: %s" % projects)

		if (args.verbose) >= 2:
				print("Runtime mode: %s" % args.mode)
				print("Verbosity level: %s" % args.verbose)
		return args

def drop_database(db):
		"""Drop database for given project"""

		print("drop_database: processing database '%s'" % db)
		db_project = DB(args.host, args.port, args.user, 'template1')
		#ret = db_project.drop_with(db)
		ret = db_project.drop(db)
		if ret is not None:
				print(" -> database '%s': not deleted, error %s" % ret)
		else:
				print(" -> database '%s': deleted" % db)

def projects_delete(projects):
		"""Delete rows in database for given tactic project-list"""

		print("projects_delete: processing in database '%s'" % args.dbname)
		for project in projects:
				print(" processing project '%s'" % project)
				for table in sthpw_tables:
						if table == 'project':
								clause = 'code'
						else:
								clause = 'project_code'
								#rows = db.rows_truncate(project, table, clause)
						rows = db.rows_delete(project, table, clause)
						print(" -> table '%s': %d rows deleted" % (table,rows))

				drop_database(project)

def projects_info(projects):
		"""Process basic infos for given tacitc projects database"""

		for project in projects:
				print("Project: <", project, ">", sep="")
				db_project = DB(args.host, args.port, args.user, 'template1')
				ret = db_project.exist(project)
				if (args.verbose) >= 2:
						print("[projects_info] db_project.exists(%s): %s" % (project, ret))
				if (ret) is not True:
						print(" -> project database '%s': does not exist" % project)
				else:
						ret = db_project.info(project)
						print(" -> project database '%s': %s" % (project,ret[0]))

def projects_rows_list(projects):
		"""List rows in database for given tactic project-list"""

		for project in projects:
				print("rows_list: processing project '%s'" % project)
				for table in sthpw_tables:
						if table == 'project':
								clause = 'code'
						else:
								clause = 'project_code'
								db.connect()
								rows = db.rows_list(project, table, clause)
						print(" -> table '%s': %d rows referenced" % (table,rows))


###
# Class Definitions
###
class DB:
		"""Database Class

		We define our methods to simplify database communication using psycopg2
		http://initd.org/psycopg/docs/usage.html

		"""

		# construct psycopg2 connect string
		def __init__(self, host=None, port=None, user=None, dbname=None):
				"""Initializes a connection string

				This constuctor will predefine the connection string as psycopg2 (dsn)

				dbname – the database name (database is a deprecated alias)
				user – user name used to authenticate
				password – password used to authenticate
				host – database host address (defaults to UNIX socket if not provided)
				port – connection port number (defaults to 5432 if not provided)
 
				:returns:

				:param arg1: host   (default to 'localhost')
				:param arg2: port   (default to '5432')
				:param arg3: user   (default to 'postgres')
				:param arg4: dbname (default to 'sthpw')

				.. note::

				We do **not** process a password argument. Please provide a security relevant 
				configuration inside the home direcotry of the connectiong user (~/.pgpass) 

				"""

				self.host = host
				self.port = port
				self.user = user
				self.dbname = dbname

				if self.host is None:
						self.host = [ 'localhost' ]
				else:
						self.host = [ host ]

				if self.port is None:
						self.port = [ '5432' ]
				else:
						self.port = [ port ]
				if self.user is None:
						self.user = [ 'postgres' ]
				else:
						self.user = [ user ]

				if self.dbname is None:
								self.dbname = [ 'sthpw' ]
				else:
						self.dbname = [ dbname ]

				self.string_connect = "dbname=" + self.dbname[0] + " user=" + self.user[0] + " host=" + self.host[0] + " port=" + self.port[0]
				if (args.verbose) >= 3:
						print("[init] Connect string: %s" % self.string_connect)

		def connect(self):
				try:
						self.conn = psycopg2.connect(self.string_connect)
						if (args.verbose) >= 3:
								print("[connect] db-connection: %s" % self.conn)
								cursor = self.conn.cursor()
								if (args.verbose) >= 3:
										print("[connect] db-cursor: %s" % cursor)
				except Exception as e:
						print("[psycopg2] can't connect to dbname")
						print("[connect] connection string: %s'" % connect)
						print(e)

		def close(self):
				try:
						if (args.verbose) >= 3:
								print("[close] db-cursor: %s" % cursor)
								cursor.close()
								if (args.verbose) >= 3:
										print("[close] db-connection: %s" % self.connect)
										conn.close()
				except Exception as e:
						print("[psycopg2] can't close database connection")
						print(e)
				return

		def create(self, db):
				try:
						self.conn = psycopg2.connect(self.string_connect)
				except Exception as e:
						print("psycopg2 can't establish a connection")
						print("connection string: %s" % self.string_connect)
						print(e)
				self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
				cursor = self.conn.cursor()
				sql_string="CREATE DATABASE %s;"
				sql_param = (db, )
				if (args.verbose) >= 3:
						print("[create] sql command: %s %s" % (sql_string, sql_param))
				try:
						cursor.execute(sql_string, sql_param)
				except Exception as e:
						print("I can't create database '%s'!" % db)
						print(e)
				return

		def drop_with(self, db):
				try:
						with psycopg2.connect(self.string_connect) as self.conn:
								with self.conn.cursor() as self.cursor:
										self.conn.autocommit = True
										sql_string="DROP DATABASE " + db
										if (args.verbose) >= 3:
												print("[drop_with] sql command: %s " % sql_string)
												self.cursor.execute(sql_string)
				except Exception as e:
						print(e)

		def drop(self, db):
				try:
						self.conn = psycopg2.connect(self.string_connect)
				except Exception as e:
						print("psycopg2 can't establish a connection")
						print("connection string: %s" % self.string_connect)
						print(e)

				# needed to destroy database object
				self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
				cursor = self.conn.cursor()
				sql_string="DROP DATABASE " + db + ";"
				if (args.verbose) >= 3:
						print("[drop] sql command: %s " % sql_string)
				try:
						cursor.execute(sql_string)
				except Exception as e:
						print("I can't drop database '%s'!" % db)
						print(e)
				return

		def exist(self, db):
				try:
						conn = psycopg2.connect(self.string_connect)
				except Exception as e:
						print("[exist] can't establish a connection")
						return e

				cursor = conn.cursor()

				sql_cmd="SELECT datname FROM pg_database WHERE datistemplate = false AND datname='" + db + "';"
				if (args.verbose) >= 3:
						print("[exist] sql command: %s" % sql_cmd)
				try:
						cursor.execute(sql_cmd)
						rows = cursor.fetchall()
						if len(rows) == 1:
								if (args.verbose) >= 2:
										print("[exist] Database '%s' found" % rows[0])
										print("[exist] Encoding for this connection is '%s'" % conn.encoding)
								return True
						else:
								return False
				except Exception as db_error:
						if (args.verbose) >= 3:
								print("[psycopg2] no rows to fetch")
								return db_error
				finally:
						if conn is not None:
								cursor.close()
								conn.close()

		def info(self, db):
				try:
						self.conn = psycopg2.connect(self.string_connect)
				except Exception as e:
						print("[psycopg2] can't establish a connection")
						print(e)

				cursor = self.conn.cursor()

				sql_string="SELECT pg_size_pretty(pg_database_size(%s));"
				sql_param = (db, )
				if (args.verbose) >= 3:
						print("[info] sql command: %s %s" % (sql_string, sql_param))
				try:
						cursor.execute(sql_string, sql_param)
						size = cursor.fetchall()
						if (args.verbose) >= 3:
								print("[info] db-size: %s" % size)
				except Exception as e:
						print("[info] can't estimate the size for database '%s'!" % db)
						#print("[psycopg2]" % e)
				finally:
						if self.conn is not None:
								self.conn.close()
						return size[0]

		def rows_delete(self, project, table, clause):
				rows_deleted=0
				try:
						#self.conn = self.connect()
						self.conn = psycopg2.connect(self.string_connect)
				except Exception as e:
						print("[exist] can't establish a connection")
						return e

				cursor = self.conn.cursor()

				# Never, never, NEVER use Python string concatenation (+) or string
				# parameters interpolation (%) to pass variables to a SQL query
				# string. Not even at gunpoint.
				# The correct way to pass variables in a SQL command
				# is using the second argument of the execute() method
				sql_string = "DELETE FROM " + table + " WHERE " + clause + "=%s;" # Note: no quotes
				sql_param = (project, )
				if (args.verbose) >= 3:
						print("[delete] sql command: %s %s" % (sql_string, sql_param))
				try:
						cursor.execute(sql_string, sql_param)
						# commit the changes to the database
						self.conn.commit()
						rows_deleted = cursor.rowcount()
						cursor.close()
				except Exception as e:
						if str(e) == "'int' object is not callable":
								return 0
						if str(e) == 'no results to fetch':
								return 0
						else:
								print("[psycopg2] can't delete from table")
								print("Error: '%s'" % e)
				finally:
						if self.conn is not None:
								self.conn.close()
								return rows_deleted

		def rows_list(self, project, table, clause):
				try:
						#self.connect()
						self.conn = psycopg2.connect(self.string_connect)
						cursor = self.conn.cursor()
						sql_cmd="SELECT * FROM " + table + " WHERE " + clause + "='" + project + "';"
						if (args.verbose) >= 3:
								print(" -> sql command: %s" % sql_cmd)
						cursor.execute(sql_cmd)
						rows = cursor.fetchall()
						cursor.close()
				except Exception as e:
						print("psycopg2 can't establish a connection")
						print("connection string: %s" % self.string_connect)
						print(e)
				finally:
						if self.conn is not None:
								self.conn.close()
						return len(rows)

		def rows_truncate(self, project, table, clause):
				try:
						#self.conn = self.connect()
						self.conn = psycopg2.connect(self.string_connect)
						cursor = self.conn.cursor()

						# Never, never, NEVER use Python string concatenation (+) or string
						# parameters interpolation (%) to pass variables to a SQL query
						# string. Not even at gunpoint.
						# The correct way to pass variables in a SQL command
						# is using the second argument of the execute() method
						sql_string = "TRUNCATE " + table + ";" # Note: no quotes
						cursor.execute(sql_string)
						if (args.verbose) >= 3:
								print(" -> sql command: %s" % sql_string)
								# commit the changes to the database
								self.conn.commit()
								cursor.close()
				except Exception as e:
						print("psycopg2 can't establish a connection")
						print("connection string: %s" % self.string_connect)
						print("Error: '%s'" % e)
				finally:
						if self.conn is not None:
								self.conn.close()
						return rows_deleted


###
# Main
##

if __name__ == "__main__":
    import doctest
    doctest.testmod()

# process calling arguments
args = parse_args()
if len(projects) is 0:
		print("please provide a tacitc projects list")
		sys.exit(1)

if (args.dryrun) is False:
		# create db object
		db = DB(args.host, args.port, args.user, args.dbname)

		# process given database
		ret = db.exist(args.dbname)
		if ret is True:
				if (args.verbose) >= 1:
						print("Database: %s" % db.dbname)
				if (args.verbose) >= 3:
						print("DB-Connection: %s" % db)
		else:
		   		# Database connection error
				print("%s" % ret)
				sys.exit(1)
else:
		print("would run: db.exist(%s)" % args.dbname)

if args.mode == 'list':
		if (args.dryrun) is True:
				print("would run: projects_rows_list(%s)" % projects)
		else:
				ret = projects_rows_list(projects)
				if ret is not None:
						print("return value %s" % ret)
elif args.mode == 'delete':
		if (args.dryrun) is True:
				print("would run: projects_delete(%s)" % projects)
				sys.exit()
		else:
				ret = projects_delete(projects)
				if ret is not None:
						print("return value %s" % ret)
elif args.mode == 'info':
		if (args.dryrun) is True:
				print("would run: projects_info(%s)" % projects)
		else:
				ret = projects_info(projects)
				if ret is not None:
						print("return value %s" % ret)

# end
