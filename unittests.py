import unittest
import sqlite3 
import tempfile
import random
import os
from utils import *
from sqlalchemy.engine import Engine


class Database_Conversion_Test(unittest.TestCase):
	def setUp(self):
		self.database = tempfile.NamedTemporaryFile(delete=False)
		self.con = sqlite3.connect(self.database.name)
		self.curs = self.con.cursor()
		self.curs.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)")
		self.curs.executemany("INSERT INTO test_table (value) VALUES (?)", [("A",), ("B",), ("C",)])
		self.con.commit()


	def tearDown(self):
		self.con.close()
		if os.path.exists(self.database.name):
			os.remove(self.database.name)


	def test_connect_s3_database(self):
		self.assertIsInstance(self.con, sqlite3.Connection)
		self.assertIsInstance(self.curs, sqlite3.Cursor)
		self.assertFalse(self.con.close())
		with self.assertRaises(sqlite3.ProgrammingError):
			self.curs.execute("SELECT 1")


	def test_load_table_to_df(self):
		df = load_table_to_df(self.curs,'test_table')
		expected_df = pd.DataFrame({'id': [1, 2, 3], 'value': ['A', 'B', 'C']})
		pd.testing.assert_frame_equal(df, expected_df)


	def test_get_tables_list(self):
		self.curs.execute('CREATE TABLE test_table_2 (id INTEGER PRIMARY KEY, name TEXT)')
		self.curs.executemany("INSERT INTO test_table_2 (id, name) VALUES (?, ?)", [(1, "John"), (2, "Jürgen"), (3, "Johann")])
		self.curs.execute('SELECT name from sqlite_master WHERE type = "table"')
		self.assertEqual(['test_table','test_table_2'],[table[0] for table in self.curs.fetchall()])


	def test_create_db(self):
		db_name = random.randint(0,	100)
		engine = create_engine(f'sqlite:///{db_name}.db')
		self.assertIsInstance(engine,Engine)
		self.assertEqual(f'Engine(sqlite:///{db_name}.db)',str(engine))




if __name__ == '__main__':
    unittest.main()









