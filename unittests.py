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

class Cleaning_Test(unittest.TestCase):
	def setUp(self):
		df_users = pd.DataFrame({
		'id': [1, 2, 3],
		'name': ['Alice', 'Bob', 'Charlie'],
		'age': [25, 30, 35],
		'signup_date': pd.to_datetime(['2023-01-01', '2023-06-15', '2023-08-22']),
		'is_active': [True, False, True]
		})

		df_orders = pd.DataFrame({
		    'order_id': [101, 102, 103],
		    'user_id': [1, 2, 1],
		    'amount': [250.5, 100.75, 300.0],
		    'order_date': pd.to_datetime(['2023-07-01', '2023-07-03', '2023-07-10']),
		    'status': ['shipped', 'delivered', 'pending']
		})

		df_products = pd.DataFrame({
		    'product_id': [1, 2, 3,3],
		    'product_name': ['Laptop', 'Headphones', 'Monitor','Monitor'],
		    'price': [1000.00, 150.00, 300.00,300.00],
		    'in_stock': [True, True, False,False]
		})
		self.test_dict = {
		    'users': df_users,
		    'orders': df_orders,
		    'products': df_products
		}

	def tearDown(self):
		del self.test_dict

	def test_drop_duplicates(self):
		test_df = self.test_dict['products']
		test_df.drop_duplicates(inplace=True)
		expected_df = pd.DataFrame({
		    'product_id': [1, 2, 3],
		    'product_name': ['Laptop', 'Headphones', 'Monitor'],
		    'price': [1000.00, 150.00, 300.00],
		    'in_stock': [True, True, False]
		})
		pd.testing.assert_frame_equal(test_df,expected_df)	

	def test_create_db(self):
		engine = create_engine('sqlite:///test_database.db')
		self.assertIsInstance(engine,Engine)
		del engine			

if __name__ == '__main__':
    unittest.main()











