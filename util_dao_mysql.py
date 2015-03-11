__author__ = 'BensonHo'

import mysql.connector as msql
from mosql.query import insert, select, update, delete
from mosql.util import raw
import mosql.mysql
import pandas as pd
import datetime as dt
is_debug = False


class MyDao:
	def __init__(self, user, password, host, database, port):
		self.msqlcons = msql.connect(user=user, password=password, host=host, database=database, port=port)

	def insert(self, table, dict_data):
		sql_string = insert(table, dict_data)
		if is_debug:
			print sql_string
		try:
			cursor = self.msqlcons.cursor()
			cursor.execute(sql_string)
		except Exception, e:
			print "Couldn't do it: %s" % e

	def select(self, table, where=None, columns=None, order_by=None, limit=None, group_by=None):
		sql_string = select(table, where=where, columns=columns, order_by=order_by, limit=limit, group_by=group_by)
		if is_debug:
			print sql_string
		try:
			cursor = self.msqlcons.cursor()
			cursor.execute(sql_string)
			return cursor.fetchall()
		except Exception, e:
			print "Couldn't do it: %s" % e

	def __del__(self):
		if self.msqlcons:
			self.msqlcons.close()


class VecsInfoDao:
	def __init__(self):
		self.daos = {}

	def get_dao(self):
		self.daos = MyDao(user='root', password='root', host='localhost', database='vecsgardenia_2015', port=8881)
		return self.daos

	def get_referer(self):
		dao = self.get_dao()
		data = dao.select('oc_online_plus_archive', columns=['time_arrived', 'base_referer'], order_by='time_arrived asc')
		if data:
			return data

	def __del__(self):
		del self.daos



vecsInfo_dao = VecsInfoDao()

if __name__ == '__main__':
	data = vecsInfo_dao.get_referer()
	df = pd.DataFrame(data, columns=['da', 'base'])
	df['base'] = df['base'].apply(lambda x: x.decode('utf-8'))
	df['da'] = df['da'].apply(lambda x: x.strftime('%Y-%m-%d %H'))
	print df.head(10)