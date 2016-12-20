#coding:utf-8
import requests
import config
import sqlite3
from lxml import etree
import time
import random
from multiprocessing import Pool
import os, time, random

class Spider(object):
	"""docstring for Splider"""
	def __init__(self):
		super(Spider, self).__init__()
		self.sl = DBHelper()

	def run(self):
		
		for parser in config.parserList:
			for url in parser['urls']:
				self.get_proxy(url,parser['pattern'],parser['postion'])
		self.sl.close()
				

	def get_proxy(self,url,pattern,postion):

		proxy_array = []
		proxy_db = self.sl.select(count = 'limit 0,20')

		random_headers = config.HEADER
		random_headers['User-Agent'] = random.choice(config.USER_AGENTS)

		random_proxy_address = ''
		try:
			random_proxy = random.choice(proxy_db)
			random_proxy_address = '%s:%s'%(random_proxy[0],random_proxy[1])
			proxies = {"http": "http://{proxy}".format(proxy=random_proxy_address),
                       "https": "https://{proxy}".format(proxy=random_proxy_address)}			
		except: 
			print ("Error to fetch database")
		
		try:
			r = requests.get(url,headers = random_headers,timeout = config.TIMEOUT)
			if r.status_code == requests.codes.ok:
				r.encoding = 'utf-8'
				et = etree.HTML(r.text)				
				result_even = et.xpath(pattern)

				for res in result_even:
					proxy_args = {}
					for key,value in postion.items():
						proxy_args[key] = res.xpath(value)[0]
					print (proxy_args)
					if Spider.is_alive(proxy_args):
						proxy_array.append(proxy_args)
			self.sl.batch_insert(proxy_array)
			self.sl.commit()
		except:
			print ('get_proxy read html error %s'%(url))

	def is_alive(args):
		random_headers = config.HEADER
		random_headers['User-Agent'] = random.choice(config.USER_AGENTS)
		proxy_address = '%s:%s'%(args['ip'],args['port'])
		proxies = {"http": "http://{proxy}".format(proxy=proxy_address),
                       "https": "https://{proxy}".format(proxy=proxy_address)}
		try:
			r = requests.get(config.TEST_URL,headers = random_headers,proxies = proxies ,timeout = config.TIMEOUT)
			if r.status_code == requests.codes.ok:
				args['speed'] = r.elapsed.microseconds
				return True
			else:
				return False
		except:
			print('--------------------------%s',(proxy_address))
			return False



class DBHelper(object):
	"""docstring for Proxy"""
	tableName='proxys'
	def __init__(self):
		super(DBHelper, self).__init__()
		self.database = sqlite3.connect(config.DB_CONFIG['dbPath'],check_same_thread=False)
		self.cursor = self.database.cursor()
		self.createTable()

	def createTable(self):
		self.cursor.execute("create TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY ,ip VARCHAR(16) NOT NULL,"
               "port INTEGER NOT NULL ,updatetime TimeStamp NOT NULL DEFAULT (datetime('now','localtime')) ,speed DECIMAL(3,2) NOT NULL DEFAULT 100)"% self.tableName)

		self.database.commit()

	def close(self):
		self.commit()
		self.cursor.close()
		self.database.close()

	def insert(self,value):
		print(value)
		proxy = [value['ip'],value['port'],value['speed']]
		self.cursor.execute("INSERT INTO %s (ip,port,speed)VALUES (?,?,?)"% self.tableName,proxy)

	def update(self,condition,value):
		self.cursor.execute('UPDATE %s %s'%(self.tableName,condition),value)
		self.database.commit()

	def commit(self):
		self.database.commit()

	def selectAll(self):
		self.cursor.execute('SELECT DISTINCT ip,port FROM %s ORDER BY speed ASC '%self.tableName)
		result = self.cursor.fetchall()
		return result

	def select(self,count):
		command = 'SELECT DISTINCT ip,port FROM %s ORDER BY speed ASC %s '%(self.tableName,count)
		self.cursor.execute(command)
		result = self.cursor.fetchall()
		return result

	def batch_insert(self,values):
		for value in values:
			if value!=None:
				self.insert(value)
		self.database.commit()

	def delete(self,condition):
        # '''
        # :param tableName: 表名
        # :param condition: 条件
        # :return:
        # '''
		deleCommand = """DELETE FROM %s WHERE %s;"""%(self.tableName,condition)
		self.database.execute(deleCommand)
		self.commit()

	def count(self):
		deleCommand = 'SELECT COUNT( DISTINCT ip) FROM %s'%(self.tableName)
		self.cursor.execute(deleCommand)
		count = self.cursor.fetchone()
		return count


if __name__ == "__main__":
	sl = DBHelper()
	while True:
		print('clean')
		proxy_dbs = sl.selectAll()
		for proxy_db in proxy_dbs:
			if Spider.is_alive(args = {'ip':proxy_db[0],'port':proxy_db[1]}) == False:
				sl.delete("""ip = '%s'"""%(proxy_db[0]))
		print('clean up')


		print('find')
		if sl.count()[0] < config.MINNUM:
			sp = Spider()		
			sp.run()
		print('find over')
		time.sleep(config.UPDATE_TIME)
