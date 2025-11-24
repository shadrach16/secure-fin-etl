import sys
import os
# import django
# sys.path.append(r'E:\rica\backend\bots')
# os.environ['DJANGO_SETTINGS_MODULE'] ='ricabackend.settings'
# django.setup()
from FILETEST.db_client import create_connection
from FILETEST.func import get_env_settings,get_ip,logError,replaceParams
from FILETEST.func import convert_time, format_int, format_int_bin, is_time_between, get_first, get_last, clean,GenRunDate,get_query_fields,get_multi_value,get_spf,get_footer_label,clean_emails,find_grouping,license_expired,get_expired_notification
from FILETEST.datahelper import getChart3Data,getLineChartData,generateAnalytics
from FILETEST.date import date_func, create_date, create_time
from FILETEST.ricaED import E 

from FILETEST.mailer import send_response_mail

import cx_Oracle
from sqlalchemy import create_engine,inspect,MetaData,text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError,DatabaseError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.automap import automap_base
import pandas as pd
import etlhelper as etl
from textwrap import dedent

from FILETEST.xlsx import create_lite_excel
from FILETEST.first import  step_1,  get_status
from FILETEST.config import LOG_PATH,FULL_LOG_PATH,STATUS,MAP, time_formatter,date_formatter,DTTYPES,language,USE_THREAD,BATCH_SIZE
from FILETEST.common import load_queries
import re
from pathlib import Path
import datetime
from datetime import time 
import datetime as dt
import argparse 
import time 
from urllib.parse import urljoin
path = os.getcwd()
sys.path.append(path)
import json
import shutil
from typing import Iterator
from etlhelper.row_factories import dict_row_factory
from concurrent.futures import ThreadPoolExecutor, as_completed

import pathlib
import fnmatch

enc = E("@adr0it")





def clean(string=''):
	if string:
		return re.sub(r'[-:\s\.]*', "", str(string))
	else:
		return ""

class DatabaseConnector():
	"""docstring for DatasetConnector""" 

	def __init__(self,file_path,dir_name,service={}):
		self.dir_name = dir_name
		self.file_path = file_path
		self.db_type = service.get('ricaDatabaseType')  
		self.host =  service.get('ricaDatabaseHost')  
		self.port = service.get('ricaDatabasePort') 
		self.db_name = service.get('ricaDatabaseName')  
		self.username = service.get('ricaUser') 
		self.password = service.get('ricaPassword')   
		self.oracle_client_dir = service.get('ricaService')
		self.odbc_driver = service.get('ricaService') 
		self.service = service
		self.state = True
		self.connect_to_database()

	def connect_to_database(self):
		credentials = get_env_settings() 

		try:
			# Extract form inputs
			db_type = self.db_type or str(credentials['DATABASE_TYPE']).lower() 
			if db_type == 'oracle':
				spldb = str(credentials['DATABASE_NAME']).split("/")[0].split(":")
				host = self.host or credentials.get('DATABASE_HOST') or spldb[0]
				port = self.port or credentials.get('DATABASE_PORT')  or spldb[1]
			else:
				host = self.host or credentials.get('DATABASE_HOST') 
				port = self.port or credentials.get('DATABASE_PORT') 
			db_name = self.db_name or str(credentials['DATABASE_NAME']).split("/")[-1]
			username = self.username or credentials['DATABASE_USER']
			password = enc.D(  self.password or credentials['DATABASE_PASSWORD']) 
			print('here 1',db_type)

			# Database connection URL
			db_type = db_type.lower()
			if db_type == 'postgresql':
				db_url = f'postgresql://{username}:{password}@{host}:{port}/{db_name}'
			elif db_type == 'mysql':
				db_url = f'mysql+pymysql://{username}:{password}@{host}:{port}/{db_name}'
			elif db_type == 'sqlite':
				db_url = f'sqlite:///{db_name}'
			elif db_type == 'oracle':
				try:
					cx_Oracle.init_oracle_client(r"{}".format(self.oracle_client_dir or credentials.get('ORACLE_CLIENT_DIR', r"D:\oracle\instantclient_19_11")))
				except:
					pass
				db_url = f'oracle://{username}:{password}@{host}:{port}/{db_name}'
			elif db_type == 'mssql':
				driver = self.odbc_driver or 'ODBC Driver 17 for SQL Server'
				if port:
					db_url = f'mssql+pyodbc://{username}:{password}@{host}:{port}/{db_name}?driver={driver}' 
				else:
					db_url = f'mssql+pyodbc://{username}:{password}@{host}/{db_name}?driver={driver}' 
			elif db_type == 'sqlite':
				db_url = f'sqlite:///{db_name}' 
			elif db_type == 'MariaDB':
				db_url = 'mysql+pymysql://{username}:{password}@{host}:{port}/{db_name}'
			else:
				raise ValueError('Invalid database type')
			# Connect to the database 
			print('Loading DB configs.. ' )

			date_now = str(dt.date.today()).replace("-","")
			time_now = dt.datetime.now()
			current_time = str(time_now.strftime("%H:%M:%S")).replace(":","")

			log_args = {
			"ricaLogId":f'Table-{self.dir_name}-{date_now}-{current_time}',
			'ricaApplication':"ETL Service State",
			'Folder':self.dir_name,
			'ricaText':"ETL: Create Engine, connecting to db..",
			'ricaStatus':"Ongoing",
			'ricaRunDate':date_now,
			'ricaRunTime':current_time,					   
			}
			logError("State").log(log_args,'Main')
			print(db_url)

			self.engine = create_engine(db_url)
			self.engine.connect()
			Session = sessionmaker(bind=self.engine)
			self.session = Session() 
			log_args['ricaText'] = f"ETL: DB Connected.."
			logError("State").log(log_args,'Main')
			# print('Connected',db_url)
			self.state =  True
		except Exception  as e:
			print('eeeeeeeee',e)
			date_now = str(dt.date.today()).replace("-","")
			time_now = dt.datetime.now()
			current_time = str(time_now.strftime("%H:%M:%S")).replace(":","")

			log_args = {
			"ricaLogId":f'Table-DB-{date_now}-{current_time}',
			'ricaApplication':f"FILETEST",
			'Folder':db_url,
			'ricaText':f"Error: {e}",
			'ricaStatus':"Error",
			'ricaRunDate':date_now,
			'ricaRunTime':current_time,					   
			}
			logError("Error").log(log_args,'Main')

			send_email_error(self, '\n'.join([f"{key}: {value}" for key, value in log_args.items()]),self.get_spf())

			self.state =  False

			# sys.exit(0)

	def extract_mapper(self,mappers):
		try:
			return {value.strip() : key.strip() for key, value in (pair.split('->') for pair in str(mappers).rstrip(";").replace("\n","").replace(" ","").split(';'))} 
		except Exception as e:
			print(e)
			return {}

	def extract_mapper_from_fields(self,mappers):
		map_key = {}
		try:
			mappers = re.sub(r'\s+', "", mappers)
			for key, value in (pair.split('->') for pair in str(mappers).rstrip(";").split(';')):
				if map_key.get(value):
					map_key[value].append(key)
				else:
					map_key[value] = [key]
		except Exception as e:
			print(e)
		return map_key

	def extract_field_mappers(self,mappers):
		field_maps = {}

		try:

			mappers = re.sub(r'\s+', "", mappers)
			tables = re.findall(r'\[[^\]]+\]',mappers)
			splitlist = [x for x in re.split(r'\[[^\]]+\]',mappers) if x.replace(" ","")]
			for index, table in enumerate(tables):
				field_maps[table.replace('[',"").replace(']',"")] = self.extract_mapper_from_fields(splitlist[index]) or {}

		except Exception as e:
			print("Err @ extract_field_mappers: ",e)

		return field_maps


	def get_credentials(self,CONNECTOR):

		data = None
		get_config_query = text(load_queries.get('get_creds_query').format(CONNECTOR=CONNECTOR) )
		result = self.session.execute(get_config_query) 
		column_names = [desc[0] for desc in result.cursor.description]  # Retrieve column names from the ResultSet
		while True:
			rows = result.fetchmany(50)  # Retrieve 50 rows at a time
			if not rows:
				break
			data = [dict(zip(column_names, row)) for row in rows]
		if len(data):
			df = pd.DataFrame(data)
			df.fillna('', inplace=True)
			filtered_mapper = {k: v for k, v in MAP.items() if k in df.columns}
			if len(filtered_mapper.values()) == 0:
				return data[0]
			renamed_df = df.rename(columns=filtered_mapper)
			renamed_df =  renamed_df[filtered_mapper.values()].to_dict('records')
		else:
			return {}
	
	def fetch_many(self,query_name,params={'a':'a'},chunk=50):

		data = None
		get_config_query = text(load_queries.get(query_name).format(**params) )
		result = self.session.execute(get_config_query) 
		column_names = [desc[0] for desc in result.cursor.description]  # Retrieve column names from the ResultSet
		while True:
			rows = result.fetchmany(chunk)  # Retrieve 50 rows at a time
			if not rows:
				break
			data = [dict(zip(column_names, row)) for row in rows]
		
		if len(data):
			df = pd.DataFrame(data)
			df.fillna('', inplace=True)
			filtered_mapper = {k: v for k, v in MAP.items() if k in df.columns}
			renamed_df = df.rename(columns=filtered_mapper)
			return renamed_df[filtered_mapper.values()].to_dict('records')
		else:
			return []




	def get_column_and_primary_key(self, table_name):
		try:
			date_now = str(dt.date.today()).replace("-","")
			time_now = dt.datetime.now()
			current_time = str(time_now.strftime("%H:%M:%S")).replace(":","")
			log_args = {
			"ricaLogId":f'Table-{self.dir_name}-{date_now}-{current_time}',
			'ricaApplication':"ETL Service State",
			'Folder':self.dir_name,
			'ricaText':f"ETL: Getting Columns Information..",
			'ricaStatus':"Ongoing",
			'ricaRunDate':date_now,
			'ricaRunTime':current_time,					   
			}
			logError("State").log(log_args,'Main')
			Base = declarative_base(bind=self.engine)
			Base.metadata.reflect()
			if table_name.lower() in list(Base.metadata.tables.keys()):
				table = Base.metadata.tables[table_name.lower()]
				columns_data_types = {column.name: column.type for column in table.columns  } #if str(column.type).upper() in DTTYPES
				primary_key_column = None #inspector.get_primary_keys(table_name)
				log_args['ricaText'] = f"ETL: DB Columns info extracted Successfully.."
				logError("State").log(log_args,'Main')
				return primary_key_column,columns_data_types
			else:
				print(f"Couldn't find columns data type for {table_name} or Table: {table_name} does not exist")
				return None,{}
		except Exception as e:
			return None,{}

   

	def extract_field_mappers(self,mappers):
		field_maps = {}
		mappers = mappers.strip().replace(";","").split('\n')
		for x in mappers:
			sign_fd = x.split("->")
			key = sign_fd[1]
			value = sign_fd[0]
			field_maps[key] = value
		return field_maps



	def validateVal(self,ky, val):
		try:
			float(val)
			return val
		except Exception as e:
			return f"'{val}'"



	def get_pk(self,table_name):
		pk = self.fetch_many('GET_PK',{"table_name":table_name},chunk=1 )
		print('pk',pk)
		return get_first(pk).get("column_name")




	def post(self,data=[],table_name="",pk="",columns_dtypes={},file_name="",service={},batch_size=1000):
		
		date_now = str(dt.date.today()).replace("-","")
		time_now = dt.datetime.now()
		current_time = str(time_now.strftime("%H:%M:%S")).replace(":","")
		log_args = {
		"ricaLogId":f'Table-{table_name}-{date_now}-{current_time}',
		'ricaApplication':"ETL Service State",
		'Folder':table_name,
		'ricaText':f"ETL: Start processing data into db..",
		'ricaStatus':"Ongoing",
		'ricaRunDate':date_now,
		'ricaRunTime':current_time,					   
		}
		logError("State").log(log_args,'Main')


		date_now = str(dt.date.today()).replace("-","")
		time_now = dt.datetime.now()
		current_time = str(time_now.strftime("%H:%M:%S")).replace(":","")
		error = False

		try:
			pk = self.get_pk(table_name)
			status = None

			for i in range(0, len(data), batch_size):
				batch = data[i:i+batch_size]
		
				for entry in batch: 
					entry =  {k: (v if v != '' else 'NULL') for k, v in entry.items()}
					try:
						placeholders = [ret_oup(entry,key,columns_dtypes) for key in entry.keys()]
						insert_sql = f"INSERT INTO {table_name} ({', '.join(entry.keys())}) VALUES ({', '.join(placeholders)})"
						# print(insert_sql)
						insert_sql = dedent(insert_sql.format(**entry).replace("'NULL'", 'NULL') ) 
						print(insert_sql,entry)
						# print("Committing Data")
						self.session.execute(insert_sql)
						status='Created'
						print('COMMIT')
						# self.session.execute(f"INSERT INTO {table_name} ({', '.join(entry.keys())}) VALUES ({', '.join([f':{key}' for key in entry.keys()])})", {**entry})
					except IntegrityError as e:
						self.session.rollback()
						if 'unique' in str(e).lower() or 'constraint' in str(e).lower():
							placeholders = [f"{key} = {ret_oup(entry,key,columns_dtypes)}" for key in entry.keys() if key != pk]
							update_sql = f"UPDATE {table_name} SET {', '.join(placeholders)} WHERE {pk} = '{entry.get(pk)}'" 
							# print(update_sql.format(**entry).replace("'NULL'", 'NULL'))
							update_sql = dedent(update_sql.format(**entry).replace("'NULL'", 'NULL') ) 
							print(update_sql)
							self.session.execute(update_sql)
							status='Updated'
							print('UPDATED')
						else:
							log_args = {
							"ricaLogId":f'File-{table_name}-{date_now}-{current_time}',
							'ricaApplication':"ETL Service", 
							'Folder':table_name,
							'ricaTable':table_name,
							'ricaText':f"ETL: {e}",
							'ricaStatus':"Failed",
							'ricaRunDate':date_now,
							'ricaRunTime':current_time,					   
							}

							logError("Error").log(log_args,table_name) 

					log_args = {
						"ricaLogId":f'File-{table_name}-{date_now}-{current_time}',
						'ricaApplication':"ETL Service", 
						'Folder':table_name,
						'ricaTable':table_name,
						'ricaText':f"ETL: data {status} successfully on {date_now}",
						'ricaStatus':"Success",
						'ricaRunDate':date_now,
						'ricaRunTime':current_time,					   
						}

					logError("Success").log(log_args,table_name)
				self.session.commit()

		except Exception as e:
			print(e)
			self.session.rollback()
			log_args = {
				"ricaLogId":f'Table-{table_name}-{date_now}-{current_time}',
				'ricaApplication':"ETL Service: FILETEST",  
				'Folder':table_name,
				'ricaTable':table_name,
				'ricaText':f"{e}",
				'ricaStatus':"Failed",
				'ricaRunDate':date_now,
				'ricaRunTime':current_time,
			   
				}
			send_email_error(self, '\n'.join([f"{key}: {value}" for key, value in log_args.items()]),self.get_spf())

			logError("Error").log(log_args,table_name)  
			error = True

		return error

		# Close the session
		self.session.close()

	def get_spf(self):
		MAP = {
			"RICASTMPMAILADDRESS": "ricaStmpMailAddress",
			"RICASTMPMAILSERVER": "ricaStmpMailServer",
			"RICASTMPMAILPORT": "ricaStmpMailPort",
			"RICASTMPMAILUSER": "ricaStmpMailUser",
			"RICASTMPMAILPASSWORD": "ricaStmpMailPassword",
			"RICARELEASENO": "ricaReleaseNo",
			"RICAAPPSID": "ricaAppsId",

		}
		data = []
		get_config_query = text(load_queries.get('GET_SPF').format(lang=language) )
		result = self.session.execute(get_config_query) 
		column_names = [MAP.get(desc[0],desc[0]) for desc in result.cursor.description]  # Retrieve column names from the ResultSet
		while True:
			rows = result.fetchmany(1)  # Retrieve 50 rows at a time
			if not rows:
				break
			data = [dict(zip(column_names, row)) for row in rows]

		return data[0] if len(data) else {}





def send_email_error( connector, error,spf): 
	authorizer_emails =  []
	external_obj = {}
	external_obj['app'] = spf.get("ricaAppsId")
	external_obj['systemDate'] = str(dt.datetime.now().date())
	external_obj['systemTime'] = str(dt.datetime.now().time()) 
	

	try:
		user = connector.fetch_many('GET_USER',{'user_id':'superuser'})[0]
		inputter_emails = [user.get("ricaUserEmail") ]

		ruq = connector.fetch_many('GET_USER_REQUEST',{'id':'payload'})[0] 
		subject = replaceParams(ruq.get('ricaSubject'),external_obj)
		owner_flag = connector.fetch_many('GET_STATUS',{'id':ruq.get('ricaOwnerFlag')})[0] 
		if str(owner_flag.get('ricaModelflag'))=='1':
			for x in connector.fetch_many('GET_USER_INFO_VIA_DESIG',{'designate':ruq.get('ricaOwner') })   :
				authorizer_emails.append(str(x['ricaUserEmail'].strip()))

		if ruq.get('ricaEmailReceiver'):
			count = 0
			json_receiver = json.loads(ruq.get('ricaEmailReceiver') ) 
			print('json_receiver',json_receiver)
			for key, x in enumerate(json_receiver):
				if "ricaEmailReciever" in x:
					designate =  json_receiver.get(x)   
					print('designate',designate)
					flag =  json_receiver.get("ricaReciever."+str(count)+".ricaFlag")   
					count+=1 
					rec_flag = connector.fetch_many('GET_STATUS',{'id':flag})[0] 
					if str(rec_flag.get('ricaModelflag') ) =='1': 
						for x in connector.fetch_many('GET_USER_INFO_VIA_DESIG',{'designate':designate}) :
							authorizer_emails.append(str(x['ricaUserEmail'].strip()) )
	
		options = {} 
		options['error'] = error
		options['systemDate'] = str(dt.datetime.now().date())
		options['systemTime'] = str(dt.datetime.now().time()) 


		if ruq.get('ricaInputterMsg'):
			inputter_message = replaceParams( connector.fetch_many('GET_MESSAGES',{'id':ruq.get('ricaInputterMsg')})[0].get('ricaMessage'),external_obj)
			options['send_to'] =  {'to': authorizer_emails,'cc': inputter_emails}
			options['message'] = inputter_message
			options['code'] = ruq.get('ricaInputterMsg')
			send_response_mail(options, spf)
		# print(options)

		if ruq.get('ricaAuthorizerMsg'):
			authorizer_message = replaceParams(connector.fetch_many('GET_MESSAGES',{'id':ruq.get('ricaAuthorizerMsg')})[0].get('ricaMessage')  ,external_obj )
			options['send_to'] = {'to': authorizer_emails,'cc': inputter_emails}  
			options['message'] = authorizer_message
			options['code'] = ruq.get('ricaAuthorizerMsg')
			send_response_mail(options, spf)

		print(options)

	except Exception as e:
			print('Payload error MAIL: ',e)

def format_(dtype,date_string):
	try:
		if date_string:
			date_string = str(date_string)
			if str(dtype).strip() == 'DATE':
				dt_fmt = dt.datetime.strptime(date_string, '%Y%m%d').strftime(date_formatter)
				return f"TO_DATE('{dt_fmt}','yyyy-mm-dd')"
			elif str(dtype).strip() == 'TIMESTAMP':
				if date_string == '2400':
					date_string = '0000'
				dt_fmt =  dt.datetime.strptime(date_string, '%H%M').strftime(time_formatter)
				return f"TO_TIMESTAMP('{dt_fmt}','HH24:MI:SS')"
			elif 'FLOAT' in str(dtype).strip() :
				return float(date_string)
			elif "'" in date_string:
				return date_string.replace("'","''")
			else:
				return date_string
		else:
			return f"NULL"
	except Exception as e:
		return f"NULL"


def formatter(data=[],columns_dtypes={}):
	new_data = []
	skipped_fields = [] 
	for rows in data:
		new_config = {}
		row_keys = rows.keys() 
		skipped_dicts = {}
		for key in row_keys:
			if not columns_dtypes.get(key.lower()):
				skipped_dicts[key] = rows[key]				 
				continue
			new_config[key] = format_(columns_dtypes.get(key.lower()),rows[key] ) 
		if len(skipped_dicts.keys()):
			skipped_fields.append(skipped_dicts)
		new_data.append(new_config)
	return new_data,skipped_fields




def ret_oup(entry,key,columns_dtypes):
	if entry[key]:
		if str(columns_dtypes.get(key.lower(),key) ) in DTTYPES:
			return "{"+key+"}" 
		else:
			return "'{"+key+"}'" 
	else:
		return "{"+key+"}"



def replace_values_with_mapping(dictionary, mapping):
	for key in mapping.keys():
		for field in mapping[key]:
			dictionary[field] = dictionary[key]
	return dictionary

db_connector = DatabaseConnector('', '')


def connect_to_database(service={}):
	credentials = get_env_settings() 

	db_type = service.get('ricaDatabaseType')  
	host =  service.get('ricaDatabaseHost')  
	port = service.get('ricaDatabasePort') 
	db_name = service.get('ricaDatabaseName')  
	username = service.get('ricaUser') 
	password = service.get('ricaPassword')   
	oracle_client_dir = service.get('ricaService')
	odbc_driver = service.get('ricaService') 
	service = service
	state = True

	conn = None

	try:
		# Extract form inputs
		db_type = db_type or str(credentials['DATABASE_TYPE']).lower() 
		if db_type == 'oracle':
			spldb = str(credentials['DATABASE_NAME']).split("/")[0].split(":")
			host = host or credentials.get('DATABASE_HOST') or spldb[0]
			port = port or credentials.get('DATABASE_PORT')  or spldb[1]
		else:
			host = host or credentials.get('DATABASE_HOST') 
			port = port or credentials.get('DATABASE_PORT') 
		db_name = db_name or str(credentials['DATABASE_NAME']).split("/")[-1]
		username = username or credentials['DATABASE_USER']
		password = enc.D(  password or credentials['DATABASE_PASSWORD']) 
		
		db_type = db_type.lower()
		os.environ["PASSWORD"] = password
		if db_type == 'postgresql':
			conn = etl.DbParams(
				dbtype="PG",
				host=host,
				port=port,
				dbname=db_name,
				user=username,
			)
		elif db_type == 'mysql':
			conn = etl.DbParams(
				dbtype="MYSQL",
				host=host,
				port=port,
				dbname=db_name,
				user=username,
			)
		elif db_type == 'sqlite':
			conn = etl.DbParams(
					dbtype="SQLITE",
					filename=db_name,
				)
		elif db_type == 'oracle':
			try:
				cx_Oracle.init_oracle_client(r"{}".format(oracle_client_dir or credentials.get('ORACLE_CLIENT_DIR', r"D:\oracle\instantclient_19_11")))
			except:
				pass

			conn = etl.DbParams(
					dbtype="ORACLE",
					host=host,
					port=port,
					dbname=db_name,
					user=username,
				)

		elif db_type == 'mssql':
			driver = odbc_driver or 'ODBC Driver 17 for SQL Server'

			conn = etl.DbParams(
					dbtype="MSSQL",
					host=host,
					port=port,
					dbname=db_name,
					user=username,
					odbc_driver=driver,
				)
		else:
			raise ValueError('Invalid database type')
		
		return etl.connect(conn, "PASSWORD")
	except Exception as e:
		print("connection error occured",e)
		send_email_error(db_connector, e,db_connector.get_spf())


def format_date_time(value,db_type):
	try:
		parsed_date = datetime.datetime.strptime(value, '%Y-%m-%d')
		return  f"TO_DATE('{parsed_date.strftime('%Y-%m-%d')}','yyyy-mm-dd')"  if  db_type.lower() == "oracle" else "'{}'".format(parsed_date.strftime('%Y-%m-%d'))
	except (ValueError, TypeError):
		pass

	try:
		parsed_time = datetime.datetime.strptime(value, '%H:%M:%S')
		return   f"TO_TIMESTAMP('{parsed_time.strftime('%H:%M:%S')}','HH24:MI:SS')" if  db_type.lower() == "oracle" else "{}".format(parsed_time.strftime('%H:%M:%S'))
	except (ValueError, TypeError):
		pass

	return value


def escape_single_quote(value):
	if isinstance(value, str):
		return value.replace("'", "''")
	return value


def my_transform(df,service={}) :
	df.fillna("", inplace=True)
	map_fields = service.get('FIELD_MAPPER')

	normalized_map_fields = {k.lower(): v for k, v in map_fields.items()}
	normalized_columns = {col.lower(): col for col in df.columns}

	filtered_mapper = {
		normalized_columns[k]: v
		for k, v in normalized_map_fields.items()
		if k in normalized_columns
	}

	renamed_df = df.rename(columns=filtered_mapper)

	renamed_df = renamed_df.applymap(lambda value: escape_single_quote(value))
	renamed_df = renamed_df.applymap(lambda value: format_date_time(value,service.get('ricaDestinationConnector').get('ricaDatabaseType')) )


	return renamed_df[filtered_mapper.values()].to_dict('records') 


table_exist = None



def mount_network_path(directory, username=None, password=None):
	dir_ = "T:"
	if os.path.isdir(directory) and directory.startswith('\\\\'):
		if username and password:
			mount_command = f"net use {dir_} {directory} /user:{username} {password}"
		else:
			mount_command = f"net use {dir_} {directory}"
		# Execute the mount command
		subprocess.run(mount_command, shell=True, check=True)
		print(f"Network path '{directory}' mounted.")
		return dir_
	else:
		print(f"'{directory}' is not a valid network path.")
		return directory


def connect_network_path(network_path, username=None, password=None):
	try:
		if username and password:
			win32wnet.WNetAddConnection2(0, None, network_path, None, username, password)
	except Exception as e:
		print(f"Failed to connect to network path: {e}")
	return network_path



def get_files_in_folder(folder_path):
	files = []
	try:
		for file_name in os.listdir(folder_path):
			file_path = os.path.join(folder_path, file_name)
			if os.path.isfile(file_path):
				files.append(file_path)
	except Exception as e:
		print("files error",e)
	return files


def read_file_contents(file_path,configs_vars):
	map_fields = configs_vars['FIELD_MAPPER']  
	df = None
	file_extension = file_path.split('.')[-1].lower()
	# print('file_extension',file_extension)
	if file_extension in ('xlsx','xls'):
		df = pd.read_excel(file_path, engine='openpyxl')
	elif file_extension in ['csv', 'txt']:
		df = pd.read_csv(file_path)
	elif file_extension in ['json']:
		with open(file_path, 'r') as f:
			df = pd.read_json(f, lines=True)
	
	return my_transform(df,configs_vars)




def match_directory_structure(root_dir, schema):
    pattern = schema.replace('{FOLDER}', '*').replace('{FILE}', '*')
    matching_paths = []
    for path in pathlib.Path(root_dir).rglob('*'):
        if fnmatch.fnmatch(str(path), pattern):
            matching_paths.append( str(pathlib.Path(str(path) ).parent) if os.path.isfile(str(path) ) else str(path)   )
    return matching_paths

# root_dir = 'E:\\dataframe_engine\\sample_data'; schema = 'E:\\dataframe_engine\\{FOLDER}'





def delete_files_by_name(file_path):
	os.remove(file_path)
	print(f"Deleted file: {file_path}")




def copy_file_with_dynamic_root(source_file ):
	base_destination = r"C:\ETL_ARCHIVE"
	normalized_source_file = os.path.normpath(source_file)
	path_parts =  [ x for x in normalized_source_file.split(os.sep) if x]
	relative_path = os.sep.join(path_parts[1:len(path_parts)-1])
	destination_file = os.path.join(base_destination, relative_path)
	os.makedirs(destination_file, exist_ok=True)
	shutil.copy2(normalized_source_file, destination_file)
	print(f"File copied to: {destination_file}")







def process_file(file_path, configs_vars, connector):
	# Simulate reading file content
	data = []
	print('file extension',connector.get("ricaFileType", "").lower(), file_path.lower())
	if connector.get("ricaFileType", "").lower() in file_path.lower():
		data.extend(read_file_contents(file_path, configs_vars))
	return data






def pull_data(connector, configs_vars):
	data = []
	file_paths = []
	source_folder = connector.get('ricaDirectory')
	source_folder = connect_network_path(source_folder, connector.get('ricaUser'), connector.get('ricaPassword'))
	source_folder = match_directory_structure(source_folder, connector.get('ricaDirectorySchema'))
	print('match source_folder', source_folder, connector.get('ricaDirectorySchema'))

	for source_path in source_folder:
		all_files = get_files_in_folder(source_path)
		file_paths.extend([os.path.join(source_path, file_name) for file_name in all_files])

	
	if USE_THREAD:
		with ThreadPoolExecutor(max_workers=4) as executor:
			futures = []
			for i in range(0, len(file_paths), BATCH_SIZE):
				batch = file_paths[i:i + BATCH_SIZE]
				print('Processing batch')
				for file_path in batch:
					futures.append(executor.submit(process_file, file_path, configs_vars, connector))

			for future in as_completed(futures):
				try:
					result = future.result()
					if result:
						data.extend(result)
				except Exception as e:
					print(f"Error processing file: {e}")
	else:
		for i in range(0, len(file_paths), BATCH_SIZE):
			batch = file_paths[i:i + BATCH_SIZE]
			print('Processing batch')
			for file_path in batch:
				data.extend(process_file(file_path, configs_vars, connector))
			time.sleep(1)



	return data, file_paths




def delete_or_copy_files(file_paths, source_connector):
	if USE_THREAD:
		with ThreadPoolExecutor(max_workers=4) as executor:
			futures = []
			
			for path in file_paths:
				if '269' in source_connector.get('ricaArchiveFile'):
					copy_future = executor.submit(copy_file_with_dynamic_root, path)
					futures.append(copy_future)

				if '269' in source_connector.get('ricaDeleteFile'):   
					print("path to be deleted",path)			 
					copy_future.add_done_callback(lambda future: executor.submit(delete_files_by_name, path))
			
			for future in as_completed(futures):
				try:
					future.result()  # Ensures any exceptions are raised
				except Exception as e:
					print(f"Error during file processing: {e}")
	else:
		for i in range(0, len(file_paths), BATCH_SIZE):
			batch = file_paths[i:i + BATCH_SIZE]
			print('Processing batch')
			for path in batch:
				if '269' in source_connector.get('ricaArchiveFile'):
					copy_file_with_dynamic_root( path)

				if '269' in source_connector.get('ricaDeleteFile'):   
					print("path to be deleted",path)			 
					delete_files_by_name(path)
		



def post_data(data, connector,configs_vars,file_paths,source_connector):
	target_db_connector = DatabaseConnector('',"",connector)
	table_name = configs_vars['ricaDestinationTable']
	global table_exist

	if not table_exist:
		table_exist = target_db_connector.get_column_and_primary_key(table_name)

	if table_exist:
		pk, columns_dtypes = table_exist
		has_error = target_db_connector.post(data,table_name,pk,columns_dtypes,None,configs_vars)

		if has_error:
			exit(0) 
		else:
			delete_or_copy_files(file_paths, source_connector)







def run_operation(service):
	print("Running Service: ",service['ricaPayloadId'] )
	data,file_paths = pull_data(service['ricaConnector'],service) 
	print('len_data pulled',len(data))
	post_data(data, service['ricaDestinationConnector'],service,file_paths,service['ricaConnector']) 
	


class PayloadService():

	def __init__(self,payload):

		self.env_settings = get_env_settings()
		self.cursor = create_connection(MAP)
		self.scenario = payload


		self.APP_URL = self.env_settings.get("APP_URL",get_ip())
		self.BACKEND_URL = self.env_settings.get("BACKEND_URL",get_ip())

	
	def fetchAllWIthColumns(self,cursor):
		columns = [MAP.get(col[0], co[l0]) for col in self.cursor.description]
		return [
			dict(zip(columns, row))
			for row in self.cursor.fetchall()
		]



	def update_rica_scenarios(self,scenarioRecord,execute=None):
		copy_data = {}
		try:
			copy_data['ricaRunMode'] = scenarioRecord['ricaRunMode'] or "INTERVAL"  
			copy_data['ricaLastRunDate'] = scenarioRecord['ricaLastRunDate'] or datetime.date.today().strftime("%Y-%m-%d")
			copy_data['ricaLastRunTime'] = scenarioRecord['ricaLastRunTime'] or datetime.datetime.now().strftime("%H:%M:%S")

			# scenario_date = GenRunDate({**scenarioRecord,**copy_data}).run()

			# if scenario_date.get('ricaLastRunDate'): 
			#	 last_run_date = scenario_date.get('ricaLastRunDate')
			#	 last_run_time = scenario_date.get('ricaLastRunTime') 
			# else:
			last_run_date = datetime.date.today().strftime("%Y-%m-%d")
			last_run_time = datetime.datetime.now().strftime("%H:%M:%S")

			update_query =  load_queries.get('UPDATE_QUERY').format(
				ricaLastRunDate=last_run_date,
				ricaLastRunTime=last_run_time,
				ricaNextRunDate=last_run_date,
				ricaNextRunTime=last_run_time,
				ricaPayloadId=scenarioRecord['ricaPayloadId']
				 )


			# print('update_query',update_query,execute)
			try: 
				if execute:
					execute(update_query,update=True, commit=True)
				else:
					self.cursor.execute(update_query)
					self.cursor.commit()
				# self.cursor.close()
			except Exception as e:
				print('write error', e)
			print('PAYLOAD UPDATED')

		except Exception as e:
			print('update _ error',e)

	def update_next_rundate(self,scenarioRecord,execute=None):
		copy_data = {}
		try:
			copy_data['ricaRunMode'] = scenarioRecord['ricaRunMode'] or "INTERVAL"  
			copy_data['ricaLastRunDate'] = datetime.date.today().strftime("%Y-%m-%d")
			copy_data['ricaLastRunTime'] = datetime.datetime.now().strftime("%H:%M:%S")

			scenario_date = GenRunDate({**scenarioRecord,**copy_data}).run()
			print('scenario_date',scenario_date)

			if scenario_date.get('ricaNextRunDate'): 
				next_run_date = scenario_date.get('ricaNextRunDate')
				next_run_time = scenario_date.get('ricaNextRunTime') 

				update_query =  load_queries.get('UPDATE_QUERY_NEXTRUNDATE').format(
					ricaNextRunDate=next_run_date,
					ricaNextRunTime=next_run_time,
					ricaPayloadId=scenarioRecord['ricaPayloadId']
					 )

			print('update_query nextrun date',update_query)
			try: 
				if execute:
					execute(update_query,update=True, commit=True)
				else:
					self.cursor.execute(update_query)
					self.cursor.commit()
				# self.cursor.close()
			except Exception as e:
				print('write error', e)
			print('PAYLOAD NEXT RUN DATE UPDATED')

		except Exception as e:
			print('update next run date_ error',e)




	def execute(self, query, commit=False, close=False, update=None, max_retries=5, retry_delay=5,bind_dicts=None):
		result = []
		retry_count = 0
		while retry_count < max_retries:
			try:
				result = self.cursor.execute(query,bind_dicts) if bind_dicts else self.cursor.execute(query) 
				if commit:
					self.cursor.commit()
				if close:
					self.cursor.close()
				return result
			except Exception as e:
				date_now = str(datetime.date.today()).replace("-","")
				time_now = datetime.datetime.now()
				current_time = str(time_now.strftime("%H:%M:%S")).replace(":","")

				log_args = {
				"ricaLogId":f'Scenario-{self.scenario}-{date_now}-{current_time}',
				'ricaApplication':"Exception",
				'ricaScenario':self.scenario,
				'ricaQuery':str(query).encode("utf-8"),
				'ricaText':f"Exception: {self.scenario} encounted an error: {e}",
				'ricaSendTo':'',
				'branch':'',
				'ricaStatus':"Start",
				'ricaRunDate':date_now,
				'ricaRunTime':current_time,
			   
				}

				logError("Error").log(log_args,self.scenario)
				if "deadlock" in str(e).lower() and retry_count < max_retries - 1:
					print(f"Retrying after {retry_delay} seconds...")
					time.sleep(retry_delay)
					# retry_count += 1
				else:
					break
		return result



	def custom_execute(self,cursor):

		def _cx(query, commit=False, close=False,update=None):
			if update:
				result = cursor.execute(query)
			else:
				result = self.fetchAllWIthColumns(cursor.execute(query))
			if commit:
				print("commiting", type(cursor))
				cursor.commit()
			if close:
				print("closing", type(cursor))
				cursor.close()
			return result
		return _cx






	def run_service(self,scenarioId,journalEntriesData=None,cursor=None,scenarioRecord=None):

		df = []
		f = [] 
		groupFieldList=[]
		all_emails = {
					'to':[],
					'cc':[],
					}
		z=[]
		fields = ["RICATRANSREFID","RICATRANSID",'RICANARRATIVE','RICATRANSTYPE',"RICAFILETESTID","RICAFILETESTricaPayloadId","RICATRANSCODE","RICALCYAMOUNT","RICAENTRYDATE","RICAENTRYTIME"]
		cursor_execute = self.custom_execute(cursor) if cursor else self.execute
		spf = get_spf(cursor_execute, "en",load_queries.get('GET_SPF'))
		still_valid = license_expired(spf,self.env_settings)
		if still_valid:
			expired_notification = get_expired_notification(spf)
			scenarioRecord = scenarioRecord or step_1(cursor_execute, scenarioId)

			if not scenarioRecord:
				raise Exception(f"No payload found for {scenarioId}")
			ricaRunStatus = get_status(cursor_execute,scenarioRecord["ricaRunStatus"]) if scenarioRecord["ricaRunStatus"] else {'ricaModelflag':1}
			
			if scenarioRecord and ricaRunStatus and int(ricaRunStatus["ricaModelflag"]) :
				# print(scenarioRecord)
				v_lastrundate = scenarioRecord["ricaLastRunDate"] #'2021-01-06'
				v_lastruntime = scenarioRecord["ricaLastRunTime"]#'06:01:01' 
				v_nextrundate =  scenarioRecord["ricaNextRunDate"] #'2021-06-01' 
				v_nextruntime = scenarioRecord["ricaNextRunTime"]  #'08:11:01' 
				groupFieldList = []

				print('am here 0',scenarioRecord['ricaDestinationConnector'])
				scenarioRecord['ricaDestinationConnector'] = db_connector.get_credentials(scenarioRecord['ricaDestinationConnector'])
				print('am here 1')
				scenarioRecord['ricaConnector'] = db_connector.get_credentials(scenarioRecord['ricaConnector'])
				print('am here 2')
				scenarioRecord['FIELD_MAPPER'] =  db_connector.extract_field_mappers(scenarioRecord['ricaMapper']) if scenarioRecord.get('ricaMapper') else None
				print('am here 3')
				scenarioRecord["v_nextrundate"]  = str(create_date(v_nextrundate,''))
				scenarioRecord["v_nextruntime"]  = str(create_time(v_nextruntime,'')) 
				scenarioRecord["v_lastrundate"] = str(create_date(v_lastrundate,'') ) 
				scenarioRecord["v_lastruntime"] = str(create_time(v_lastruntime,''))
				print('am here 4')

				run_operation(scenarioRecord)
						
				self.update_next_rundate(scenarioRecord,cursor_execute) 
				if str(STATUS).lower()=='production':
					self.update_rica_scenarios(scenarioRecord,cursor_execute) 
				return (all_emails)
			else:
				raise Exception("Scenario is Deactivated or encounter an error.")



	def gen_str(self,string):
		return str(string) if len(str(string)) > 1 else '0{}'.format(string)


	def get_date(self):
		now = datetime.datetime.now()
		ricaRecordDate = '{}{}{}'.format(
			now.year, self.gen_str(now.month), self.gen_str(now.day))
		ricaRecordTime = '{}:{}:{}'.format(
			self.gen_str(now.hour), self.gen_str(now.minute), self.gen_str(now.second))
		date_time = "{}-{}".format(ricaRecordDate, ricaRecordTime)
		return date_time


	def run_time_reached(self,date_str, time_str):
		try:
			if date_str and time_str:
				current_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

				try:
					date_str = str(date_str).split(" ")[0]
					time_str = str(time_str).split(" ")[1]
				except Exception as e:
					print('efdf',e)

				print('date_str, time_str',date_str, time_str)

				format_1 = '%Y-%m-%d %H:%M:%S'
				format_2 = '%Y-%m-%d %H:%M:%S.%f'

				try:
					given_datetime = datetime.datetime.strptime(str(date_str) + " " + str(time_str), format_1)
				except:
					given_datetime = datetime.datetime.strptime(str(date_str) + " " + str(time_str), format_2)

				
				if str(given_datetime) <= str(current_datetime):
					return True
				else:
					return False
			else:
				return False
		except Exception as e:
			print('runtime reached errs: ',e)
			return True






	def runService(self,scenario = ""):

		group_field = ""

		try:
			print('\n\n')
			print("=====================================")
			print(f"   Running Payload  -  {scenario}	")
			print("=====================================")

			date_now = str(datetime.date.today()).replace("-","")
			time_now = datetime.datetime.now()
			current_time = str(time_now.strftime("%H:%M:%S")).replace(":","")

			log_args = {
			"ricaLogId":f'Scenario-{scenario}-{date_now}-{current_time}',
			'ricaApplication':"Exception",
			'ricaScenario':scenario,
			'ricaQuery':"",
			'ricaText':f"Exception: {scenario} run started on {date_now}",
			'ricaSendTo':'',
			'branch':'',
			'ricaStatus':"Start",
			'ricaRunDate':date_now,
			'ricaRunTime':current_time,
		   
			}

			logError("Start").log(log_args,scenario)
			
			cursor_execute =  self.execute
			scenarioRecord = step_1(cursor_execute, scenario) 

			v_lastrundate = scenarioRecord["ricaLastRunDate"] #'2021-01-06'
			v_lastruntime = scenarioRecord["ricaLastRunTime"]#'06:01:01' 
			v_nextrundate =  scenarioRecord["ricaNextRunDate"] #'2021-06-01' 
			v_nextruntime = scenarioRecord["ricaNextRunTime"]  #'08:11:01' 

			default_variables  ={
			'v_lastruntime':str(create_time(v_lastruntime,':')),
			'v_lastrundate':str(create_date(v_lastrundate,'-')),
			'v_nextrundate':str(create_date(v_nextrundate,':')),
			'v_nextruntime':str(create_time(v_nextruntime,'-')),	
			}
			# print(scenarioRecord)

			if self.run_time_reached(scenarioRecord['ricaNextRunDate'], scenarioRecord['ricaNextRunTime']):
				emails = self.run_service(scenario,scenarioRecord=scenarioRecord) 
			else:
				print(f'CURRENT DATETIME: {datetime.datetime.now()} , NEXT RUN DATE not reached until: ', str(create_date(v_nextrundate,'-')), str(create_time(v_nextruntime,':')))
		except Exception as e:
			print('RunTime Error found: ',e)
			date_now = str(datetime.date.today()).replace("-","")
			time_now = datetime.datetime.now()
			current_time = str(time_now.strftime("%H:%M:%S")).replace("-","")

			log_args = {
			"ricaLogId":f'Scenario-{scenario}-{date_now}-{current_time}',
			'ricaApplication':"Exception",
			'ricaQuery':replaceParams(scenarioRecord.get("ricaQueryPanel"),default_variables),
			'ricaScenario':scenario,
			'ricaText':f"Exception: {scenario} Error: {e}",
			 'ricaSendTo':', '.join([]),
			group_field:', '.join([]),
			'ricaStatus':"Error",
			'ricaRunDate':date_now,
			'ricaRunTime':current_time,
		   
				}
			logError("Error").log(log_args,scenario) 




if __name__=='__main__':
	parser = argparse.ArgumentParser(description ='Run Exception Services')  
	parser.add_argument('-r', dest ='payload',
						action ='store', help ='run payload (seconds)',default=None)  

	args = parser.parse_args() 
	payload =  args.payload  #if args.payload else list(reversed(__file__.split("\\")))[1] 
	# print(get_env_settings())
	if payload:
		 PayloadService(payload).runAlert(scenario= payload)
	else:
		raise Exception("No payload provided")




