from FILETEST.common import common_query
from FILETEST.func import get_env_settings



class Ad(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self

# and now I make this function:
def module_to_dict(module):
    return Ad({val: getattr(module, val) for val in dir(module) if '__' not in val}) 

def get(query_name):
	env_settings = get_env_settings()
	db_type = str(env_settings.get('DATABASE_TYPE'))

	if db_type.lower() == "mssql":
		from FILETEST.common import mssql
		try:
			query = module_to_dict(mssql)[query_name]  
		except Exception as e:
			query = module_to_dict(common_query)[query_name] 
		return query


	elif db_type.lower() == "mysql":
		from FILETEST.common import mysql
		try:
			query = module_to_dict(mysql)[query_name]  
		except Exception as e:
			query = module_to_dict(common_query)[query_name] 
		return query

	else:
		from FILETEST.common import oracle 
		try:
			query = module_to_dict(oracle)[query_name] 
		except Exception as e:
			query = module_to_dict(common_query)[query_name] 
		return query

