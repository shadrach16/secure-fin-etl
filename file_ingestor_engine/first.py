

from FILETEST.func import get_first
from FILETEST.common import load_queries



columns = ["1 Day", "2-7 Days", "8-14 Days", "15-28 Days", "29 Days - 3 months",
		   "3 months - 6 months", "6 months - 1 year", "1 year - 3 years", "3 years - 5 years", "Over 5 years"]


def get_status(execute, status_id=""):	
	query = f""" select ricaModelflag from rica_status where ricaStatusId='{status_id}' """
	return get_first(execute(query))


def step_1(execute, scenario=""):
	query =  load_queries.get('GET_SCENARIO').format(
				scenario=scenario
				 )
	# print(query)
	return get_first(execute(query))


def replaceMsgParams(msg="",obj={}):
	for x in obj:
		val = "${"+x+"}"
		msg = msg.replace(val,str(obj.get(x)) )
	return msg


