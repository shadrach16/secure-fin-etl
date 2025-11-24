


from ACCOUNT.db_client import create_connection
from ACCOUNT.func import get_sqlite_con_dir
MAP = {
	'RICAPAYLOADID':'ricaPayloadId',
	'RICAPAYLOAD':'ricaPayload',
	'ID':'ricaPayloadId'
}
cursor = None

while True:
	try:
		print("CONNECTING TO DATABASE...")
		cursor = create_connection(MAP)
		print("CONNECTED...")	
		break
	except Exception as e:
		print("CONNECTION FAILED: ",e)



from ACCOUNT.config import SCHEDULE_TIME,PAYLOAD_CHECK_TIME,THREAD_POOL, PROCESS_POOL,MAX_INSTANCES,PAYLOAD_ID
from ACCOUNT.run import PayloadService
# from ACCOUNT.scheduler import BotScheduler

from pytz import utc
from datetime import datetime
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor

sqlite_con_dir = get_sqlite_con_dir() 



import time
import threading
import random
# import asyncio
# cwd = os.getcwd() 

scheduled_jobs_map = set()


def retrieve_jobs_to_schedule(payload_id=""):
	print("payloads id",payload_id)
	loc_payloads = f"""select distinct ricaPayloadId,ricaPayload from rica_payload_builders
		where ricaPayloadId = '{payload_id}' AND
		 ricaRunStatus LIKE '%-208'
			 """
	
	payloads = cursor.execute(loc_payloads)
	print("payloads",payloads)
	return payloads[0] if len(payloads) else None


def main():
	while True:
		try:
			job = retrieve_jobs_to_schedule("ACCOUNT")
			if job:
				print("executing job with ricaPayloadId: " + str(job['ricaPayloadId']))
				while True:
					Als = PayloadService(str(job['ricaPayloadId']))
					Als.runService(str(job['ricaPayloadId']))
					time.sleep(10)
		except Exception as e:
			# Handle exceptions from retrieving jobs
			print('execute_jobs error: ', e)

		time.sleep(PAYLOAD_CHECK_TIME)


# def test_payload(payload):
# 	PayloadService(payload).runAlert(payload)

 
if __name__ == '__main__':
	main()
	# test_payload('test1')
