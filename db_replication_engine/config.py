# Exception Configurations
# Exception ID to run
PAYLOAD_ID='*'
# Seconds in which the exception we'll be reschedule
SCHEDULE_TIME=120 #seconds
# Seconds in which new payloads will be check
PAYLOAD_CHECK_TIME=60*3 # seconds
# Short Location in which the Logs for this exception will be located eg. Desktop, Documents etc
LOG_PATH='Desktop' #Optional: defaulted to Desktop
# Full Path in which the Logs for this exception will be located eg. C:\Users\Adroit\Desktop
FULL_LOG_PATH=r"C:\\" #Optional : defaulted to LOG_PATH 
# The  Directory in which Sqlite File (Optional) is located eg. E:\rica\backend
SQLITE_DIRS=[r'E:\dataframe_engine\backend',r"C:\inetpub\wwwroot\dfa\backend",r"C:\dataframe_engine\backend",r"E:\dataframe_engine\backend",r'C:\inetpub\wwwroot\radarpro\backend',r'E:\radarpro\ic4probackend'] #Optional: defaulted to base_dir where backend is located
# Development Status
STATUS=r"production" # production/development (This will not update last_run_date if in development)
# thread pool executor 
THREAD_POOL=51 #  creates a thread pool executor with a maximum of 51 threads
# process pool executor
PROCESS_POOL=5 # creates a process pool executor with a maximum of 5 processes.
# allowed concurrent instances
MAX_INSTANCES=3 # specify the maximum number of allowed concurrent instances of a job.
date_formatter='%Y-%m-%d'
time_formatter='%H:%M:%S'
DTTYPES=['DATE','TIMESTAMP','NUMBER' ]
language='en'

MAP = {
"RICAPAYLOADID":"ricaPayloadId",
"RICAPAYLOAD":"ricaPayload",
"RICARUNONCE":"ricaRunOnce",
"RICATYPEID":"ricaTypeId",
"RICASUBTYPEID":"ricaSubTypeId",
"RICAREQUESTOR":"ricaRequestor",
"RICAPURPOSE":"ricaPurpose",
"RICAINTERVALOF":"ricaIntervalOf",
"RICANEXTRUNDATE":"ricaNextRunDate",
"RICANEXTRUNTIME":"ricaNextRunTime",
"RICALASTRUNDATE":"ricaLastRunDate",
"RICALASTRUNTIME":"ricaLastRunTime",
"RICAACTION":"ricaAction",
"RICAIMPLICATIONS":"ricaImplications",
"RICAGROUPBY":"ricaGroupBy",
 
"RICADAILY":"ricaDaily",

"RICARUNMODE":"ricaRunMode",
"RICAWEEKLY":"ricaWeekly",
"RICAMONTHLY":"ricaMonthly",
"RICAQUARTERLY":"ricaQuarterly",
"RICAYEARLY":"ricaYearly",

"RICANEWRECORDSTATUS":"ricaNewRecordStatus",


    "RICAALERTID": "ricaAlertId", 
    "RICACASEID": "ricaCaseId", 
    "RICANETLOSSAMOUNT": "ricaNetLossAmount", 
    "RICADISPOSITION": "ricaDisposition",
    "RICARISKASSESSMENT": "ricaRiskAssessment",
    "RICACREATEDATE": "ricaCreateDate",
    "RICACREATETIME": "ricaCreateTime",
    "RICASCENARIO": "ricaScenario",
    "RICASCENARIOID": "ricaScenarioId",
    "RICAFLOWTYPE": "ricaFlowType",
    "RICASTMPMAILSERVER": "ricaStmpMailServer",
    "RICASTMPMAILPORT": "ricaStmpMailPort",
    "RICASTMPMAILUSER": "ricaStmpMailUser",
    "RICASTMPMAILPASSWORD": "ricaStmpMailPassword",
    "SPECIAL_DATE": "special_date",
    "SPECIAL_TIME": "special_time",
    "RICASPECIALDATE": "special_date",
    "RICASPECIALTIME": "special_time",
   
    "RICAFOOTERLABEL": "ricaFooterLabel",
    "RICALICENSENOTIFYDUR": "ricaLicenseNotifyDur",
    "RICAEXPIRYDATE": "ricaExpiryDate",
    "RICACLIENTNAME": "ricaClientName",
    "RICABOTNOTIFIER": "ricaBotNotifier",
    "RICABOTNOTIFIERMSG": "ricaBotNotifierMsg",
    "RICAMESSAGE": "ricaMessage",

    "RICADESTINATIONCONNECTOR": "ricaDestinationConnector",
    "RICACONNECTOR": "ricaConnector",
    "RICADESTINATIONTABLE": "ricaDestinationTable",
    "RICAMAPPER": "ricaMapper",
    "RICAQUERYPANEL": "ricaQueryPanel",



    "RICACONNECTORNAME":"ricaConnectorName",
"RICADESCRIPTION":"ricaDescription",
"RICADATABASENAME":"ricaDatabaseName",
"RICADIRECTORY":"ricaDirectory",
"RICADIRECTORYSCHEMA":"ricaDirectorySchema",
"RICADATABASEHOST":"ricaDatabaseHost",
"RICADATABASEPORT":"ricaDatabasePort",
"RICAUSER":"ricaUser",
"RICAPASSWORD":"ricaPassword",
"RICASERVICE":"ricaService",
"RICACONNECTORTYPE":"ricaConnectorType",
"RICADATABASETYPE":"ricaDatabaseType",
"RICALICENSECODE":"ricaLicenseCode",
"RICARUNSTATUS":"ricaRunStatus",
"RICAMODELFLAG":"ricaModelflag",


"RICAUSEREMAIL":"ricaUserEmail",
"RICAUSERID":"ricaUserId",
"RICASUBJECT":"ricaSubject",
"RICAEMAILRECEIVER":"ricaEmailReceiver",
"RICAAUTHORIZERMSG":"ricaAuthorizerMsg",
"RICAINPUTTERMSG":"ricaInputterMsg",
"RICAMESSAGE":"ricaMessage",
"RICAOWNERFLAG":"ricaOwnerFlag",
"COLUMN_NAME":"column_name",


}