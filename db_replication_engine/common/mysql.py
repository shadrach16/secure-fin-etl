UPDATE_QUERY = """ 
                     UPDATE rica_payload_builders
                SET ricaLastRunDate = CONVERT(date, '{ricaLastRunDate}', 23),  
                ricaLastRunTime = CONVERT(time, '{ricaLastRunTime}', 114),
                     ricaRunOnce = null
                WHERE ricaPayloadId = '{ricaPayloadId}'
     """

UPDATE_QUERY_NEXTRUNDATE = """ 
                     UPDATE rica_payload_builders
                SET ricaNextRunDate = CONVERT(date, '{ricaNextRunDate}', 23),  
                ricaNextRunTime = CONVERT(time, '{ricaNextRunTime}', 114)
                WHERE ricaPayloadId = '{ricaPayloadId}'
     """
     
USERS1= """
       SELECT  ricaUserId,
       ricaUserEmail,
        ricaUserTitle,
        ricaFirstName,
        ricaMiddleName,
        ricaLastName,
        ricaUserPwd,
        ricaUserRole,
        ricaStaffNo,
        ricaUserStatus,
        ricaCountry,
        ricaCompany,
        ricaMainLanguage,
        ricaStaffGrade,
     date_format(ricaDateJoined,'%Y-%m-%d') AS ricaDateJoined,
                   date_format(ricaRecordDate,'%Y-%m-%d') AS ricaRecordDate,
       ricaOperation,
        ricaOperator,
        ricaLanguage,
        ricaWorkstation,
        ricaRecordTime,
        ricaUserLocation,
        ricaRecordCounter
         from rica_user
     """



GET_SPF=""" select ricaClientName,ricaStmpMailServer,ricaStmpMailPort,ricaStmpMailUser,ricaStmpMailPassword,
     ricaDefaultOwner,ricaDefaultRespondent,ricaDefaultInvestigator,ricaLicenseCode,ricaLicenseNotifyDur,
     date_format(ricaExpiryDate,'%Y-%m-%d') AS ricaExpiryDate 
     from rica_spf 
     where ricaSpfId='{lang}-SYSTEM' """


