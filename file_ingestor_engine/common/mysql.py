
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


