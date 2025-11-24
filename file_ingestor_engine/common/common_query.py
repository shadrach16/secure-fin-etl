
GET_SCENARIO="""
        select * from rica_payload_builders
        Where ricaPayloadId = '{scenario}'
        """

get_creds_query=""" 
SELECT  * from rica_connector 
WHERE ricaConnectorName = '{CONNECTOR}' 


 """



GET_USER=""" 
select ricaUserEmail,ricaUserId  from rica_user
WHERE ricaUserId = '{user_id}' 
 """



GET_USER_REQUEST=""" 
select ricaSubject,ricaEmailReceiver,ricaAuthorizerMsg,ricaInputterMsg,ricaOwnerFlag   from rica_userrequest
WHERE ricarequest = 'PAYLOAD' 
 """

GET_USER_INFO_VIA_DESIG=""" 
select ricaUserEmail from rica_user 
where ricaUserRole = '{designate}'
 """
GET_MESSAGES=""" 
select * from rica_messsages
where ricaMsgId='{id}'
 """

GET_STATUS=""" 
 select ricaModelflag from rica_status where ricaStatusId='{id}'
 """