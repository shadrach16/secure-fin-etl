

from datetime import datetime as dt
import os
import sys
from email.mime.image import MIMEImage
# import django
# django.conf.settings.configure()

# from django.core.mail import EmailMultiAlternatives,get_connection
from email.mime.base import MIMEBase
from email import encoders
import premailer
import logging 
import datetime
import threading
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from FILETEST.func import logError
from email.utils import make_msgid 
from FILETEST.func import get_sqlite_con_dir
from jinja2 import Environment, FileSystemLoader


from email.message import EmailMessage

path = os.getcwd()
sys.path.append(path)

now = dt.now()
date = now.strftime('%d-%b-%y %H:%M:%S %p').upper()

BASE_DIR = Path(__file__).resolve().parent.parent


template_path = os.path.join(get_sqlite_con_dir(),"bots", "PAYLOAD_FILE_MIGRATIONS", "FILETEST","templates")
images_path = os.path.join(get_sqlite_con_dir(),"bots", "PAYLOAD_FILE_MIGRATIONS","FILETEST","images")
date = now.strftime('%d-%b-%y %H:%M:%S %p').upper()


def send_response_mail(options={}, spf={} ):

    html_out = gen_template("error.html", {**options })
    subject = f"""{spf.get("ricaAppsId")} - [URGENT] Error in Payload: FILETEST"""
    send_html_mail(subject, html_out, options['send_to'], spf=spf)


def gen_template(name, template_vars={}):
    print('TEMPLATE PATH',template_path)
    env = Environment(loader=FileSystemLoader(template_path, encoding='utf8'))
    template = env.get_template(name)
    html_out = template.render(template_vars)
    return html_out





def attach_image(msg,f='',name=''):
    locn = os.path.abspath(os.path.dirname(__file__))
    file_location = os.path.join(BASE_DIR, f)
    print('IMAGES',file_location)
    fp = open(file_location, 'rb')
    msg_img = MIMEImage(fp.read())
    fp.close()
    msg_img.add_header('Content-ID', '<{}>'.format(name if name else f))
    msg.attach(msg_img)

def attach_excel_file(msg,f='',name=''):
    print('excel name',f)
    locn = f
    fp = open(locn[0], 'rb')
    part = MIMEBase('application', "octet-stream")
    part.set_payload(fp.read())
    fp.close()
    encoders.encode_base64(part)    
    # locn = os.path.abspath(os.path.dirname(__file__))
    # file_location = os.path.join(locn,'excel')  
    name = locn[1] #.strip(file_location)
    part.add_header('Content-Disposition', 'attachment',filename=f'{name}')
    msg.attach(part)

def clean_emails(emails=[]):
    if emails:
        return [ x for x in list(set(emails)) if x]
    else:
        return []





def custom_send(subject, html_content, recipient_list=[],  spf={}):

    date_now = str(datetime.date.today()).replace("-","")
    time_now = datetime.datetime.now()
    current_time = str(time_now.strftime("%H:%M:%S")).replace(":","")

    log_args = {
    "ricaLogId":f'Mail-{date_now}-{current_time}',
    'ricaApplication':"Exception Mailing", 
    'ricaText':f"", 
    'ricaStatus':"",
    'ricaRunDate':date_now,
    'ricaRunTime':current_time,
   
    }



    from_email = spf.get("ricaStmpMailAddress") or spf.get("ricaStmpMailUser") or  spf.get("ricaAlertResponseFrom")  
    host=spf.get("ricaStmpMailServer") 
    port= 587 if 'ionos' in spf.get("ricaStmpMailServer") else  spf.get("ricaStmpMailPort")
    username=spf.get("ricaStmpMailUser")  
    password=spf.get("ricaStmpMailPassword")  
    # recipient_list = ['IbukunAkinteye@keystonebankng.com']

    to = []
    cc = []

    if isinstance(recipient_list,dict):
        to =   clean_emails(recipient_list.get('to',[])) 
        cc =   clean_emails(recipient_list.get('cc',[])) 

    else:
        to =  clean_emails(recipient_list)

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject # 'Re: TEST1: TESTING QUERY THAT GROUP WITH INPUTTER, AUTHORISER ETC. ALERT ID: TEST1-005-20230910-14212355'
    msg['From'] = from_email
    msg['To'] = ','.join(to)
    msg['Cc'] = ','.join(cc) 
    msg['Reply-To'] = ','.join(to+cc)  

    print('from_email',from_email)
    print('port',port)
    log_args['ricaSendTo'] = f"from:{from_email}, to:{','.join(to)}, cc:{','.join(cc)}"

    html  = html_content
    part2 = MIMEText(html, 'html')
    msg.attach(part2)

    
    try:
        print('send mail to', to,cc)
        service = smtplib.SMTP(host, int(port))
        service.connect(host, int(port))
        service.ehlo()

        if str(port)=='587' or str(port)=='465' :
            service.starttls()
            service.login(username, password)

        service.sendmail(from_email,to+cc, msg.as_string())
        service.quit()
        log_args['ricaStatus'] = "Success"
        log_args['ricaText'] = "Mail Sent Successfully"
        logError("OutMail").log(log_args,'FILETEST')
        print('mail sent')

    except Exception as e:
        log_args['ricaStatus'] = "Success"
        log_args['ricaText'] = str(e)
        logError("OutMail").log(log_args,'FILETEST')
        print('mail error', e)

def send_html_mail( subject, html_content, recipient_list,spf): 
    custom_send(subject, html_content, recipient_list, spf )