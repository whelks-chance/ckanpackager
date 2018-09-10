import os
import smtplib
import logging
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE, formatdate

import requests

from email.mime.text import MIMEText

from ckanpackager.utils.QueueWriter import QueueWriter
import local_settings


class Things:
    def __init__(self):
        self.config = {}
        self.request_params = {}
        self.log = logging.getLogger(__name__)

    def host(self):
        pass

    def post_stuff(self):
        resp = requests.post('http://127.0.0.1:8765/statistics', data={'secret': '8ba6d280d4ce9a416e9b604f3f0ebb'})
        # print(resp.content)
        with open('ckanpackager_service.html', 'w') as f1:
            f1.write(resp.content)

        resp = requests.post('http://127.0.0.1:8765/status', data={'secret': '8ba6d280d4ce9a416e9b604f3f0ebb'})
        # print(resp.content)
        with open('ckanpackager_status.html', 'w') as f2:
            f2.write(resp.content)

    def email_things_via_o365(self):
        import smtplib

        mailserver = smtplib.SMTP('smtp.office365.com', 587)
        mailserver.ehlo()
        mailserver.starttls()
        mailserver.login('username@domain.com', 'passwords')
        mailserver.sendmail('username@domain.com', 'username@domain.com', 'python email')
        mailserver.quit()

    def build_mail(self, send_from, send_to, subject, message, html, files=[]):
        """Compose and send email with provided info and attachments.

        Args:
            send_from (str): from name
            send_to (list[str]): to name
            subject (str): message title
            message (str): message body
            html (str): html message body
            files (list[str]): list of file paths to be attached to email
            server (str): mail server host name
            port (int): port number
            username (str): server auth username
            password (str): server auth password
            use_tls (bool): use TLS mode
        """
        msg = MIMEMultipart()
        msg['From'] = send_from
        msg['To'] = COMMASPACE.join(send_to)
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = subject

        msg.attach(MIMEText(message, 'plain'))
        msg.attach(MIMEText(html, 'html'))

        for path in files:
            part = MIMEBase('application', "octet-stream")
            with open(path, 'rb') as file:
                part.set_payload(file.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition',
                            'attachment; filename="{}"'.format(os.path.basename(path)))
            msg.attach(part)

        return msg

    def email_from_localhost(self, files=None):
        if not files:
            files = []
        server = "127.0.0.1"
        port = 25
        username = ''
        password = ''
        use_tls = True

        # place_holders = {
        #     'resource_id': self.request_params['resource_id'],
        #     'zip_file_name': os.path.basename(zip_file_name),
        #     'ckan_host': self.host()
        # }

        send_from = 'ArchiveBot'
        send_to = local_settings.send_to
        html_message = '''
            <html>
              <head></head>
              <body>
                <p>Thank you for using this service,<br>
                
                   Your FileZilla queue file is attached.<br><br>
                   
                   Using the "Import..." option in the File menu, your selected files will be added to your download queue.<br><br>
                   
                   Use the username and password in the attached file to log in.<br><br>
                   
                   These credentials will expire in 30 days.<br><br>
                   
                   Many thanks,<br><br>
                   
                   Archive Bot, <br><br>
                                      
                   Here is the <a href="https://www.example.com">link</a> you didn't ask for.
                </p>
              </body>
            </html>
        '''

        msg = self.build_mail(
            send_from=send_from,
            send_to=send_to,
            subject='filezilla queue file',
            message='this is the email text, we should add html to this later',
            html=html_message,
            files=files
        )

        smtp = smtplib.SMTP(server, port)
        # if use_tls:
        #     smtp.starttls()
        # smtp.login(username, password)
        res = smtp.sendmail(send_from, send_to, msg.as_string())

        print res

        smtp.quit()


if __name__ == '__main__':
    t = Things()
    # t.post_stuff()

    qw = QueueWriter()
    for f in local_settings.files:
        qw.add_file(f)

    filename = 'FileZilla2.xml'
    qw.write_queue_xml(filename=filename)
    files = [filename]

    try:
        t.email_from_localhost(files=files)
        print 'sent'
    except Exception as e1:
        print(e1)
