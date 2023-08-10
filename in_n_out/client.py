import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart

# from email.mime.text import MIMEText
from email.utils import make_msgid

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


class GoogleDriveClient:
    def __init__(self):
        pass

    def initialise_client(self):
        gauth = GoogleAuth()
        gauth.LocalWebserverAuth()
        self.drive = GoogleDrive(gauth)

    def query(self):
        pass


class GoogleMailClient:
    def __init__(self, email_params):
        self.sender_email = email_params["sender_email"]
        self.recipient_email = email_params["recipient_email"]
        self.password = email_params["password"]
        self.in_reply_to = email_params.get("message_id")
        self.subject = email_params.get("subject")
        self.content = email_params.get("content")

        self.message_id = make_msgid()

        self.build_message

    @property
    def build_message(self):
        message = MIMEMultipart()
        message["From"] = self.sender_email
        message["To"] = " ".join(self.recipient_email)
        message["Subject"] = self.subject
        message["Message-ID"] = self.message_id
        message["In-Reply-To"] = self.in_reply_to
        message["References"] = self.in_reply_to
        self.message = message

    def add_attachment(self, content, filename):
        # Open PDF file in binary mode
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(content)

        # Encode file in ASCII characters to send by email
        encoders.encode_base64(part)

        # Add header as key/value pair to attachment part
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {filename}",
        )

        # Add attachment to message and convert message to string
        self.message.attach(part)

    def send_email(self):
        session = smtplib.SMTP("smtp.gmail.com", 587)  # use gmail with port
        session.starttls()  # enable security
        session.login(
            self.sender_email, self.password
        )  # login with mail_id and password
        text = self.message.as_string()
        session.sendmail(self.sender_email, self.recipient_email, text)
        session.quit()
