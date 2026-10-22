import os
import smtplib
from email import encoders, utils
from email.header import Header
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_email(email_config, attachment_path=None):
    """Sends the file at attachment_path as an attachment in an email
    following the email_config.

    email_config must contain:
        - email_server: SMTP server address
        - sender: sender email address
        - recipients: list of recipient email addresses
        - subject: email subject
        - body: email body (string)
        - maintype: attachment maintype (e.g., 'text')
        - subtype: attachment subtype (e.g., 'html')
    """
    server = email_config['email_server']
    sender = email_config['sender']
    recipients = email_config['recipients']
    
    subject = email_config['subject']
    body = email_config['body']
    maintype = email_config['maintype']
    subtype = email_config['subtype']

    # Create the email message
    msg = MIMEMultipart()
    msg.set_charset('utf-8')
    msg['Subject'] = Header(subject)
    msg['From'] = Header(sender)
    msg['To'] = Header(", ".join(recipients))
    msg['Date'] = Header(utils.formatdate())
    msg['Message-ID'] = Header(utils.make_msgid())

    # Attach the message body (plain or html)
    msg.attach(MIMEText(body, "html", "utf-8"))

    # Attach the file
    if attachment_path:
        with open(attachment_path, 'rb') as f:
            file_data = f.read()

        part = MIMEBase(maintype, subtype)
        part.set_payload(file_data)
        encoders.encode_base64(part)
        part.add_header(
            'Content-Disposition',
            f'attachment; filename="{os.path.basename(attachment_path)}"'
        )
        msg.attach(part)

    # Send the message
    try:
        with smtplib.SMTP(server) as smtp:
            smtp.sendmail(sender, recipients, msg.as_string())
    except Exception as e:
        print("ERROR: Fail to send the report by email due to unexpected error")
        print(f"Error: {str(e)}")