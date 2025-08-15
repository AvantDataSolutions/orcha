import base64
from dataclasses import dataclass
from importlib.resources import as_file, files

from orcha.utils import graph_api


def send_email(
        token: str,
        send_as: str,
        to: list[str],
        subject: str,
        header: str,
        body: str,
        cc: list[str] = [],
        bcc: list[str] = [],
        importance: str = 'normal',
        attachments: list = [],
    ):
    """
    Send an email using the Graph API.
    #### Parameters
    - token: The token to use for authentication with the Graph API. The token
    must have the Mail.Send permission.
    - send_as: The email address to send the email as.
    - to: The email address to send the email to.
    - subject: The subject of the email.
    - header: The header of the email is populated into the email template.
    - body: The body of the email as plaintext or html.
    - cc: The email address to cc the email to.
    - bcc: The email address to bcc the email to.
    - importance: The importance of the email.
    - attachments: A list of attachments to attach to the email.
    """
    endpoint = f'https://graph.microsoft.com/v1.0/users/{send_as}/sendMail'

    email_html = _base_template.populate(
        header=header,
        title=subject,
        content=body,
        footer='<a href="https://github.com/AvantDataSolutions/orcha">Orcha ETL</a>'
    )

    data = {
        'message': {
            'subject': subject,
            'body': {
                'contentType': 'HTML',
                'content': email_html
            },
            'toRecipients': [{'emailAddress': {'address': e}} for e in to],
            'ccRecipients': [{'emailAddress': {'address': e}} for e in cc] if cc else [],
            'bccRecipients': [{'emailAddress': {'address': e}} for e in bcc] if bcc else [],
            'importance': importance,
            'attachments': attachments if attachments else []
        }
    }
    # Sending an email is a secondary function so we don't want to raise
    # an exception if it fails which kills the thread that called this.
    try:
        return graph_api.do_post(endpoint, token, data)
    except Exception as e:
        print('---------- ERROR SENDING EMAIL -----------')
        print(e)
        print('------------------------------------------')


class EmailSendResult():
    """
    Not currently used.
    """
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    RATE_LIMITED = 'RATE_LIMITED'
    NO_CREDS = 'NO_CREDS'


@dataclass
class EmailTemplate():
    """
    The class used to create an email template from a set of parameters.
    """
    name: str
    template: str

    def populate(
            self,
            header: str,
            title: str,
            content: str,
            footer: str
        ):
        """
        Populate the email template with the given parameters.
        """
        full = self.template.replace('{{header}}', header)
        full = full.replace('{{title}}', title)
        full = full.replace('{{content}}', content)
        full = full.replace('{{footer}}', footer)
        return full


logo_text = files('orcha').joinpath('assets/images/orcha-font-black.png')
with as_file(logo_text) as path:
    logo_text = str(path)

logo_round = files('orcha').joinpath('assets/images/orcha-logo-round.png')
with as_file(logo_round) as path:
    logo_round = str(path)

# load src as base64 from assets/images/orcha-logo.png
_orcha_logo_src = "data:image/png;base64," + base64.b64encode(open(logo_round, "rb").read()).decode()
_orcha_logo_text_src = "data:image/png;base64," + base64.b64encode(open(logo_text, "rb").read()).decode()

# Define style variables
container_style = "font-family: system-ui,-apple-system,'Segoe UI',Roboto,'Helvetica Neue',Arial,'Noto Sans','Liberation Sans',sans-serif,'Apple Color Emoji','Segoe UI Emoji','Segoe UI Symbol','Noto Color Emoji'; display: flex; flex-direction: column; margin-right: auto; margin-left: auto; max-width: 80em;"
row_style = "display: flex; flex-wrap: wrap; justify-content: center; align-items: center;"
top_spacer_style = "height: 80px; background-color: rgb(250, 250, 250); border-bottom: 5px solid #007bff; justify-content: center; width: 100%;"
header_style = "justify-content: center; background-color: rgb(230,230,250); padding-top: 1rem; padding-bottom: 1rem; font-weight: 400; font-size: 1.5rem; padding: 0.5rem; padding-bottom: 0;"
general_style = "opacity: 0.5; font-weight: 400; font-size: 1rem; padding: 0.5rem; padding-bottom: 0;"
title_style = "font-weight: 400; font-size: 1.5rem; padding: 0.5rem; padding-top: 0;"
content_outer_style = "background-color: rgb(230,230,250);"
content_inner_style = "background-color: white; margin-left: 1rem; margin-right: 1rem; padding: 1rem; width: 100%;"
content_footer_style = "display: flex; justify-content: space-between; background-color: rgb(230,230,250); padding-top: 0.5rem; padding-bottom: 0.5rem; width: 100%;"
col_style = "display: flex; padding-right: 15px; padding-left: 15px;"
orcha_logo_style = "padding-right: 0.1rem; height: 4rem; opacity: 1.5;"
orcha_logo_text_style = "padding-left: 0.1rem; height: 2.5rem; opacity: 1.5;"

# Replace class styles with inline styles in the template
_base_template = EmailTemplate(
    name='base_email',
    template='''
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
        </head>
        <body>
            <div style="{container_style}">
                <div style="{row_style} {top_spacer_style}">
                    <img style="{orcha_logo_style}"
                        src="{logo_src}">
                    <img style="{orcha_logo_text_style}"
                        src="{logo_text_src}">
                </div>
                <div style="{row_style} {header_style}">
                    <div style="{general_style}">
                        {{header}}
                    </div>
                </div>
                <div style="{row_style} {header_style}">
                    <div style="{title_style}">
                        {{title}}
                    </div>
                </div>
                <div style="{row_style} {content_outer_style}">
                    <div style="{content_inner_style}">
                        {{content}}
                    </div>
                </div>
                <div style="{row_style} {content_footer_style}">
                    <div style="{col_style}">
                        {{footer}}
                    </div>
                    <div style="{col_style}">
                        Avant Data Solutions
                    </div>
                </div>
            </div>
        </body>
        </html>
    '''.replace('{logo_src}', _orcha_logo_src).replace('{logo_text_src}', _orcha_logo_text_src)
    .replace("{container_style}", container_style)
    .replace("{row_style}", row_style)
    .replace("{top_spacer_style}", top_spacer_style)
    .replace("{header_style}", header_style)
    .replace("{general_style}", general_style)
    .replace("{title_style}", title_style)
    .replace("{content_outer_style}", content_outer_style)
    .replace("{content_inner_style}", content_inner_style)
    .replace("{content_footer_style}", content_footer_style)
    .replace("{col_style}", col_style)
    .replace("{orcha_logo_style}", orcha_logo_style)
    .replace("{orcha_logo_text_style}", orcha_logo_text_style)
)