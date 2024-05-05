from dataclasses import dataclass

from orcha.utils import graph_api


def send_email(
        token: str,
        send_as: str,
        to: str,
        subject: str,
        header: str,
        body: str,
        cc: str = '',
        bcc: str = '',
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
            'toRecipients': [{'emailAddress': {'address': to}}],
            'ccRecipients': [{'emailAddress': {'address': cc}}] if cc else [],
            'bccRecipients': [{'emailAddress': {'address': bcc}}] if bcc else [],
            'importance': importance,
            'attachments': attachments if attachments else []
        }
    }
    return graph_api.do_post(endpoint, token, data)


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
        full = self.template.replace('{{header}}', header)
        full = full.replace('{{title}}', title)
        full = full.replace('{{content}}', content)
        full = full.replace('{{footer}}', footer)
        return full


_base_template = EmailTemplate(
    name='base_email',
    template='''
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                .container {
                    font-family: system-ui,-apple-system,"Segoe UI",Roboto,"Helvetica Neue",Arial,"Noto Sans","Liberation Sans",sans-serif,"Apple Color Emoji","Segoe UI Emoji","Segoe UI Symbol","Noto Color Emoji";
                    display: flex;
                    flex-direction: column;
                    margin-right: auto;
                    margin-left: auto;
                    max-width: 80em;
                }
                .row {
                    display: flex;
                    flex-wrap: wrap;
                    justify-content: center;
                    align-items: center;
                }
                .is-top-spacer {
                    height: 80px;
                    background-color: rgb(250, 250, 250);
                    border-bottom: 5px solid #007bff;
                    justify-content: center;
                    width: 100%;
                }
                .is-header {
                    justify-content: center;
                    background-color: rgb(230,230,250);
                    padding-top: 1rem;
                    padding-bottom: 1rem;
                    font-weight: 400;
                    font-size: 1.5rem;
                    padding: 0.5rem;
                    padding-bottom: 0;
                }
                .is-header .general {
                    opacity: 0.5;
                    font-weight: 400;
                    font-size: 1rem;
                    padding: 0.5rem;
                    padding-bottom: 0;
                }
                .is-header .title {
                    font-weight: 400;
                    font-size: 1.5rem;;
                    padding: 0.5rem;
                    padding-top: 0;
                }
                .is-content-outer {
                    background-color: rgb(230,230,250);
                }
                .is-content-inner {
                    background-color: white;
                    margin-left: 1rem;
                    margin-right: 1rem;
                    padding: 1rem;
                    width: 100%;
                }
                .is-content-footer {
                    display: flex;
                    justify-content: space-between;
                    background-color: rgb(230,230,250);
                    padding-top: 0.5rem;
                    padding-bottom: 0.5rem;
                    width: 100%;
                }
                .col {
                    display: flex;
                    padding-right: 15px;
                    padding-left: 15px;
                }
                .orcha-logo {
                    padding-right: 0.1rem;
                    height: 4rem;
                    opacity: 1.5;
                }
                .orcha-logo-text {
                    padding-left: 0.1rem;
                    height: 2.5rem;
                    opacity: 1.5;
                }
            </style>

        </head>
        <body>
            <div class="container">
                <div class="row is-top-spacer">
                    <img class="orcha-logo"
                        src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAJYAAACVCAYAAAC6lQNMAAAAAXNSR0IArs4c6QAAIABJREFUeF7tXQd8FVXW/097Jb0RQrWgIqAoYG+oqGDB7qooK2sXBRufoFhXXV0LYsXK2rBhV0RF7KCLCooK0kFaEtKTV6fcb8+dmfBIXt6b10JA7v5cILlz639OP2cE7Gg7TiADJyBkYMwdQ+44AewA1g4QZOQEdgArI8e6Y9AdwNqBgYycwA5ggYsDuVAKekFxD4Q7ewAU756A2B2iXAjAA8AFQAIgAmAAdEAIAywEQ6sDsAFh/1JogQXQg/MRqFkKgH5uZOTWtoFB/2rAEuHx7ARPp1Oh5J8JSekPCNkwwZWBxoIwtMUIN74Lf9WbCDctMUG5/bcMHWhHOjhPT+SXXQ5P/t8hSF0zByKHe2ZGNcKN0+GreAThpj8sCujw4W2n2/YILAFK/iDkd7kTctYxAOQOfB0GNPUHNG24GcHqz7cn1rkdAcvbA0U7PwiX93RLHurAeIq6NAYt/BUa1o9BuPa3bW3xLde7rQNLRHaXEcgpmwxBKN7WLyNi/X74qu5A458PAVC3xX1tq8ByoWDXe+EpGLuNUienWGHQfG+gasmlABqcPtQR+m1rwHKhoNfj8ORftNWF8Pa+PS0wE1WLzwVQ395TJzPftgIsEQW97oMn/7q/HKBa3qrqfxPVf5wPIJTMhbfXMx0fWNldzkNul+c7uHbXXvdlz8MQqL4F9Wvubu+Jnc7XcYHl9XZHbu/vIIrdnW7mL9ivEQ0rjoK//qeOtveOCCwBRb0mw5VPgvmO5uQEtMCHqFp8akey6ncsYHk8PVHQ52dAIB/djpbYCYRRv3owAjXfJ/ZYZnp3HGDl7zQO3uL7M7PNv9Co4aZnULOUzBNbtXUEYEko7T8XonzAVj2J7Wlyw1iHyp/7AGjaWtvaysDK6oKy3ksBIWdrHcB2PK+OhhUHbi3BfusBy1MyGAU9v/jL26UyjexA1WWo//PpTE/TcvytA6ycHlcgp9MT7b3Zv+x8obonULvyyvbcf/sDq3CX++Au/L/23OSOuQBooRmo+v2k9jqL9gVWcd/noHgubK/NOZvHjh6mqONUGkUs03/UUh0rlXXEeNbQ56Lyl8PaI7iw/YBV3PclKB7ycXWw9hcCFp08035ExULSwO23ICP30T7AajdKRWflZEuR1MU+11SpzDZAseytMv17VPxycEYQZQ3q5BZSm79dZCr75aM/nQDkLw4sulE1+AmqFw1L7XLbfjqzwMq49pcsQCKfs48g2lEkyi3s/rHGbCYbEbeS2Wto8/ozaKXP3I5MO9WXmXkjogEqEZbmlG2lmhYYi3raY9MVZO4a4p5/sHYs6lY9Grdfgh0ytCOyqO+5PnMnFuvCE2WFTi4/wVNt7u5k7GjAciorJruuFs/Vrz4UgZq5aRqND5MJYEkoG1CXGTeNEwriBFhOj7DlfJEgSHYtscZ0uq609zNQvrIYqKPs7bS09AOrtP9/M+dQTvYykz2rjgSsTFMxVonyBWXpMkOkF1gZC31xAqhEZCynQEtk3mgsLVWWHblOGivD8pjmex1VS85xejqx+qUPWDxIr++adCyq9RiJXHAmWWG8o2x5nOkGFs2fzv1F2U/juuPgq5yV6j2mC1gCygZUpzfyM1LVT0Ttj9iSwMyXvPlx+1KMiJ/FMg0kAminFCvafE72127uIh3l86lQSkpZQOkBVlGvh9Mbox7LnOD0XSIQiZZ2Yl4KEwyIhgD6n06g44iLdWGZAFZLipPoXjNMseig9PC32PTb4U5POlq/1IFF2TT5fdamsogtn030oKPPLEDETjt1wvVjR+Goww5CbnYOAqqKX/9YgVdeew8zP/4CIU0D0+0jiHZhf1Fg0ZE2rj8GvorZyd5r6sAq3Xdt+lK0UtF8tgSBAAP33nwVrr1sJCC4Tcol6NCYAI0BYcYw69u5uOyKG9DYqMEw7LJVsQTkZEGfrNCdqiE4WVjw50Ion5+VbAWc1IBlJpO+nNLymx+2DzFZUt8CWIxBEgzMnP4sDh7UD6qqQXZ5oCgyRDAwiNCYDr/BcM3Nd+PVaR/CMOJpXn8pYAGhhsdQu3xMMvebCrBElA0kAS9N9adasp1EARadbQlgIOplylIiZEnDYQftgykP3Itdu3dDmInwg+HZ19/CxAn3ghkGWCthP5mjTfcz6QrvSXBd5fPzkylIkjywCno9AE/+9QkuM0b3zACLc0BBpP/jVF1gBDUqKBrGGccfiReefBCqIMFv6Jj8xAu474GnOOViHF2Jgjt9p9G2yaWd16SHvsGm349IdGfJAsuFsoHB9LqEIkNfOBysvURbYjT1vG2V3SsLWLZ0DsKqih9+/h133fUAFi9awYlY/z474duP34bGGJoYcNqIi/HT3N+gEZVrHjLZY4p2HS3XGW/slufSzsCiLZQv7gEE1iUCrni7ij5WQa9n4Mm/OJGJnPd14ktLRFsjXm3gzVcexVGHH4QwRITB8O2cHzBy1BgY4TDOHn4Mpjz+IFSDoVY3cOABR6G+yseFfLOl8zKd7M+eN5pMl861OLwVXV2ATb8OdNibd0sGWBmgVpFLdnLwiQGLKM8+vbvjq9nvQGQkcYnwM4Z5ixbjrFNGQdBCeO35hzHkyMEIwUBNOIShx52Ntas3mjLXXx1YJtXqDgQoYsVRSxxYBbtOgqfgWkejt9nJiSAaLV4pQUDZ8wsMkigg78xzcOHB/XDL6cdCg4wmQ8QDD0/Bk48+AxE6vpj5Gvr13h1hCAgxhsenPo/773kMejgalbB+RrIYl98iqYti/ptKwdvvL9vyqAneXjcDU7ymIZfGMBiX7QxVRFjXAGbvl4EJOsimS6oIb1tw1HagYnp4Ljb9dqjTe08UWKQJ0mlRMf0U2lYAFgNcp5+PkJKFw3JVvHXLlQgKIhp1hiFHnoTKdZWcZY46/xRcOfoS5OXnYuXK1Xj62Wl4/Z1PIzTFttijCSxSDbKyZOzSqzsMyFizaiMCgRAMfXN5d4ExvDT1Ppw6bIiJEKab4wvgwNIh8WLwQdVAVXUtFiz4GR98/CW+/va/qK2qN79KQI81q6/tACxOtebnkenUycUnBqzsLucjt8tLTgZOvU80iuVk1LbMDoD32BOhFXaG/j971e2Dd8eok49FABIafQEce8QJaKxrBLih1HIBkQZJl8ck6PbdWZSC3iyyhtnOSBEivG7g/Q9ewV69d7dHgA6GH35fhL+d/Q/oTYBm6Jzm1K7+L7wukVNOu+nNFIojjI9hkPuJA8lcS4CF8c338zFp0pP4Zf5vYDrRWurLyZnVMgS0UOPTqF12mZNbSAxYnQdUtV914vQCiw7D7c6FIXthiCrkvCx88fYTKOvSFRokqIaKV199C29Mmw5/YxMURUFBfj58TU1YuXw9dJ1kLbNJJPx/8RZ22qUrjGYWJ0AWBUicnhArtJggA0ICQ5U/jIP2OwLBJrMI8uknHIZJd07kHgACDzUOB0FAYWEuJEmCKEugERn5Ny1qRqPr0KALBoKGhGmvv4t77nwAfn8YhiFm2kxCDmqLz8eGVwLA8vZAWZ8/naA1PX3SDywlKx+C7IFu6DD0INyihrlz3kdBcSF0JkIXBBjNzmnBtHkxhkcfnIInH3nWoggMBw7aF5+8+xR0yNCYacc3qQYgEXVTQ3jl1ekg89l555wFURHhYwJ+/HUxRpwykhNFJphsk1n2NZutmS5y0+4mMJLDJPToVoZhxx6Jv511MvbstSsEQebPE+3TIHAt9933P8L/jbsNKhmBGMx9cNEvltkmiZtq2HAS/OUz4j3pHFhFfd6Ay3tWvAHT9/v0A8ud2w1n3XIR9thvL9x/wdVoKq+FC0E8/Pg9OG7YMdZHbuhCTQ5HArZhMEx9dhruufsRU4YyGDyKgOW/fo2CHDdUxhA2JIREkVMeX70PgwYeipAhAboKr0vGb4u+hyEr8OkGjjj4ODRV+aEbJuXiQjtndTaJMykXRV9sZrQMgmAadwWo2GOX7rhtwrU4bujRMBiDJpC5V4DP0HDlJWPw5RfzwHQJjIMq8tM9aWCRhr4clb/sHu+enQJLQNlAOokUhfZ4y4n8va1lJerAbVvG6jRgf0z+YAp0TURTTTXGHjoMRpjYjIC8LIYTTh6KffftD1kytxnyh7BmzZ947pkXoTI3DAIbl7l0yIIEpjBCAI484Ug8POVByAZwzeir8flHXyMMk6pQmM4V11+Ei8deBh0ipr/1NiprGiFyomTSJ003tUFV0xDyB1CzqRpV5ZUoX70WtVU10MIqx4fIDBj0F06MBLgkDdeNuQjXXz0aqighzMCBPmPmp7ju6glgYZlT0s0t0bNs475M53Qg1m06A5aSvx+Ke/2QCCy27NvS0JeGN6f5NbcPLiKIr+VCBUAyJBQcfDienn4/mCFzY+jVQ09H9Zr1pq3KMMcxHT6mS0cUSSSnaAiywpv0g1Mzq58oStwudvZFI3DtxLFggogRp/4Ny39eyUFEzE4wdBx35gm4edLtMAQZAc7CRNO0QBYGoikmPjkrJtSQ3EbKgk5gExlcIR0rlyzFjGdexs8ffw4jrPHn+Rj/i+LwyDqefOJ+HDdsKLfDhXRg+do/ccpxZ8EImWzaxFeagNVUfhWaNjyeOrBK9pwJOSuFrNlU/YBtbSESsLGAxSBIWcgbeARuu+8K7NKrBwRDQGPAj09eegObqqohCQK8Hg8Utws5ebkoKC6GqyQf2fl5cOVnw+VyI8vlgkd2Q+LkxqQ4LmZwSlXgcnGhfsGcubhu1LXQVJOVgql48asZ6NqrK0RGPYjmiNA4wAgYJNfZbnJbC6VARBPLZP1XRfPPkMjAdA1LZs/BsxNuR6iqkQvx0MlAYeDYwQfg6RefgiqI0DQRKyvX4+TBJ8EIiRyk9iuRnF084g6YsR4VP8esZu2MYplsMMkohmRZWrTnkjOQioKG7IHHQtirD5RgA6bcdw1cAqnxRBFINqI32jwKYnccNMyMP7VlX1Mwp5+RvCPwv3PRSDTgZgIUCFB0AouBed99j8dvuouzwRse/zd23bsvvExAqK4eT/zzXkA057YF9sg5BEmC15WDnnvuhp799kDZbrtAzsuBwUSEBJGzO80A/KKOBS+/hem33w+mqqCIHxL2e/QswuyvZ0GXgIAhYf6vv+GS00dBU9MILNp4+Xw3ANsC3OrNdwAsT0+UpZIkkSyw0md5FyUJRbdNgt8rQ1YZ8iUNt566Hzp73DBE0gbNCAhuDbItlRbIZAJThJhCxIqaSBfJ/2ICUOIREQYfx08iNv2c6VAEEVlcLDJwyZBTsWbFBj5H88G3FIEs+xgHNo1Ja5NF9D1wX1xw+zh07tMHfkmEaggIG0CgtgH3nPg3hNaVc+7IWBjde3TGrLmzEBIkNIDhP49MwX8emgpDT1YhisIxGv48Bf6q99viJfGBlb/zv+AtujF5+Yq/1wm4JSNP2pYL7Nnt37Xs07Lflv3z++0H/aLL4VYNSAaFI2sI63500pqwtysEffWfnJ2owTA2VW1CU2MjVLUJTVX1CPgDUMMhzo9CZD3nl0csjkAkg4kkoBMADBx49GDc/vRkhCXR0iyJsglQwiquO20kFi1czClPJAM3lcIW7h5OwohFWhodgUtnnNVKOR6MvudWDDrtePgFESGSx5r8uH6fI8DClnGXaTh48AF4/JWnOLhUzcDJBx2H2vI6rlCYZDj+1ce88zjhNPFH77zPOghSt9SAlcjTibh7IsdtO0mBMztFgqy4wWRSw0mxUyGGwhStDI3kGbIrcRYogIkaBGaxSEHg1MkQKRFDsagaIIik/jMwka5bgKATmMKQPAKGnnEijhh2HLRAGDOnv4PvZn/Df6+StsljDiNeDNvcELEVkZEMRyCx2bRp8+JNECAbDEW9euD+T98Ac3u4VnnpHgeCqeQaIqXA4FT2iRcfxgFHHwEiVFMmPYYXJr9oymQmzU3kUqL1VVE+n76VHbXFAxb5BrXU4Z3IHpxsPJqsteVBmRszbUFkJCHtjdNN61LppSXjpGhIMLiNyL44BrmgGN4LLgX2GwTB64KweBF8Tz4Ko2IDQL5h0sSOHQ73WefC+Gku/C+/BN3XaLFRzXJKkyZqAcPyIhJYCcCbYRWN8gL9+/bAHXdOxF13P4b5C343WWwLMNL+FJeC8yZcga/enIHVi1aYnkoOLHNuV5aIrxd8BU+WB8cdMhSV62qh01uSLg2xfH5BW18jiw0sj2cXFPRdmQgsNve1CX6ib0YsAT2G5tf8HjIYgguiGIZrn0P4hZCxkctJjD4+T4iy3lhuRzKbTbFcffogfPlVCJNLhaiYqdVzs0H21CloePU5gLnh+mIuVHrrmQCvHoDw4P3wz3jfjD4l+0EkBbLNGAJD5y5FgCyiorwWhiZZbMmSzyhAh2moWfMjXJKC8qCKXnseDOgUdkh9owPRjK6I0pgBSdIhCgJUmov3S/Q+Ytx+/bozEah8K1qP2MAq6HEtPJ0mbUvAgiDBowHK/41F4JzzOBvj/zHTusvdKJxcWYEuhA1Og0z93kVx8bIAF5fOSc4hVmkgoErchJBz751o+vR9lH72IZryS+HSgQaiYhpD1lNPounV/0DX7NBmi+kIAg7bb098+OZUeGUX1wiDBvDtj79g9NUTsGHDJuikxjEKjdGwfMFs9OhUjOXlFei3/wlWzISDW2gO4Yno2+pnaQSWFnjP+oZPq8XFBlanveZAch3iYEtp6NKSBTrRYCLFYHMJMhg8I0eAjRsHj2FA0S1Acdsjl4ZMCmW9/RT0Rz/SDdHU5HQBHqjwfPQJVj30b96v9NqxMIafjgZVQl44gJqhQ8CMIFw790bpreNR22dfBEi1JOXggjOhr1wLlWQZnvUDKCLQ+OcPkCST9dqNVq8aGj6eNx/n/n0MNB85kg24xDD69OmN35ashErylklStzxj+9/NLiE7Loz6ckGumSVv+WAagQVWjfIFJYlTrLIBjZkpRxRtKWkAFmPI3qUnci64GKJLgxQwIAZVwFC57YnLWFxCty6JAEUimCRByipAw7ChMHQBBX8sxrqLz4RKIOBUTEfR86/Dt3s/Dkz1HyPhX/ErZIp4EHWU3vUg/EcfjyYwFP2xBLUXXgCNTDyWUrdz1xKs+GEG1yIjWREnJgZRRIZ6I4zTRlyOud8sMOOtqDWb+S0TPf+ZDbBYml2LF24LqpVOYIGhfH7UAWNRLPIP0tHEE/DTQK1oiEgKxV9R+3TbXAIXjbkcJHFNyHyEcY3Nds5s8babyLLeZuuNtu5A7tkNnunvwRMUod8yAQ3fzOZxW/Rrty4je8hgaP++H9lkBP3nTaj+5BMue8kUFeESUfTd92gIAdkG0HTswdDUsJllzRjcso7GP3+EBKnZ6WwIEndo077JBPHuR7Nw8bUT0eALA4wASBqnffzkSjJNNpziWvvYLHFZe7KPLSJCo5k6WkI7iQRk6U+LyYEGbyP4LxZo8lA2sB2/P9yW0N72mymTRifq3JbEiLrw3ZASS0dOL5J9CdGwb1rQbR1NKS6G58NPAFWA8tJzaJz6ODTN1LIkkaFo9PXQzxvJ2Zo29irUzJvLfyeTFiZp6Pzah6juvjM8MOAbdggMX5BTP5P9AscfNQDvvPQMFNFmhRqq63yY9MSzeOTJ5xEgDZKbGSx0MBHc7EAvigQUFmah127d0WfPPbDHnr2xc/euKCoqhMfjgSCacVhqOAy/z4fq6jqUV1Rj5YpVWLlmHdasWYOaaj+a/H5umuBzcJNcG8pAIqSiesX+UOt/bPlI28BSCgageNf5icyRWt/EgGUvnCzb3rPPh7bTzhAMAxIJwLQQWeFOYQKPaWrglk1THbfEdZJnJJLOmQGX4gJOORkal8s01P/9DOjry7nMI3beBTnTX0O2KMDtErH+sMMRDvlN0NHFMwElH3yGxk7FyBMM1A4+CGGV5CUTWJxKiAyKqKOssymSVFZWQ+WXTKAgAkJaBLFeA7KsY9CgPrjwHyNw/FGDUZqXxeO8NuuwbbMRc6yIm7CwQz/TwLhD/alp72DCjfdC5/6rFFtT+Rg0bXjMObCyOl+EvG7PJj+tE3tUtNFbAqw1xeLHwS3egHL+SBhjrqabg2xZE8gpTJfJuRxPtTf/ZKIpZ9lmHBNkPPAKkkj/8QgsHrxnGEHkLl8OpmoI7N0PkiCjUAdc33yCpTfeDGbFsBO1kGQB2bPnQZEEFEghrDn4SOgsxFllvMaD+ggNooZDDuyPSffcin127wUX+Y6s8ORWlxbFvMDdRNbPN8fCRz5pUjUC1y77HI6KGr+1vliyWrzVU2xRwzTULm/1YYi2IVu462NwF6TwYZ8MAksgh68A18jzoV0/BjJRLQKZ5b8z49TNQzGpU+ttWkpi86vPzQ2cc5JAzmNYeGyTRGPKQDYpBhtWY9lZZ4Gppm2MmospyL7jdhhDj4dXAIo/eheL7r2Lx8JsYc9qw9ouCDrOOnkIHn/gDuTnZEMxzMgGM4I0nQKuwKnvjNnf4IxRYznbNW8oRWDp6nxs+nWQc4pV2u8ziG5KI9nKrbVJgSiL97hhwIRrkf/9fPjmfAejoYb707hMEmH4tFPrOciI3djmBgt5xCL4Ty3vsijJUA46GGz4cI4ockJnh/1QX3wBf0593sy2oWlIkzQM5J56BoITbjSBJzBUDDkaQV/DZt2j1emZoRNkGe/WJRdfffw6epaW8jU4IHBJ3wWx62BYRdGuAxFi5G2wDcUpAgu8dmnnBIC19x8Qld5J7yStD27JHiWIkAqzYNT6uLCuE6+z7Ta8sFqzgmhpgbZ5ofXVtfwJyWOi8T/NT1QhZHm4y45MFppIQXtmiRFqiiDBff14GGecASqSlCWqMB5+HBtfnwpGVu42GsVkEa249JIzMPmfE0BBqAR4U+fLXKM4+/2OGY7flq4xqRWX6ez5UjFBsADKF1BE6Rat7b2UDagAhNLMbDXSlOBkBrO/RBktdMHc9WH64SgCwFSkRM7CmqUoHh9uknqu/XFbjh1o0UIfsn9vLUXWaVyKXiDR3HQAEyAMGRBdBfAMOxHSVZcilO1BNiR4yeHy0SxsuGsCDJ2yEw0zrqt5m6YaRqAlSvXsU3di5PDjLZOUGfOVDmDZZq/mE7XGJVnvkrHj8eLbM8F4sKEVZ9YMrJSoFmXutIrViwWsekCgBMUMtCTlL7L9SGTnkeAu6QLXwL3hLe0C2ZMP5pZ5FKgoKKZ8YmuBJras/yOt0NwOF5rt+CsCLDOg6zpEyvvTdR6uLAoGmFuBll/GtU6jR3c0FWbDS+EQLgFuxuAh88PUZ1D1zKPkDALzeqEHw9C1iGhz7uRWuMb66ouTcNoxh0Gy4iZbGtRTOexWwLICKcbd9i889tyr0DitjEZNUwKWYQFrC+IfC1gBQPCkstG2n00CWAJl6ylQTjgJ8jVjwTy55MKHxxC4v46gJFNgAQ9PNxkWyRWRzf4XF+itW7Bz+nipIwIYJdeIppxm0UJokoGwBMiGAE0R4CG/o8hQXLEeK64ai9DaFfy6bv/vp/B0K8XaL+fg0fOv5qYLE8RE8QQ8/MB4XH7uGZDIlmWtLZPAIvPqpdeMx8tvzkSYyRY1bw7AiTialIBF1nczf7fFiNHvv2xAGKDXP9kWCzzOf0fsT5cMyKIH+U89hkC/QcjWCEkSci07DJkXBB65ECGncFa4pexgu914dKZp1iLjAt+gybrMPD8z1880adBKyWIeVBh0mcGt+qF9PReVj00BW7cSGrEWxuDN9uKp5fTVEAF6WMVFvQ7krkIOcibgnFOPwguP3QNFNI80VUBFo042Q6V44Rp/CBdeciU+/+q/UBmxZyuIPmokRErAIut7K90jFsWi9z+FdC/n4NkSui2eI0rFgLzHn0TTAfsjzwCyKJKgfBU2Pf8S9PJKM6JTCJpWbh6fy9U8cLcJl7FMumF6OkzNkUtZBCIK6uOF2fh1b3YF8SwakY/Hgn4EqmsgVNdC1YOQuGYoc1eSxgkduZGAiVMfwqAjD8N/brsX77/0lpXXB+R7JWxa/j1PGbMBkQlgBZiB9Y0+BFTg3HNGYsniVZagHg/JqQjv3K2TELAozyTJBIp4VC6WEbTF7wQReUOGIXT3ncgSBBTpIVRdfDkCi+dzasGFeeuWTBcN5X2YVIJUajOQzyJPkTqDlTBhq2LNZROspTcnklr/NoV/YmlmWhgPAuZGSeKdpsnCkElGIxOWrYVSyI6Gbz98GQcN6h/vUJp/H50a2QsxF06+Ub5HCGjUNKyv98FH4TiNIZx64mmoq2/isqjN6mNP3r7ACvJywxlpzoElChKyP/8SWlY2ygQdlWeejeCGZTwHT6QYJmJVJPtQ2swWXJ7+oQByGIzICud3puzFXy+JNDRAtZ3fDkgIZeIMGn40uvftjfceeQZEGngdCE2CmkNGBxmCX4VmsVcig3v0LMKi72clFF4XE1gCKQVUmBcob/SjIhgGRBkKRHz+yacYP34idO4aMiMpNlecaOsiU2SDdOqJyVgDGwDkZgRXmwNDooh7JjpIBhKZjLxz/obaG65HngZkT38dmx66n1+bxFxghXnIHXkRPP36wsjKhkAuG0MBpRnTGy3BhfAn09Ew7T9gGhnTNe5zm/TNB1C6lmLqNeMx/8MveJr6Fv6XVnIIUQkDux2wL255cyqXwX568yNMuWYiZF0B9h0I8aEpnFpq5w2Hvqna9DFCwE+fv4Z99twtSsJErJONtFXYLBoICwLqgioqGpoQpEgJkaQ/wNBUXHHxaPw0f7Fl+LTjzjbb3TJzj6a4mCCwBmwChKhBXKkvMlb4sTk6yT2UUu79aAa0Ll2hGAyhoScBmzZyecg75jqIp9P3hMxUd9IOiX3xMGLrLvwuHaImQDvqKPg1qo2solf/Ppg48yV4whJqa6px1f5DrTLcsXbFqylgwLGH4YbnHuX5gp8/Pw3P3nEfV99Lpj6Put2JjRQHAAAbB0lEQVT2Qg6Vb7vmMjQt+JGPWZztRvny75pzECNniEWVeJE1qhxDxUYEAbWBMCp8PgTo2EijJG+BQEGNEr787Ev837ibeCQFpa1utqfY4nOKbC7+ZZO5oZUs3rbwXrr3MogKvWoZaPGBRWlTZJTMu/xCCFdchoJfFuDPCy+GZgjodMMtaDxlOBeGiZIpAuOFg7iJgGfbUOkM083v3bAKy845i1Msg1GtdxHv/j4HqteLaXfdj+lPv+pgfyTcEwXSMOz801Fa3AnTHnmKR52Sa6fT7O8RcivwuHTUHnY41BBFNjA8cPs1uPayEVzWaXXQW/gBNwt/dDIBQ0e5P4iagMZfFIpZp5r1JPJSHD7Z7jds2IhLLrwC5RuqeYq+6eWMdp2ZBhYLonwBlSXcosUC1lcQFYdlmFv785IP2o8AHReOyWYkgZHLxJAgevIhfTUbWbqIHF2A/Pn7KH/4AYQDVo0Ki41x7ZAuIlAH1bDNcebRCxKDx63A8IUQtiwqPES5VWv7BbB7e7vvCvH1t5AlMRTVrcOS4SeDaZRPyFCzci6y3CSmmvsgW5bt6BaoGgwZVwXAr+qoDfhRGwohpIl8v9zfyRUFkxKbWf0CGmsbMebKsVj42x9mWA4l13Nltq2rzDiwqlC+oJNzYCVUGTlDwOI80RS4STbXSUTNUlAw+2sEPAK6z/wIK2++3ap4Z5YF4jFNnFdQ5ShKeJd5SI3Z7D+Z5R7aHP3L/XUtHYdkG+OWiy0FXF4wxBqxZOIdaDphOLJkA577H8SGt1/nLpMD9toJ33w8nfcjQNFIKjMQVHXUB0KoV3WEdDN/UeSmkYg4A9GKI7M8CESxqioqMX7czVjw80IwbpDjZlfuyiL3U9stw8Ay1F9R+WsrlbdtipXTdTRyymJWFNm8GRtYKWsYzUOaxceoXJCOTnvuhk2rVkEPkpBtQCwshLtLT4SWLeRRnmazQGCZAOjQTauUeehkJvDmerHH7jth/0H90X+vPbHzrj1RXFgIF6cqQDgQxtXj7sS8H37hYLapmIsBORNvgtKrF6qvvhJCUxAqTxw1kPfld9DcbmQJBiqHHgGhiVxOOl57+REM2KcffIaOIBVb4NSKdmWul1OkCCpjBiRaMfn29w4EAT9/vwA333In1m2s5BSMKtZEj7eKhFaGwRQ5VbjxTdQsa1U3rW1geYsOQv7O38V4FSJ+lX5gkUyj6CLumv0mpD67wFtdjTH7HwcxbMa5SwYVTPTAECj3ynLhWuzDjm/IyhJx+mnDcOXFF6DPHrvypAkytvJwYstwyjVCipHQDSxbsx77Hj6cEtmtxE9TGM4fMgT6bf/m1CV76tPY9PKTkEi26z0AeOFZuCSgU105Vp1wEre2S5KBb3/6gpszuIWfO4PtNZrHZoMq8k9uzhUE+JsaMeWJp/Hqa29CDwOa6IJcVALD54MeaHJwJe0ILF/lDWhcd3/LRbUNLKAIZQOrHewirV1sTxZpRsQinln1AzxMBjM0jOp7MLcdUZ6fWfyGq0lWCViqOSXAJYdwwcjTceO4MehSVMzL+xCVaA7rjYhzN13VJqWgXMCyPgeg2m97CGlbCk+b7/Lh5wjmF8DDRDRccwmafl5ABbuR+9w0qH32RjbB5p47UP3hh9yWduZZJ2LCTddBaI7U20ypCLS240kgnyQjZ7eIUCiED977CE8++SSqymsgiAp0yQ13py6QS7vBkN0Ir/gNan1NC9NFpBiymdVvvpRYV5yGq6tfdRgCtXMSARal10fWGUzDKuIPwX1+ltOFguxOuHgERo6/CjOfeQEvPPC0GV1pnaUJFhMehUVuPPPI3ThpyJG8EEdLS5A9c0uLOpdVDGD09Tfh2dc/tD4WYPsKdXj2OQLKE4+arqRQDVYT9SJDa2FX5Hz0AYiJkkq0bvAh0DSqmcDw+ZwPkJebzUOiI6mT+S/LQQ4ZG8vLMf311zF9+ruoryN7tC3nCcjtty+YJw+i5DJDeEQFgcU/IVxHwGrZWrrPooEt/tkn1aN8fjG5JhMBFlA2IM0RDrZ0vNm11EyhLNmD24q5cEwuGTOli9Mk4iu6FXPFkyPMYKO8LBGvTXsMxx64HyQSgnmmi3mwnGC0qS2ZayG3yMLfl2G/oadDg4tTENOCL/IwnOJPZkHPKYREKB8/EZu+ngFBd6Fw8iNQDz4Y2SLgnfofrHj2Ma7i9ehahvc/mcZBYht37HiwjRvK8emMWXj77bexdu1Gbh23E2abs7G5HGag4MDjuaeWV2V2uyG6Paj/9jNoTbVR7j/SV2WCt50oVlTjKD/XmCgt3Xs+RGVAUkhuJhGb/0KXqBgGVO5+MWMPeG0q7nezQ0wEnHryUTh6yJEYf/M/EWy0tD3Lf2z6AyVILIybJozGTWMvBZU8sfW0SBxFFgDh3DNitzyCgarlaRpKdx/Es5x54gXpBxI5QiQUnUs2tKvgkRjy6uux7KTBJPBA6LYzxPfeQIGmINcIYOVRR0AMkQZq4Nmpk9F/n37YsGE9fvzuZ3w2azZ+W7QYPn+Ql9bmvkTOsQj4pjbYrNPxNZLNSkThaReBuUTosgJBVqCJInyvTIERpLLIHaWxBpQvoM/OtWqxgVWw863wFN2R7Dbs7GM7nIWq1Uk5eZBKciB6cqH7NGgV6yEEG3mpHZ7OXpCFVYu+hqEK+PLnRTjltL9zTdB0+JqZJp1LsjB31lvYqayTmawaIQxHrpWXGbLE5qCqorK2HuUVFSgvr+Q1sAKBAKa99RG+mreQq+2cTnGKZ0DqvCuU919DFpORDwOVI0fAt2Ipp2Qlb7wHX89uyKJSRteMQ/V3s7mNzaSQATBG5Y4Ix7YP3/wXZUKLJcUQcwvBGqqAUAAGDwqkGqg89sekooKMvPOvgihSgVwKYJQhCCHUvvAQrzffYZoWmo2q349JHFiunL4o2uN3pxsx2RovgADBULjgzLIKkHfGSfCceQpChZ25L0uib8UwnRfWJ5kkZ+0qNN5yM/T165DlYti49AdIBsOT06bj2pvvM7U+bp8ycNKwQ/H21Ee4Ndpu1rVxAJKxsKa+Dp9/9S1effs9fP/TQlTXNXJLuCGRRGSZRFo6nYl1UR8yTioeFMyYAZabiywIyP7sU6y+7Xr+oYHCU89BYPwNKNBF5P8xH0svvIRTW7MaYIShMmpWjgBv564Ide4FmfIHy/+AFtah+pog6JQuZgJREGXkjrqas2KBfibKyIKKdc/fY+bjmq+A02vJXL/GDf+Ar/z5xIFFZl2zPpazZrE1RXfD6FGC3H/eCq3/PlweyqJiG2EVYn0DRH+QJ04GC3IBRYaPTACNgH7v9QjP+y9ys13Ybddd8dOvC6Ezr1lSnxkYP+5i3H79FdzgaEd8mt9PZfjl9yW4495JmPXVHIQ1ckJzJ48VUGfJHM1gamkJtbik6IIkeJH3zivQS7tAkYDOjZvwx/FDIYcMCIXdIX7yHlyCjE7BaqwecgL0cNgyTUTy2dbjW6QIguiCe5/BYJIIuW4NtMZ66E0+MH9jc7iNq6gbvCMvBZNdkIrzIRcWQPrsXVS889rm7zZ1BGCVLy0FmjYlAyyg875VEESS/GM0m4QbkJVc5Ey6C8HDDzct5uRIrdMhNvjgamgE21QOVFTAVdsE1RdGUA8g59CB8HXrAU+tH3Wjz4YQVJtLKpIcQqzpwbvH4apRI0yjupUJWtfoww23/wsvTf8AukF1ZuxUioiLtVgoLb715+GsOg8GWbpFCLklKH7tJdR3KkG2YKBIV7Hx+NMQrq0BZBU5M2dBKyhEl7CKVcNPAqsu5zFhrdwpzYH1Jth4fXayptOyXF549z8SgqBAqK+AXlcJBAIIV5dzJzI90WnUlQgceAgU2rukwgUXasaPhl5TyyM0zBYpxWTYpBD95qMmUdhd46+osNcUuPMvj40q0wRItCO33y5geSWQu3WDceD+0A46ACyrGELYshnxYvmAp74BwTlz4P5jGdQmP4TDB8GreaB/8g5q5329OQv4f9WBJl5/GW695kJ+GfTKllfX4tyLrsLcH38Fo292CVRv3QQTl0Ba+maa2WZr9uE2dKguCd59DoH++INcaM+FhDxoqBgxEuqq32HIAkqeegmBvfujJBzCxjNPQbh8ownmmO4Ui2lx8FE9JRlFp4yCz5sFvb4OWWuXwQg0Qa3agFBtLdcQRcWNgoefhsIEhF0yXFCQ1VCB1eMuBtW9sUsjbXkfKbFFy3LrjCk19zK0n1G5sE3FLj6wXDl9ULTHovjTmk4Ugg/V6yQZi6vupGFle1F4+WWQzhwBTXXx+CdG/juNQfnzD4RffANGQyO8+w9AduVGrH2HIg5MoJw07AhMf+ZBbn4IqRrOv3QsPpw1h18CuZT5//gXG8yttGJCvGwRET1ii3bRWcuEQeJgfifk3n0ntH0HQlUMFKgUSx/ExvPOQ3DdCh73VXj33WBHDkGuvw7rzzwTal2tGVHQnPTZ8j3dchXcXCJqyO19APTjT+NrlgJByHNncWG84Y/5pqooAJ1GXAzj8CPBFAFuw/W/Sm8e+G65Fo0rF/EPHzDLzrfVgdW4YRR85S+0hYv4wKLtmnJWnNci0ijXYjpeP4FB8HrR7bkXEO7Rm8eSB2l2nSH3t1+w6dnnIOcXwFucj5r3XuMhKV06ZWPp/M84qN794CNcOHq8+SkRayl8xpZIimK3Ih2NinKQtsUFeLcbSt/+yL3wQjT17QdBkZEtaPBqMryVK7HmvFHQw/SJOaDk5juhDxuM3Hm/Yu24K6m4MlcEWkcTRPpJt9Tc+IcDRBViXimKLxvDIyqEBfMgVK2Bb+lKhAIB/qklb9+DUXjbrdxnLssymOJG9tJFWH7dpdB1W8OMphWmRLHi04xoPcrn5wCgjOGozQmwgJK9v4esHBh/BW2owrzoBX0+xIDCDJRMvg/CoKE8Op2+XEV10GtuvAH6pg3Iyi9B/c/fwKUrWLbwIxTk5eHsEZfii+/mQyP1m1M7E02WGTTusjy6hOwJ48FOOtH89iDIZygh6CErA+Pp8S4qMvLoQ6iY9jrAwtBkLzrddx/ye+2MilvuQP2iOZBCOdB4arSdnh45ddvA4rYxbrc1rb1MUcw0NYPxlDVD0uHabSDy7r8bkuCBIBlwQ0S2pmLxOcOhBygc2T7btAIrOTbIWC0qFhTFOnhnwPIUH4OCnWbFvcGoHVpTMorP7vL4w9D3pa9kmWKD+81XUf3WazAa64GmRjx+/0QceuAgHH7cqdB9DIEuvSAMORzeTXXwffkxmKZZ1fDi23Xoo0s5386DKDJIJOqQ9YIiMUWS0DS4Pv4Yax64G2JjCLrE4M3vhN3GXIJV09+C9vvvCKSSBdfiTEwvApXvJnAzaC4JBSeeCunCSyGTtV+U4CHJUdCw9srL4F9N1ZAj21agTi3vNVhzB+pW3546sOg9i1+Wuw30twAWuWuoLAy86PbJRwh4i6CoQOEP32HZvbcCNY3o1asM11x8PsZM+CcXzpVO3eA+/nSEy8qgwQ1Fa0D4qckw/D7+7cF4jSIaut59F4JHHwePqkNsbIB70e/Y9N67qP3xWwghi9NbNUqJipGwbTAFgqBx42c6GvcwiAwesuPlyPAMOBg5V1wBo7AUHkOGoJDIICJHCGP16CvRtGoRoTAKNNOxmhSy+svnFwKoSwewgJK9voDsOjLxLUUAy6qFyVkDGLL79kXnp6fxr1W53n0F6x57hLO6k4cfhffen8nT0omB6h6eAw3WrTuyRlyAkJwNV1MV/A/ehRD37VGen30BrSkYJ8uk60sKF37NjyfxmolcXiONkscC2i4d/oAZNWH6JNsqFJvYaZAngpJr8++bhPBhR5j2OQNwGQYEmT6rKaJzXT0W/uM8aJs2cAVocxBf2ilVsmywAhULyuLt3BkrpFFchXuhaJdf4w3Y+vetgWX3Ie9/z3ffh1rQDVXnnohwRRUULcQFbeYpgKv3HtBKOsMVZvxDjy6qHeX1wnXUkdBEBeLsNxCa/RnCPN3J3kp81tjmHtIEoLbG53a97rtCeHc6cjSRg4x861Q81yuG4Zs8GRvfepVTyFZKSTzdKbGLseONEnuKejeWXwjfhv/Ee9A5sGiksoGkBbQqWRN7khhx44KI/KOPR95lI7Hu3PPNHDi3CyUT/4XA0YcjLItw+ylkhMG1cDGaZs6Gu6kJbOduUPv3Q5G/AZW3Uh4dGVTTAKx4p+X49/ZaWlvgRcWFnT+fhWBuNnKDYahLl6DumedQ998vIIddvIy3+WVUu9l/T+yq4iw1OWplpnqRSyOu/JHYanN73oDskn87O98Y5gcagNfvFMC8LmR16YnAmqWQe/SF++Wp0NxeGGRxDllZxladH5e/CU0vvQJvvR/CMYfzb/A13nwtmN/fXMAjIlbA2TIz0stmW61fKsUIQ6TvUruzwFQf/2CUHddhO8K3BFViV+RgO8lTK9X/Pqr/OMXBHHHCZlqPoKBsYChuuE3zc7HBRSFO9NEgMkHQp2eVwk5QSoqh7L0XXMOPhdpnILyGCI3S5A36arsAORyA8NzLEHv3BCvqhtCkW+Fbt46+AGDNmgIrdHJijvq0DSySGRVDR1g2zRORZS1bDx1pwnA0cWY7lS/pAvjKnUyS+OtQ0vs1yNlnOxm8dZ8ol97Cr9b8jERCrYC8U06FMmECRE1GGCJCmgHv8jUQl/4CreduCD/9GJoW/hBBm51krEQDfGLpX7H333KsONTb0WGmRXhPlgUChr4Ulb84rvCYOLCAFOq/OweWRBX6JBUK1VrPUlD2+nSES3fiGiRRr8JZXwHduqJ+yiTULZwfIezuAFYbOE0eVDRgzZL+CPscK2/JAAso6fMRZO/xjl60mGGycYBmf0WCFHNJRLdXXoHeoxcadaBwYzmy6xqx4c4b0bB2rfX9GC68xVhWZLKB3S8WJUiWrSZCsdq22G+5kZQpVvLAMrQ/UblwJ2f3bfZKDlhAPsoGxjSQWYtoTn6KSMeMWF8CF8cAxS0j77NvYMguZPmakL9+A5ZeMQqGX42TtGlP+ZcFFlmAky9JVb1kX6i+X9oDWEDxntOhZJ3pYLIYWohNNWJRmc2eZsUQkXXogcDkJ5CjhtB5yTL8etlIXvDf9h86k30iZZ5YlMDh+ponbcs0EEvGikexUjY3pAYq6EtR7ly2so8iWYpFz7tRNpAKJqQyhjPqZQn49PElUXCjdMZMhPML0fmtaVg+aTL/IGX0cBIHsHdkeHRCWaNpcIk+17J/yloh/7iZk1Nos0/54p5AYG2iY6QGivydJsJbfFeik0bvHyMygjNtCkfmpVeQd8wJyLvlDtSOOBlN6zdC1CluwsklRpvZieziZOwOByyb3CZ/x1rgQ1QtHp7M/SY/qT1bWSoF2mKp/W0I9pSM4Xaj5//is/4cdrTDDw05AU+s40trqIo1kRPtNZkrbX6Gaty2+TFwByNT3SvKxaVxEm6pAysrfxDyerX6rJizlTgEVssYcoGhdMgxqPhiFpiZpBenOemzXQErdRboq7wYjeuei3eybf0+dWDRyCV9PoDsPSnZRSTEGnlnsxRiHJE/YthkBeCWwnvKMk+UrTo11jo+3VQpFUWLrETFL70czxilY3qAZaaJ+VMkvS2W1zYrTH7DiVKutAvTmQZWihqg9daWz6cSodGKRDg++nQBC0io7JGT9TkRmJ2ME9knWWBFizZIdO62+qeNYqXO/miJgU1XoH7tk6nuLn3AopUU7fE0XDmXpLaoTAAq1oqcWN4TBWQiJ5A2YCUyaRvSR3geKn9zkNsQf6r0AovmK913LUSxe/ypE3mDkx8t/pM7gGWdUdD6cPjmKizxD6/NHukHFpBjuXtSMMxFvsWJ2ocSjVLIJDVycjMdhGLVLt8doYblTlbspE8mgAWkZIKwl20f+A5gObnIlPo0VoyCb32byafJjJ0ZYNFK8nteCm/JU8ksKnPPRJPfoh1B5o7F2d5SN5o7m4fMn76nULMkVgkFx0NFdszsCRbu+jjcBaOTWllGHnKiGGTCVpXoZux1ZphNa6FvUPW7w1r+ie0hs8CitZT0+xCy+8TElpWp3rGiFTJpUkh0P8mHTjmfiUct7BkngM35cC16Zh5YNGHpPnMgSockvcq0P5hOgdmmLk4pXTtRo5hnxjagfEEPuzhP2o83fSEvcZcmoHP/eRDk/eL2bJcOf2FgMUYJpwSqtJgV2rqu9qFY5uwCOu8zF4J0ULtgx67ZzSdzEiqcYXmmfTYdZxZOqXbONKjMy27vVtz3Yyieoe0zbVvaVTopVvvsJPVZuEzVJ5PsL3KN7Q8smj0trp9Uj7oluLZjimVqf4MzJahHu4mtAyxaScEuY+ApfCRVeKT+fDvajFJfbOIjZMhOFW8hWw9YtDJv0SHI3/mb+NUC421jx++jnkAGLOpOT3rrAouvsqAAZbssAYRSp4ve0S/uCQRRu3zvdPr+4s7YokMHABZfkYCS3q8mn7qf6La34/4GD305rD00v1in2FGAZa4xu/RY5HafmXLK0naMmxhbYwhsGp2OIL10HF/HApa5Izc67fUZJBe9dTuakxMwY9T3TzWc2MlUTvt0RGBZ1KvzEOR2m8GBtqO1dQIGfJWXppJNk6mj7bjAMncsonC3h+HOuypTB7DNjmsmk56RbN5fpvfd0YFl7z8Pnfp9CMl9eKYPpOOPTxb0pcckk/bennvbVoBlnYm3Ozrt9j6kFD/O2Z4nnK65qJRQ7YqTE636kq7pEx1nGwOWvT1vN3Tq9QYkVwcKxUn06B32p0p6dcvPTKTomcORM9ptGwVW85nkonD3B+DOvWg7M1EwqP4PUL32Mqc1PzOKkiQG39aBZW9ZQFbZCcjpPBmitFsS59AxHqFYqaaKG+Hb8KKTktcdY9HRV7G9ACtyd17kdL0Q2aU3QhC7deTD52ujDx6Fah9B3erJ8T4j0uH3ErHA7RFYkefvQlbJMGR3HgfJTQGGVPx+azcdhvYrfJWT4St/M9an2bb2QlOZf3sHVsuzyYe39BhkF4+E7DkMEOjTaJk8A6rB2ggt/AMC1S/D1zSjrW8op3KJHfHZTB5qR9xvtDXlQsnvDXf2QVCyDoLs7QNJphIBuYBAhcvsLInIs4pI92FUNqgJhrYRWnAJ1MA8aL65CNQuBlDbnsF1HenAdwAr/m3EOiPnJbriz7Nd9dgBrO3qOjvOZnYAq+PcxXa1kh3A2q6us+NsZgewOs5dbFcr+X9aWdfvVmtRgwAAAABJRU5ErkJggg==">
                    <img class="orcha-logo-text"
                        src="data:image/png;base64,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">
                </div>
                <div class="row is-header">
                    <div class="general">
                        {{header}}
                    </div>
                </div>
                <div class="row is-header">
                    <div class="title">
                        {{title}}
                    </div>
                </div>
                <div class="row is-content-outer">
                    <div class="is-content-inner">
                        {{content}}
                    </div>
                </div>
                <div class="row is-content-footer">
                    <div class="col">
                        {{footer}}
                    </div>
                    <div class="col">
                        Avant Data Solutions
                    </div>
                </div>
            </div>
        </body>
        </html>
        '''
)
