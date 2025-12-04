import msal
import requests


def do_get(endpoint: str, token: str):
    """
    General function to call the Graph API with a token and
    raises an exception if the response is not successful.
    """
    response = requests.get(
        endpoint,
        headers={'Authorization': 'Bearer ' + token},
    )
    response.raise_for_status()
    return response


def do_post(endpoint: str, token: str, data: dict):
    """
    General function to call the Graph API with a token and
    raises an exception if the response is not successful.
    """
    response = requests.post(
        endpoint,
        headers={'Authorization': 'Bearer ' + token},
        json=data
    )
    response.raise_for_status()
    return response


def do_put(endpoint: str, token: str, data: bytes, content_type: str = 'application/octet-stream'):
    """
    General function to upload bytes to the Graph API with a token and
    raises an exception if the response is not successful.
    """
    response = requests.put(
        endpoint,
        headers={
            'Authorization': 'Bearer ' + token,
            'Content-Type': content_type
        },
        data=data
    )
    response.raise_for_status()
    return response


def get_msal_token_app_only_login(
        client_id: str,
        client_secret: str,
        authority: str,
        scope=['https://graph.microsoft.com/.default']
    ):
    """
    This uses the client credentials flow which requires a client ID and client secret.
    This is recommended for server-to-server communication however is limited to scope-based
    permissions and broad access such as File.Read.All.
    Using the Resource Owner Password Credential (ROPC) flow for accessing single shared files
    is another option.
    """
    app = msal.ConfidentialClientApplication(
        client_id, authority=authority,
        client_credential=client_secret,
        # token_cache=... # https://msal-python.readthedocs.io/en/latest/#msal.SerializableTokenCache
    )

    result = app.acquire_token_silent(scope, account=None)

    if not result:
        result = app.acquire_token_for_client(scopes=scope)

    if not result:
        raise Exception('Failed to acquire token.')

    if result.get('access_token'):
        return result['access_token']
    else:
        raise Exception('Failed to acquire token.')


def get_msal_token_resource_owner_login(
        client_id: str,
        authority: str,
        username: str,
        password: str,
        scope=['https://graph.microsoft.com/.default']
    ):
    """
    This uses the Resource Owner Password Credential (ROPC) flow which requires
    a username and password and NO MFA enabled. This is not recommended by Microsoft
    and should only be used with limited permissions on the account and a very
    long and complex password.
    The advantage is it can be used for shared files to limit having to use
    File.Read.All permissions which is a high level of access when only reading
    a limited number of files.
    """
    app = msal.PublicClientApplication(
        client_id,
        authority=authority
    )

    result = app.acquire_token_by_username_password(username, password, scopes=scope)

    if not result:
        raise Exception('Failed to acquire token.')

    if result.get('access_token'):
        return result['access_token']
    else:
        raise Exception('Failed to acquire token.')


# create a function to do interactive login
def get_msal_token_interactive_login(
        client_id: str,
        authority: str,
        scope=['https://graph.microsoft.com/.default']
    ):
    """
    This uses the interactive flow which requires a browser to login.
    This is recommended for user-to-server communication and is the most
    secure method as it requires MFA.
    Note: This doesn't work on headless sessions.
    """
    app = msal.PublicClientApplication(
        client_id,
        authority=authority
    )

    result = app.acquire_token_interactive(scopes=scope)

    if not result:
        raise Exception('Failed to acquire token.')

    if result.get('access_token'):
        return result['access_token']
    else:
        raise Exception('Failed to acquire token.')