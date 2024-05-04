
import base64
import io
import logging
from dataclasses import dataclass
from typing import Optional

import msal
import pandas as pd
import requests

from orcha.core.module_base import EntityBase, SourceBase, module_function


@dataclass
class _User:
    email: str
    displayName: str
    id: Optional[str] = None


@dataclass
class _CreatedBy:
    user: _User


@dataclass
class _LastModifiedBy:
    user: _User


@dataclass
class _ParentReference:
    id: str
    siteId: str
    driveType: Optional[str] = None
    driveId: Optional[str] = None
    name: Optional[str] = None
    path: Optional[str] = None


@dataclass
class _Hashes:
    quickXorHash: str


@dataclass
class _File:
    hashes: _Hashes
    mimeType: str


@dataclass
class _FileSystemInfo:
    createdDateTime: str
    lastModifiedDateTime: str


@dataclass
class _Shared:
    scope: str


@dataclass
class SharedDriveItem:
    odata_context: str
    microsoft_graph_downloadUrl: str
    microsoft_graph_Decorator: str
    createdBy: _CreatedBy
    createdDateTime: str
    eTag: str
    id: str
    lastModifiedBy: _LastModifiedBy
    lastModifiedDateTime: str
    name: str
    parentReference: _ParentReference
    webUrl: str
    cTag: str
    file: _File
    fileSystemInfo: _FileSystemInfo
    shared: _Shared
    size: int
    _file_name: str | None = None
    _file_bytes: bytes | None = None

    @staticmethod
    def from_dict(request_dict: dict) -> 'SharedDriveItem':
        """
        Takes the text from a request and returns a SharedDriveItem object.
        The text is required to convert unparseable keys into python-valid keys.
        """
        dict_copy = request_dict.copy()
        dict_copy['odata_context'] = dict_copy.pop('@odata.context')
        dict_copy['microsoft_graph_downloadUrl'] = dict_copy.pop('@microsoft.graph.downloadUrl')
        dict_copy['microsoft_graph_Decorator'] = dict_copy.pop('@microsoft.graph.Decorator')
        dict_copy['createdBy'] = _CreatedBy(**dict_copy.pop('createdBy'))
        dict_copy['parentReference'] = _ParentReference(**dict_copy.pop('parentReference'))
        dict_copy['file'] = _File(**dict_copy.pop('file'))
        dict_copy['fileSystemInfo'] = _FileSystemInfo(**dict_copy.pop('fileSystemInfo'))
        dict_copy['lastModifiedBy'] = _LastModifiedBy(**dict_copy.pop('lastModifiedBy'))
        dict_copy['shared'] = _Shared(**dict_copy.pop('shared'))

        return SharedDriveItem(**dict_copy)

    @staticmethod
    def get(share_url: str, token: str) -> 'SharedDriveItem':
        """
        Given a share URL and a token, returns the file name and the file data.
        """
        base64_value = base64.b64encode(share_url.encode()).decode()
        encoded_url = base64_value.rstrip('=').replace('/', '_').replace('+', '-')
        filedata_endpoint = f'https://graph.microsoft.com/v1.0/shares/u!{encoded_url}/driveItem'
        file_metadata = call_graph_api(filedata_endpoint, token)
        shared_drive_item = SharedDriveItem.from_dict(file_metadata.json())
        content_endpoint = f'https://graph.microsoft.com/v1.0/shares/u!{encoded_url}/driveItem/content'
        file_data = call_graph_api(content_endpoint, token)
        shared_drive_item._file_name = shared_drive_item.name
        shared_drive_item._file_bytes = file_data.content
        return shared_drive_item


    def to_df(self) -> pd.DataFrame:
        """
        A niche function to convert the SharedDriveItem to a DataFrame for files
        that happen to be dataframe-like; typically CSV or XLSX files.
        """
        if self._file_bytes is None:
            raise ValueError('File data is not loaded.')
        if self.name.endswith('.xlsx'):
            return pd.read_excel(io.BytesIO(self._file_bytes))
        elif self.name.endswith('.csv'):
            return pd.read_csv(io.BytesIO(self._file_bytes))
        else:
            raise ValueError('File is not dataframe-like.')


@dataclass
class _ContentType:
    def __init__(self, id, name):
        self.id = id
        self.name = name

@dataclass
class Item:
    eTag: str
    createdDateTime: str
    id: str
    lastModifiedDateTime: str
    webUrl: str
    createdBy: _User
    lastModifiedBy: _User
    parentReference: _ParentReference
    contentType: _ContentType
    fields_odata_context: str
    fields: dict

    @staticmethod
    def from_dict(data: dict):
        """
        Converts a dictionary to an Item object and fixes invalid keys
        and creates nested objects.
        """
        formatted_dict = data.copy()
        # converts invalid keys to valid keys
        formatted_dict['eTag'] = formatted_dict.pop('@odata.etag')
        formatted_dict['createdBy'] = _User(**formatted_dict['createdBy']['user'])
        formatted_dict['lastModifiedBy'] = _User(**formatted_dict['lastModifiedBy']['user'])
        formatted_dict['parentReference'] = _ParentReference(**formatted_dict['parentReference'])
        formatted_dict['contentType'] = _ContentType(**formatted_dict['contentType'])
        formatted_dict['fields_odata_context'] = formatted_dict.pop('fields@odata.context')
        return Item(**formatted_dict)


class ItemList:
    def __init__(self, context, items: list[Item], columns: list[str] | None = None):
        self.context = context
        self.items = items
        self.columns = columns

    @staticmethod
    def from_dict(data: dict):
        return ItemList(
            context=data["@odata.context"],
            items=[Item.from_dict(item) for item in data['value']]
        )

    @staticmethod
    def get(
            site_id: str,
            list_id: str,
            columns: list[str],
            token: str
        ) -> 'ItemList':
        """
        Given a site ID, list ID, and a token, returns an ItemList object.
        """
        endpoint = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields($select={",".join(columns)})'
        response = call_graph_api(endpoint, token)
        item_list = ItemList.from_dict(response.json())
        item_list.columns = columns
        return item_list

    def to_df(self, missing_column_as: str | None = None) -> pd.DataFrame:
        """
        Converts the ItemList to a DataFrame.
        """
        if self.columns is None:
            raise ValueError('Columns must be set before converting to DataFrame.')
        items_df = pd.DataFrame(columns=self.columns)
        for item in self.items:
            for column in self.columns:
                if column not in item.fields:
                    items_df.loc[item.id, column] = missing_column_as
                    continue
                items_df.loc[item.id, column] = item.fields[column]
        return items_df


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
        logging.info("No suitable token exists in cache. Let's get a new one from AAD.")
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


def call_graph_api(endpoint: str, token: str):
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


class AppOnlyEntity(EntityBase):
    """
    Entity for App-Only flow. This is recommended for server-to-server communication
    however is limited to scope-based permissions and broad access such as File.Read.All
    which may be unnecessary for reading a few files. Resource Owner Password Credential
    (ROPC) flow is another option for accessing single shared files.
    """
    def __init__(
            self, module_idk: str, description: str,
            client_id: str, client_secret: str, authority: str
        ):
        super().__init__(
            module_idk=module_idk,
            description=description,
            user_name='',
            password=''
        )
        self.client_id = client_id
        self.client_secret = client_secret
        self.authority = authority

    def get_token(self):
        return get_msal_token_app_only_login(
            self.client_id, self.client_secret, self.authority
        )


class ResourceOwnerEntity(EntityBase):
    """
    Entity for Resource Owner Password Credential (ROPC) flow.
    NOTE: The ROPC flow requires a username and password and NO MFA enabled.
    WARNING: This is not recommended by Microsoft and should only be used
    with limited permissions on the account and a very long and complex password.
    """
    def __init__(
            self, module_idk: str, description: str,
            client_id: str, authority: str, username: str, password: str
        ):
        super().__init__(
            module_idk=module_idk,
            description=description,
            user_name=username,
            password=password
        )
        self.client_id = client_id
        self.authority = authority

    def get_token(self):
        return get_msal_token_resource_owner_login(
            self.client_id, self.authority, self.user_name, self.password
        )


@dataclass
class GraphApiSharedXlsxOrCsvSource(SourceBase):
    """
    Retrieves a DataFrame from a shared URL of a CSV or XLSX file.
    """
    data_entity: AppOnlyEntity | ResourceOwnerEntity
    shared_url: str | None = None

    @module_function
    def get(self, share_url: str | None = None) -> pd.DataFrame:
        """
        Returns a DataFrame from a shared URL of a CSV or XLSX file.
        #### Parameters
        - share_url: str | None = None
            - The shared URL of the file, uses the shared_url of the source if not provided.
        """
        if share_url:
            cur_url = share_url
        elif self.shared_url:
            cur_url = self.shared_url
        else:
            raise ValueError('No shared URL provided.')
        sdi = SharedDriveItem.get(cur_url, self.data_entity.get_token())
        return sdi.to_df()


@dataclass
class GraphApiListSource(SourceBase):
    """
    Retrieves a DataFrame from a SharePoint list.
    """
    data_entity: AppOnlyEntity | ResourceOwnerEntity
    site_id: str
    list_id: str
    columns: list[str]

    @module_function
    def get(self, columns: list[str] | None = None) -> pd.DataFrame:
        """
        Returns a DataFrame from a SharePoint list. Optionally can override the columns.
        #### Parameters
        - columns: list[str] | None = None
            - The columns to retrieve from the list, uses the columns of the source if not provided.
        """
        item_list = ItemList.get(self.site_id, self.list_id, self.columns, self.data_entity.get_token())
        return item_list.to_df()