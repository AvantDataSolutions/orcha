from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Generic, Literal, Mapping, TypeVar

import pandas as pd
import requests
from requests.cookies import RequestsCookieJar

from orcha.core.module_base import EntityBase, SourceBase, SinkBase, module_function


class RestEntity(EntityBase):
    """
    A REST entity that contains the url, headers, cookies and
    any other required information to make a rest call.
    This provides cookie and header creation functions
    to allow for dynamic headers and cookies based authentication.
    Username and password are optional and will be passed for
    basic authentication if provided.
    The create_headers and create_cookies functions will be passed
    the current RestEntity instance for using url, password, etc.
    """
    url: str
    headers: dict | None = None
    create_headers: Callable[[RestEntity], dict] | None = None
    cookies: RequestsCookieJar  | None = None
    create_cookies: Callable[[RestEntity], RequestsCookieJar ] | None = None

    def __init__(
            self, module_idk: str, description: str,
            url: str, headers: dict | None = None,
            create_headers: Callable[[RestEntity], dict] | None = None,
            cookies: RequestsCookieJar  | None = None,
            create_cookies: Callable[[RestEntity], RequestsCookieJar ] | None = None,
            # User and password are optional for convenience
            user_name: str = '',
            password: str = ''
        ):
        super().__init__(
            module_idk=module_idk,
            description=description,
            user_name=user_name,
            password=password,
        )
        self.url = url
        self.headers = headers
        self.create_headers = create_headers
        self.cookies = cookies
        self.create_cookies = create_cookies

T = TypeVar('T', bound=Mapping[str, Any])
T2 = TypeVar('T2', bound=Mapping[str, Any])

@dataclass
class RestSource(SourceBase, Generic[T, T2]):
    """
    A source that calls a rest endpoint and returns the response.
    """
    data_entity: RestEntity | None
    request_type: Literal['GET', 'POST', 'PUT', 'DELETE']
    request_data: dict | str | None = None
    sub_path: str | None = None
    query_params: dict | None = None
    postprocess: Callable[[requests.Response], pd.DataFrame] | None = None
    postprocess_kwargs: T2 | None = None

    @module_function
    def get(
            self,
            sub_path_override: str | None = None,
            request_data_override: dict | str | None = None,
            query_params_merge: dict | None = None,
            path_lookup_values: T | None = None,
            request_kwargs: dict[str, Any] = {},
            postprocess_kwargs_override: T2 | None = None
        ) -> pd.DataFrame:
        """
        Calls the rest endpoint and returns the response.
        Appends the sub_path to the url and adds the
        query_params to the url in the provided entity.
        Note: The query_params are not validated
        and any trailing / in the sub_path is not removed.
        postprocess: A function that takes the response
        and performs any required logic to convert it to
        a dataframe. If no postprocess function is provided
        then the response json is converted to a dataframe.
        sub_path/query_params: These are provided as a dynamic override
        for those set in the source.
        request_kwargs: kwargs are passed to the requests.request method.
        path_lookup_values: A dictionary of values to replace in the sub_path.
        """
        if sub_path_override is not None:
            sub_path = sub_path_override
        else:
            sub_path = self.sub_path

        if sub_path and path_lookup_values:
            try:
                sub_path = sub_path.format(**path_lookup_values)
            except Exception:
                for k, v in path_lookup_values.items():
                    sub_path = sub_path.replace(str(k), str(v))

        if self.query_params is not None:
            if query_params_merge is not None:
                query_params = self.query_params | query_params_merge
            else:
                query_params = self.query_params
        else:
            query_params = query_params_merge

        if request_data_override is not None:
            request_data = request_data_override
        else:
            request_data = self.request_data


        if self.data_entity is None:
            raise Exception('No data entity set for source')
        else:
            url_with_query = self.data_entity.url
            if sub_path is not None:
                if sub_path[0] != '/':
                    url_with_query += '/'
                url_with_query += f'{sub_path}'
            if query_params is not None:
                url_with_query += '?'
                for key, value in query_params.items():
                    url_with_query += f'{key}={value}&'
                # Remove the last '&'
                url_with_query = url_with_query[:-1]
            if self.data_entity.create_headers is not None:
                cur_headers = self.data_entity.create_headers(self.data_entity)
            elif self.data_entity.headers is not None:
                cur_headers = self.data_entity.headers
            else:
                cur_headers = {}
            if self.data_entity.create_cookies is not None:
                cur_cookies = self.data_entity.create_cookies(self.data_entity)
            elif self.data_entity.cookies is not None:
                cur_cookies = self.data_entity.cookies
            else:
                cur_cookies = None

            if isinstance(request_data, str):
                data = request_data
            elif isinstance(request_data, dict):
                data = json.dumps(request_data)
            else:
                data = None

            if self.data_entity.user_name and self.data_entity.password:
                _auth = (self.data_entity.user_name, self.data_entity.password)
            else:
                _auth = None

            response = requests.request(
                method=self.request_type,
                url=url_with_query,
                auth=_auth,
                headers=cur_headers,
                cookies=cur_cookies,
                data=data,
                **request_kwargs
            )

            if response.status_code != 200:
                raise Exception('\n'.join([
                    f'Response status code is not 200: {response.status_code}',
                    f'URL: {url_with_query}',
                    f'Response text: {response.text[:1000]}'
                ]))
            pp_kwargs = postprocess_kwargs_override or self.postprocess_kwargs or {}
            if self.postprocess is not None:
                return self.postprocess(response, **pp_kwargs)
            else:
                return pd.DataFrame(response.json())

@dataclass
class RestSink(SinkBase):
    data_entity: RestEntity | None
    request_type: Literal['GET', 'POST', 'PUT', 'DELETE']
    sub_path: str | None = None
    query_params: dict | None = None
    preprocess: Callable[[list | dict | str | pd.DataFrame], str] | None = None

    @module_function
    def save(
            self,
            request_data: list | dict | str | pd.DataFrame,
            sub_path_override: str | None = None,
            query_params_merge: dict | None = None,
            request_kwargs: dict[str, Any] = {},
            **kwargs
        ):
        """
        Calls the rest endpoint and returns the response.
        Appends the sub_path to the url and adds the
        query_params to the url in the provided entity.
        Note: The query_params are not validated
        and any trailing / in the sub_path is not removed.
        #### Args:
        - request_data: The data to be sent to the rest endpoint, if this
        is a dataframe then the preprocess function is used to convert
        it to
        - preprocess: A function that takes the provided dataframe
        and performs any required logic to convert it to
        a format that can be sent to the rest endpoint.

        """
        if sub_path_override is not None:
            sub_path = sub_path_override
        else:
            sub_path = self.sub_path

        if self.query_params is not None:
            if query_params_merge is not None:
                query_params = self.query_params | query_params_merge
            else:
                query_params = self.query_params
        else:
            if query_params_merge is not None:
                query_params = {} | query_params_merge
            else:
                query_params = None

        if self.data_entity is None:
            raise Exception('No data entity set for sink')
        else:
            url_with_query = self.data_entity.url
            if sub_path is not None:
                if sub_path[0] != '/':
                    url_with_query += '/'
                url_with_query += f'{sub_path}'
            if query_params is not None:
                url_with_query += '?'
                for key, value in query_params.items():
                    url_with_query += f'{key}={value}&'
                # Remove the last '&'
                url_with_query = url_with_query[:-1]
            if self.data_entity.create_headers is not None:
                cur_headers = self.data_entity.create_headers(self.data_entity)
            elif self.data_entity.headers is not None:
                cur_headers = self.data_entity.headers
            else:
                cur_headers = {}
            if self.data_entity.create_cookies is not None:
                cur_cookies = self.data_entity.create_cookies(self.data_entity)
            elif self.data_entity.cookies is not None:
                cur_cookies = self.data_entity.cookies
            else:
                cur_cookies = None

            if self.preprocess is not None:
                data = self.preprocess(request_data)
            elif isinstance(request_data, pd.DataFrame):
                raise Exception('A preprocess function is required for dataframes')
            elif isinstance(request_data, str):
                data = request_data
            elif isinstance(request_data, dict) or isinstance(request_data, list):
                data = json.dumps(request_data)
            else:
                Exception('Unable to convert request_data to a string')

            if self.data_entity.user_name and self.data_entity.password:
                _auth = (self.data_entity.user_name, self.data_entity.password)
            else:
                _auth = None

            response = requests.request(
                method=self.request_type,
                url=url_with_query,
                auth=_auth,
                headers=cur_headers,
                cookies=cur_cookies,
                data=data,
                **request_kwargs
            )

            if response.status_code != 200:
                raise Exception('\n'.join([
                    f'Response status code is not 200: {response.status_code}',
                    f'Response text: {response.text[:1000]}'
                ]))
            else:
                return response