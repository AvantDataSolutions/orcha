from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Literal

import pandas as pd
import requests
from requests.cookies import RequestsCookieJar

from orcha.core.module_base import EntityBase, ModuleBase, module_function


class RestEntity(EntityBase):
    url: str
    headers: dict | None = None
    create_headers: Callable[[], dict] | None = None
    cookies: RequestsCookieJar  | None = None
    create_cookies: Callable[[], RequestsCookieJar ] | None = None

    def __init__(
            self, module_idk: str, description: str,
            url: str, headers: dict | None = None,
            create_headers: Callable[[], dict] | None = None,
            cookies: RequestsCookieJar  | None = None,
            create_cookies: Callable[[], RequestsCookieJar ] | None = None,
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


@dataclass
class RestSource(ModuleBase):
    data_entity: RestEntity | None
    request_type: Literal['GET', 'POST', 'PUT', 'DELETE']
    request_data: dict | str | None = None
    sub_path: str | None = None
    query_params: dict | None = None
    postprocess: Callable[[requests.Response], pd.DataFrame] | None = None

    @module_function
    def get(
            self,
            sub_path: str | None = None,
            query_params: dict | None = None,
            request_kwargs: dict[str, Any] = {},
            **kwargs
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
        """
        sub_path = sub_path if sub_path is not None else self.sub_path
        query_params = query_params if query_params is not None else self.query_params

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
                cur_headers = self.data_entity.create_headers()
            elif self.data_entity.headers is not None:
                cur_headers = self.data_entity.headers
            else:
                cur_headers = {}
            if self.data_entity.create_cookies is not None:
                cur_cookies = self.data_entity.create_cookies()
            elif self.data_entity.cookies is not None:
                cur_cookies = self.data_entity.cookies
            else:
                cur_cookies = None

            if isinstance(self.request_data, str):
                data = self.request_data
            elif isinstance(self.request_data, dict):
                data = json.dumps(self.request_data)
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
                raise Exception(f'Response status code is not 200: {response.status_code}')
            if self.postprocess is not None:
                return self.postprocess(response)
            else:
                return pd.DataFrame(response.json())