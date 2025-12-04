
from __future__ import annotations

import io
import json
from dataclasses import dataclass
from datetime import timedelta as td
from time import sleep
import traceback
import time

import pandas as pd
import requests
from requests import utils as requests_utils
from requests.cookies import RequestsCookieJar
from sqlalchemy import Column, DateTime, Integer, Numeric, String

from orcha.common.modules.mssql import MssqlEntity
from orcha.common.modules.web import RestEntity, RestSource
from orcha.core.module_base import (
    DatabaseSink,
    DatabaseSource,
    PythonEntity,
    PythonSource,
)
from orcha.core.tasks import RunItem, TaskItem

#######################################################################
# Dataclasses
#######################################################################

@dataclass
class TeamMemberRecords:
    sitepass_id: int
    username: str
    first_name: str
    last_name: str
    primary_email: str
    primary_mobile: str | None
    tracking_status_name: str
    emergency_response_roles: str
    role_title: str


#######################################################################
# General Functions
#######################################################################

def _inx_sp_request(api_key: str, url: str):
    r = requests.get(
        url=url,
        headers={'x-api-key': api_key}
    )

    if r.status_code != 200:
        raise Exception(f'API call failed, non 200 response: {r.status_code} ({r.text})')

    return r

def _inx_sp_api_call(api_key: str, base_url: str, endpoint: str, next_index = 0, query_params = {}) -> list[dict] | dict | None:
    if endpoint[0] != '/':
        raise Exception('Endpoint must start with a leading /')
    # Limit to 100 records per page to avoid timeouts
    # that were happening with 200 records per page
    query_string = '&'.join([f'{k}={v}' for k, v in query_params.items()])
    if query_string != '':
        query_string = f'{query_string}&'
    url = f'{base_url}{endpoint}?{query_string}limit=100&nextIndex={next_index}'
    r = _inx_sp_request(api_key, url)

    # Sitepass likes to throw 504s, so if we get one, wait 5 seconds then try again
    if r.status_code == 504:
        sleep(5)
        r = _inx_sp_request(api_key, url)

    if r.status_code != 200:
        raise Exception(f'API call failed, non 200 response: {r.status_code} ({r.text})')

    data = r.json()['data']
    metadata = r.json().get('metadata', None)
    if metadata is not None:
        if metadata['currentPage'] < metadata['totalPages']:
            next_page = _inx_sp_api_call(api_key, url, metadata['nextIndex'], query_params)
            data += next_page

    return data


def _get_sitepass_creds(rest_entity: RestEntity) -> RequestsCookieJar:
    url = f'{rest_entity.url}/login'
    payload = json.dumps({
        'username': rest_entity.user_name,
        'password': rest_entity.password
    })
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9,en-AU;q=0.8",
        "content-type": "application/json",
        "priority": "u=1, i",
        "sec-ch-ua": "\"Chromium\";v=\"124\", \"Microsoft Edge\";v=\"124\", \"Not-A.Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin"
    }

    auth_creds = {}
    r = requests.post(url, headers=headers, data=payload, allow_redirects=True)

    if len(r.cookies) > 0:
        for c in r.cookies:
            auth_creds[c.name] = c.value
    else:
        for pr in r.history:
            if len(pr.cookies) > 0:
                for c in pr.cookies:
                    auth_creds[c.name] = c.value

    return requests_utils.cookiejar_from_dict(auth_creds)


def _get_worker_details(worker_id: str, api_key: str, base_url: str) -> pd.DataFrame:

    @dataclass
    class WorkerRaw():
        worker_id: str
        user_name: str
        first_name: str
        last_name: str
        workflow_status: str
        workflow_steps: list[dict]
        raw_json: dict

    @dataclass
    class Resources():
        resource_id: str
        file_name: str
        file_size_in_kb: int
        file_type: str
        created: int
        url: str
        uploaded_by_id: str
        uploaded_by_first_name: str
        uploaded_by_last_name: str
        uploaded_by_email: str

        def as_dict(self):
            return {
                'resource_id': self.resource_id,
                'file_name': self.file_name,
                'file_size_in_kb': self.file_size_in_kb,
                'file_type': self.file_type,
                'created': self.created,
                'url': self.url,
                'uploaded_by_id': self.uploaded_by_id,
                'uploaded_by_first_name': self.uploaded_by_first_name,
                'uploaded_by_last_name': self.uploaded_by_last_name,
                'uploaded_by_email': self.uploaded_by_email
            }


    @dataclass
    class FieldItem():
        field_id: str
        name: str
        type: str
        value: str | None
        resources: list[Resources]

    @dataclass
    class SectionItem():
        section_id: str
        model_section_id: str
        name: str
        status: str | None
        fields: list[FieldItem]

    @dataclass
    class StepItem():
        step_id: str
        name: str
        status: str
        sections: list[SectionItem]

    @dataclass
    class WorkerItem():
        worker_id: str
        user_name: str
        first_name: str
        last_name: str
        workflow_status: str
        steps: list[StepItem]

        def returnable_dict(self):
            # return a flattened dict of the worker, all steps, all sections, all fields
            ret_val: list[dict] = []
            for step in self.steps:
                for section in step.sections:
                    for field in section.fields:
                        ret_val.append({
                            'worker_id': self.worker_id,
                            'user_name': self.user_name,
                            'first_name': self.first_name,
                            'last_name': self.last_name,
                            'workflow_status': self.workflow_status,
                            'step_id': step.step_id,
                            'step_name': step.name,
                            'step_status': step.status,
                            'section_id': section.section_id,
                            'model_section_id': section.model_section_id,
                            'section_name': section.name,
                            'section_status': section.status,
                            'field_id': field.field_id,
                            'field_name': field.name,
                            'field_type': field.type,
                            'field_value': field.value,
                            # return the resources as a list of dicts
                            # as all data in a resource should be searalizable
                            'field_resources': [res.as_dict() for res in field.resources]
                        })
            return ret_val

    worker = _inx_sp_api_call(
        api_key=api_key,
        base_url=base_url,
        endpoint=f'/workers/{worker_id}'
    )

    # dump the worker to a file for debugging
    with open(f'worker_{worker_id}.json', 'w') as f:
        json.dump(worker, f, indent=4)

    if worker is None:
        raise Exception('API Call failed')
    if not isinstance(worker, dict):
        raise Exception('Worker must be a dict')
    if isinstance(worker, list):
        worker = worker[0]

    username = worker.get('primaryEmail', {}).get('contact', None)
    if username is None:
        username = worker.get('username', None)

    if username is None:
        raise ValueError(f'Worker {worker_id} has no username')

    worker = WorkerRaw(
        worker_id=worker['id'],
        user_name=username,
        first_name=worker['firstName'],
        last_name=worker['lastName'],
        workflow_status=worker.get('tracking', {}).get('statusName', None),
        workflow_steps=worker.get('tracking', {}).get('workflowSteps', []),
        raw_json=worker
    )

    worker_item = WorkerItem(
        worker_id=worker.worker_id,
        user_name=worker.user_name,
        first_name=worker.first_name,
        last_name=worker.last_name,
        workflow_status=worker.workflow_status,
        steps=[]
    )

    if len(worker.workflow_steps) == 0:
        return pd.DataFrame(worker_item.returnable_dict())

    for record in worker.workflow_steps:
        step = StepItem(
            step_id=record['stepId'],
            name=record['displayTitle'],
            status=record['verificationStatus'],
            sections=[]
        )

        form = record.get('form')
        if not form:
            continue

        structure_sections = form['structure']['sections']
        model_sections = form['model'].get('sections', {})

        for section_key, sections in model_sections.items():
            for model_section in sections:
                # Example 'Worker Assessment - Pending' form
                # For some reason the model has a key that isn't
                # in the structure, so we need to skip it
                if section_key not in structure_sections:
                    continue
                structure_section = structure_sections[section_key]
                structure_fields = structure_section['fields']
                section = SectionItem(
                    section_id=section_key,
                    model_section_id=model_section['id'],
                    name=structure_section['title'],
                    status=model_section['status'] if 'status' in model_section else None,
                    fields=[]
                )
                # Explanation due to complexity.
                # Iterate over all model (value-containing) fields,
                for field in model_section['fields']:
                    for s_field in structure_fields:
                        # Get matching structure for that model field
                        if s_field['fieldKey'] == field['fieldKey']:
                            resources: list[Resources] = []
                            # Get the value IF we have a value
                            field_value = field['values'][0] if len(field['values']) > 0 else None
                            # Handle different field types as they have different structures
                            # e.g. this one needs to lookup the option value as the
                            # model value only contains as ID
                            if s_field['type'] in ['RADIO', 'SELECT_LIST']:
                                for option in s_field['options']:
                                    if str(option['optionKey']) == str(field_value):
                                        field_value = option['value']
                            if s_field['type'] == 'FILE_UPLOAD':
                                field_value = None
                                field_resources = field.get('resources', [])
                                for res in field_resources:
                                    uploaded_by = res.get('uploadedByAccount', {})
                                    resource = Resources(
                                        resource_id=res['id'],
                                        file_name=res['fileName'],
                                        file_size_in_kb=res['fileSizeInKB'],
                                        file_type=res['fileType'],
                                        created=res['created'],
                                        url=res['url'],
                                        uploaded_by_id=uploaded_by.get('id', None),
                                        uploaded_by_first_name=uploaded_by.get('firstName', None),
                                        uploaded_by_last_name=uploaded_by.get('lastName', None),
                                        uploaded_by_email=uploaded_by.get('primaryEmail', {}).get('contact', None)
                                    )
                                    resources.append(resource)
                            section.fields.append(FieldItem(
                                field_id=field['fieldKey'],
                                name=s_field['label'],
                                type=s_field['type'],
                                value=field_value,
                                resources=resources
                            ))
                            break
                step.sections.append(section)

        for structure_section in structure_sections.values():
            # if we don't have the section in the model, we need to add it with empty fields
            if str(structure_section['sectionKey']) not in model_sections:
                section = SectionItem(
                    section_id=structure_section['sectionKey'],
                    model_section_id='0',
                    name=structure_section['title'],
                    status=None,
                    fields=[]
                )
                for field in structure_section['fields']:
                    section.fields.append(FieldItem(
                        field_id=field['fieldKey'],
                        name=field['label'],
                        type=field['type'],
                        value=None,
                        resources=[]
                    ))
                step.sections.append(section)

        worker_item.steps.append(step)

    return pd.DataFrame(worker_item.returnable_dict())


def _get_workers(entity: SitepassApiEntity, run_item: RunItem | None = None):
    """
    Get all workers from the Sitepass API
    args:
        entity: PythonEntity - passed by the PythonSource
        run_item: RunItem - custom arg passed via kwargs from get()
    """
    # Using API Key set via environment variable
    # rather than from the entity
    team = _inx_sp_api_call(
        api_key=entity.api_key,
        base_url=entity.url,
        endpoint='/workers'
    )
    if team is None:
        raise Exception('API Call failed')

    data = pd.DataFrame(columns=[
        'worker_id', 'user_name', 'first_name', 'last_name', 'workflow_status',
        'step_id', 'step_name', 'step_status', 'section_id', 'section_name',
        'field_id', 'field_name', 'field_type', 'field_value', 'field_resources',
        'model_section_id', 'section_status'
    ])

    count = 0
    error_count = 0
    for worker in team:
        count += 1
        try:
            sleep(0.1)
            worker_data = _get_worker_details(worker['id'], api_key=entity.api_key, base_url=entity.url)
            data = pd.concat([data, worker_data])
        except Exception as e:
            if error_count > 5:
                new_exception = Exception('Sitepass worker details error limit reached')
                raise new_exception from e
            error_count += 1
            print(f'Error processing worker {worker.get("id")}: {type(e).__name__} - {e}')
            print(traceback.format_exc())
            disconnect_str = 'disconnected'
            if disconnect_str in str(e):
                if run_item:
                    run_item.set_status(
                        status='warn',
                        output={
                            f'worker_id: {worker.get("id")}': 'Worker disconnected'
                        }
                    )
            else:
                error_count += 1
                if run_item:
                    run_item.set_status(
                        status='warn',
                        output={
                            f'worker_id: {worker.get("id")}': str(e)
                        }
                    )

    if run_item:
        run_item.set_output(
            output={
                'worker_count': count,
                'error_count': error_count,
            },
            merge=True
        )

    return data.reset_index(drop=True)


def _parse_team_member_record(data: dict) -> TeamMemberRecords:
    # Basic fields
    sitepass_id = data.get("id")
    username = data.get("username")
    first_name = data.get("firstName")
    last_name = data.get("lastName")

    # Primary email
    primary_email = None
    pe = data.get("primaryEmail")
    if pe:
        primary_email = pe.get("contact")

    # Primary mobile, include country code if provided
    primary_mobile = None
    pm = data.get("primaryMobile")
    if pm:
        contact = pm.get("contact")
        extra = pm.get("contactExtra")
        if contact:
            if extra:
                # format like +61xxxxxxxxx
                prefix = ("+" + extra) if not extra.startswith("+") else extra
                # avoid duplicating + if contact already contains it
                primary_mobile = contact if contact.startswith("+") else f"{prefix}{contact}"
            else:
                primary_mobile = contact

    # Tracking status name
    tracking_status_name = None
    tracking = data.get("tracking")
    if tracking:
        tracking_status_name = tracking.get("statusName")

    # Emergency response roles: find category with "emergency" in title (case-insensitive)
    emergency_titles = []
    for cat in data.get("categories", []):
        title = (cat.get("title") or "").lower()
        if "emergency" in title:
            for sc in cat.get("subcategories", []):
                t = sc.get("title")
                if t:
                    emergency_titles.append(t)
    emergency_response_roles = ", ".join(emergency_titles)

    # Role title
    role_title = None
    role = data.get("role")
    if role:
        role_title = role.get("title")

    return TeamMemberRecords(
        sitepass_id=sitepass_id or 0,
        username=username or "None found",
        first_name=first_name or "None found",
        last_name=last_name or "None found",
        primary_email=primary_email or "None found",
        primary_mobile=primary_mobile,
        tracking_status_name=tracking_status_name or "None found",
        emergency_response_roles=emergency_response_roles,
        role_title=role_title or "None found",
    )


def _get_team_members(entity: SitepassApiEntity) -> pd.DataFrame:
    team = _inx_sp_api_call(
        api_key=entity.api_key,
        base_url=entity.url,
        endpoint='/team'
    )
    if team is None:
        raise Exception('API Call failed')

    records = []
    for member in team:
        try:
            record = _parse_team_member_record(member)
            records.append(record)
        except Exception as e:
            print(f"Error parsing team member {member.get('id')}: {e}")

    df = pd.DataFrame(records)
    return df


class SitepassScrapeEntity(RestEntity):
    def __init__(
            self,
            module_idk: str,
            description: str,
            base_url: str,
            user_name: str,
            password: str,
            headers: dict | None = None,
        ):

        super().__init__(
            module_idk=module_idk,
            description=description,
            url=base_url,
            headers=headers or {
                "accept": "application/json, text/plain, */*",
                "accept-language": "en-US,en;q=0.9",
                "cache-control": "no-cache",
                "content-type": "application/json;charset=UTF-8",
                "pragma": "no-cache",
                "sec-ch-ua": "\"Chromium\";v=\"124\", \"Microsoft Edge\";v=\"124\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin"
            },
            create_cookies=_get_sitepass_creds,
            user_name=user_name,
            password=password
        )


class SitepassApiEntity(RestEntity):
    def __init__(
            self,
            module_idk: str,
            description: str,
            api_key: str,
            base_url: str = 'https://api.app.mysitepass.com/external/v1',
            headers: dict | None = None,
        ):

        auth_headers = headers or {}
        auth_headers['x-api-key'] = api_key

        self.api_key = api_key

        super().__init__(
            module_idk=module_idk,
            description=description,
            url=base_url,
            headers=auth_headers,
            user_name='',
            password=''
        )


class SitepassWorderDetailsSource(PythonSource):
    def __init__(
            self,
            module_idk: str,
            description: str,
            api_entity: SitepassApiEntity,
        ):
        super().__init__(
            module_idk=module_idk,
            description=description,
            data_entity=api_entity,
            function=_get_workers
    )


class SitepassTeamMembersSource(PythonSource):
    def __init__(
            self,
            module_idk: str,
            description: str,
            api_entity: SitepassApiEntity,
        ):
        super().__init__(
            module_idk=module_idk,
            description=description,
            data_entity=api_entity,
            function=_get_team_members
    )


# sitepass_get_visits_source = RestSource(
#     module_idk='sitepass_get_visits_source',
#     description='Source for Sitepass Get Visits',
#     data_entity=sitepass_scrape_entity,
#     sub_path='vms/visits',
#     query_params=None,
#     request_type='POST',
#     postprocess=lambda response: pd.DataFrame(response.json()['data']),
#     request_data={
#         '$currentPage': 1,
#         'view': ['list'],
#         'limit': '1000',
#         'sortBy': 'checkIn.checkInEndTime',
#         'sortOrder': 'desc',
#         'checkInDate': {'dateRange':'LAST_12_MONTHS'},
#         'nextindex': 0
#     }
# )


class SitepassVisitsSource(RestSource):
    def __init__(
            self,
            module_idk: str,
            description: str,
            scrape_entity: SitepassScrapeEntity,
        ):
        super().__init__(
            module_idk=module_idk,
            description=description,
            data_entity=scrape_entity,
            sub_path='vms/visits',
            query_params=None,
            request_type='POST',
            postprocess=lambda response: pd.DataFrame(response.json()['data']),
            request_data={
                '$currentPage': 1,
                'view': ['list'],
                'limit': '1000',
                'sortBy': 'checkIn.checkInEndTime',
                'sortOrder': 'desc',
                'checkInDate': {'dateRange':'LAST_2_MONTHS'},
                'nextindex': 0
            }
    )