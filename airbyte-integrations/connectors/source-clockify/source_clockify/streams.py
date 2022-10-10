
from abc import ABC
from typing import Any, Iterable, Mapping, MutableMapping, Optional, List, Type

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream
from requests.auth import AuthBase

# from airbyte_cdk.sources.streams.http.requests_native_auth.token import TokenAuthenticator

import requests


# Basic full refresh stream
class ClockifyStream(HttpStream, ABC):
    url_base = "https://api.clockify.me/api/v1/"
    page_size = 3
    page = 1
    primary_key = None

    def __init__(self, workspaceId: str, **kwargs):
        super().__init__(**kwargs)
        self.workspaceId = workspaceId


    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        next_page = response.json()
        self.page = self.page + 1
        if next_page:
            return {"page": self.page}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {
            "page-size": self.page_size,
            }

        if next_page_token:
            params.update(next_page_token)

        return params

    def parse_response(self,
                       response: requests.Response, **kwargs
                       ) -> Iterable[Mapping]:

        data = response.json()
        yield from data
                
    # def get_json_schema(self):
    #     schema = super().get_json_schema()
    #     schema['dynamically_determined_property'] = "property"
    #     return schema


class Members(ClockifyStream):
    def path(self, **kwargs) -> str:
        return f"workspaces/{self.workspaceId}/users"


class Projects(ClockifyStream):
    def path(self, **kwargs) -> str:
        return f"workspaces/{self.workspaceId}/projects"


class Entries(ClockifyStream):
    def __init__(self, start, end: str, **kwargs):
        super().__init__(**kwargs)
        self.start = start + "T00:00:00.441630Z" if start else None
        self.end = end + "T23:59:59.441630Z" if end else None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {
            "page-size": self.page_size,
            "start": self.start,
            "end": self.end,
            }

        if next_page_token:
            params.update(next_page_token)

        return params


    def stream_slices(
        self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        members_stream = Members(authenticator=self._session.auth, workspaceId=self.workspaceId)
        for member in members_stream.read_records(sync_mode=SyncMode.full_refresh):
            yield {"member_id": member["id"]}


    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        member_id = stream_slice["member_id"]
        return f"workspaces/{self.workspaceId}/user/{member_id}/time-entries"
