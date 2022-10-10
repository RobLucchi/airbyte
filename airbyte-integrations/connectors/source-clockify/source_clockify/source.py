from typing import Any, List, Mapping, Tuple

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth.token import TokenAuthenticator

from .streams import Members, Entries, Projects

import datetime

# Source
class SourceClockify(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            valid, message = self._check_start_end(config)
            
            if not valid:
                return valid, message

            workspace_stream = Members(
                authenticator=TokenAuthenticator(token=config["X-Api-Key"], auth_header="X-Api-Key", auth_method=""),
                workspaceId=config["workspaceId"],
            )
            next(workspace_stream.read_records(sync_mode=SyncMode.full_refresh))
            return True, None
        except Exception as e:
            return False, f"Please check that your API key and workspace name are entered correctly: {repr(e)}"

    def _check_start_end(self, config):
        s_lt_e = config.get('start') <= config.get('end') if config.get('start') and config.get('end') else True
        s_lt_t = config.get('start') <= str(datetime.date.today()) if config.get('start') else True
        e_lt_t = config.get('end') <= str(datetime.date.today()) if config.get('end') else True
        
        
        if s_lt_e and s_lt_t and e_lt_t:
            return True, None
        else:
            return False, "Please use valid start and end dates"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        authenticator = TokenAuthenticator(
            token=config["X-Api-Key"], 
            auth_header="X-Api-Key", 
            auth_method="")
        args = {"authenticator": authenticator,  "workspaceId": config["workspaceId"] }
        entries_args = {
                "authenticator": authenticator,  
                "workspaceId": config["workspaceId"],
                'start': config.get('start'),
                'end': config.get('end'),
                }
        
        return [
            Members(**args),
            Projects(**args),
            Entries(**entries_args),
            ]
                