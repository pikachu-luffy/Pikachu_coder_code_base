import os
import aiohttp
import google
import google.oauth2.credentials
from aiohttp import ContentTypeError
from google.auth import compute_engine
import google.auth.transport.requests
from pydantic import RootModel, BaseModel
from typing import Optional
import logging
import json
import requests
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class PersonaFields(BaseModel):
    phone: Optional[str] = None
    email: Optional[str] = None


class MondelezIdPersona(BaseModel):
    # TODO Need to change as per Core design as mock service is having different variable
    mondelezId: Optional[str] = None
    normalized: Optional[PersonaFields] = None
    status: Optional[int] = None
    details: Optional[str | dict | list] = None


class PersonaRequestDto(RootModel):
    root: list[PersonaFields]


class IdServiceConnector:
    def __init__(self):
        self.id_service_url = "https://amea-id-service-dev-service-995688046143.asia-south1.run.app/v1"
        self.environment = "dev"
        self.credentials = None

    def get_id_token(self) -> str:
        if self.environment == 'local':
            return os.getenv('ID_TOKEN')
        if self.credentials is None or self.credentials.expired:
            request = google.auth.transport.requests.Request()
            credentials = compute_engine.IDTokenCredentials(
                request=request,
                target_audience=self.id_service_url,
                use_metadata_identity_endpoint=True
            )
            credentials.refresh(request)
            self.credentials = credentials
            return credentials.token
        else:
            return self.credentials.token

    async def get_mdlz_id(self, persona: [PersonaFields]) -> [MondelezIdPersona]:
        headers = {"Authorization": f"Bearer {self.get_id_token()}",
                   'content-type': 'application/json'}
        async with aiohttp.ClientSession(headers=headers) as session:
            market = "in"
            personas_root = PersonaRequestDto(persona)
            body = personas_root.model_dump(mode="json")
            url = self.id_service_url + f"/mondelezIds/{market}"

            async with session.post(url, json=body) as resp:
                try:
                    data = await resp.json()

                    return [MondelezIdPersona(**d) for d in data]
                except ContentTypeError:
                    if resp.status == 401:
                        logging.error('Unauthorized Token. Please update the token')
                        raise Exception("Unauthorized Token. Please update the token")
                except Exception as e:
                    logging.error(f"Error in ID service response: {e}")
                    raise e
                if resp.status == 404:
                    raise Exception(f"Response from ID service: {data} response status: {resp.status}")

        return []
