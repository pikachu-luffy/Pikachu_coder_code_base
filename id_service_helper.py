# import time
# import asyncio
# import logging
# from typing import Dict, List
# from operators.id_service_connector import IdServiceConnector, PersonaFields, MondelezIdPersona

# async def fetch_mdlz_ids(phones: List[str]) -> Dict[str, str]:
#     connector = IdServiceConnector()
#     phone_to_mdlzid = {}
    
#     # Split phones into batches of 100 to avoid API limits
#     batch_size = 50
#     for i in range(0, len(phones), batch_size):  
#         batch = phones[i:i+batch_size]
#         persona_list = [PersonaFields(phone=p) for p in batch if p]
#         print(f"Processing batch {i//batch_size + 1} with {len(persona_list)} phones")
#         if persona_list:
#             try:
#                 results = await connector.get_mdlz_id(persona_list)
#                 for result in results:
#                     if result.mondelezId and result.normalized and result.normalized.phone:
#                         phone_to_mdlzid[result.normalized.phone] = result.mondelezId
#             except Exception as e:
#                 logging.error(f"Failed to fetch mdlzIds for batch {i//batch_size + 1}: {str(e)}")
    
#     return phone_to_mdlzid



import asyncio
import logging
from typing import Dict, List
from operators.id_service_connector import IdServiceConnector, PersonaFields, MondelezIdPersona
from asyncio import Semaphore


async def fetch_batch(connector: IdServiceConnector, batch: List[str], semaphore: Semaphore) -> Dict[str, str]:
    phone_to_mdlzid = {}
    persona_list = [PersonaFields(phone=p) for p in batch if p]

    async with semaphore:
        try:
            results = await connector.get_mdlz_id(persona_list)
            for result in results:
                if result.mondelezId and result.normalized and result.normalized.phone:
                    phone_to_mdlzid[result.normalized.phone] = result.mondelezId
        except Exception as e:
            logging.error(f"Error processing batch with {len(batch)} phones: {str(e)}")

    return phone_to_mdlzid


async def fetch_mdlz_ids_parallel(phones: List[str], concurrency: int = 5) -> Dict[str, str]:
    connector = IdServiceConnector()
    phone_to_mdlzid = {}
    semaphore = Semaphore(concurrency)
    batch_size = 100

    tasks = []
    for i in range(0, len(phones), batch_size):
        batch = phones[i:i + batch_size]
        tasks.append(fetch_batch(connector, batch, semaphore))

    results = await asyncio.gather(*tasks)
    for result in results:
        phone_to_mdlzid.update(result)

    return phone_to_mdlzid







import asyncio
import logging
from typing import Dict, List
from api.id_service_connector import IdServiceConnector, PersonaFields, MondelezIdPersona
from asyncio import Semaphore


async def fetch_batch(connector: IdServiceConnector, batch: List[str], semaphore: Semaphore) -> Dict[str, str]:
    phone_to_mdlzid = {}
    persona_list = [PersonaFields(phone=p) for p in batch if p]

    async with semaphore:
        try:
            results = await connector.get_mdlz_id(persona_list)
            for result in results:
                if result.mondelezId and result.normalized and result.normalized.phone:
                    phone_to_mdlzid[result.normalized.phone] = result.mondelezId
        except Exception as e:
            logging.error(f"Error processing batch with {len(batch)} phones: {str(e)}")

    return phone_to_mdlzid


async def fetch_mdlz_ids_parallel(phones: List[str], concurrency: int = 5) -> Dict[str, str]:
    connector = IdServiceConnector()
    phone_to_mdlzid = {}
    semaphore = Semaphore(concurrency)
    batch_size = 100

    tasks = []
    for i in range(0, len(phones), batch_size):
        batch = phones[i:i + batch_size]
        tasks.append(fetch_batch(connector, batch, semaphore))

    results = await asyncio.gather(*tasks)
    for result in results:
        phone_to_mdlzid.update(result)

    return phone_to_mdlzid

