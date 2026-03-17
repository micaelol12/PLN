import asyncio
from storage.database import Database
from api.client import ApiClient
from utils.time_decorator import medir_tempo
from pipeline.producer import produzir_ids
from pipeline.worker import worker
from pipeline.retry_worker import retry_worker

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2/"


async def main():
    api_client = ApiClient(BASE_URL)
    database = Database()
    retry_queue = asyncio.Queue()
    ids = database.get_ids()

    if len(ids) == 0:
        ids = procuraIds(api_client, database)

    # await buscar_deputados(ids, retry_queue, api_client, database)
    # await buscar_discursos(ids, retry_queue, api_client, database)
    # await buscar_preoposicoes(ids, retry_queue, api_client, database)
    await buscar_preposicoes_detalhes(retry_queue, api_client, database)

    await api_client.close()


@medir_tempo("buscar_deputados")
async def buscar_deputados(ids, retry_queue, api_client, database):
    ids_salvos = database.get_deputados_ids()
    ids_nao_repetidos = [id for id in ids if id not in ids_salvos]
    
    if len(ids_nao_repetidos) == 0:
        return
    
    queue = await getQueue(ids_nao_repetidos)
    workers = [
        asyncio.create_task(
            worker(queue, retry_queue, api_client.get_deputados,
                   database.save_deputado)
        )
        for _ in range(10)
    ]

    await queue.join()

    await retry_worker(retry_queue, api_client, database)

    for w in workers:
        w.cancel()


@medir_tempo("buscar_discursos")
async def buscar_discursos(ids, retry_queue, api_client, database):
    ids_salvos = database.get_discursos_ids()
    ids_nao_repetidos = [id for id in ids if id not in ids_salvos]
    
    if len(ids_nao_repetidos) == 0:
        return

    queue = await getQueue(ids_nao_repetidos)
    workers = [
        asyncio.create_task(
            worker(queue, retry_queue, api_client.get_discursos,
                   database.save_discursos)
        )
        for _ in range(10)
    ]

    await queue.join()

    await retry_worker(retry_queue, api_client, database)

    for w in workers:
        w.cancel()


@medir_tempo("buscar_preoposicoes")
async def buscar_preoposicoes(ids, retry_queue, api_client, database):
    ids_salvos = database.get_preposicoes_ids()
    ids_nao_repetidos = [id for id in ids if id not in ids_salvos]
    
    if len(ids_nao_repetidos) == 0:
        return
    
    queue = await getQueue(ids_nao_repetidos)
    workers = [
        asyncio.create_task(
            worker(queue, retry_queue, api_client.get_preoposicoes,
                   database.save_preposicoes)
        )
        for _ in range(10)
    ]

    await queue.join()

    await retry_worker(retry_queue, api_client, database)

    for w in workers:
        w.cancel()


@medir_tempo("buscar_preposicoes_detalhes")
async def buscar_preposicoes_detalhes(retry_queue, api_client, database):
    ids = database.get_preposicoes_ids()
    ids_salvos = database.get_preposicoes_detalhes_ids()
    ids_nao_repetidos = [id for id in ids if id not in ids_salvos]
    
    if len(ids_nao_repetidos) == 0:
        return
    
    queue = await getQueue(ids_nao_repetidos)
    workers = [
        asyncio.create_task(
            worker(queue, retry_queue, api_client.buscar_preposicao_detalhes,
                   database.save_preoposicao_detalhe)
        )
        for _ in range(10)
    ]

    await queue.join()

    await retry_worker(retry_queue, api_client, database)

    for w in workers:
        w.cancel()


async def getQueue(ids):
    queue = asyncio.Queue()

    for i in ids:
        await queue.put(i)

    return queue


async def procuraIds(api_client, database):
    ids = await produzir_ids(api_client)
    database.save_ids(ids)
    return ids

asyncio.run(main())
