import asyncio

from get_data import baixar_dados
from storage.database import Database
from utils.time_decorator import medir_tempo
from text import pre_processar_texto

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2/"
DATABASE_PATH = "data.db"


async def main():
    database = Database(DATABASE_PATH)

    # await baixar_dados(database, BASE_URL)
    # await pre_processamento_discurso(database)
    

@medir_tempo("pre_processamento_discurso")
async def pre_processamento_discurso(database):
    discursos = database.get_discursos()

    for discurso in discursos:
        id = discurso["id"]
        id_discurso = discurso["id_discurso"]
        transcricao = discurso["transcricao"]
        sumario = discurso["sumario"]

        if sumario and len(sumario) > 0:
            database.salvar_preprocessamento_discurso(
                "sumario", id, id_discurso, *pre_processar_texto(sumario))

        database.salvar_preprocessamento_discurso(
            "transcricao", id, id_discurso, *pre_processar_texto(transcricao))


asyncio.run(main())
