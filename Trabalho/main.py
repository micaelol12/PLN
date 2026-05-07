import asyncio
import nltk
import json
import pandas as pd

from nltk.corpus import stopwords
from sklearn.feature_extraction.text import CountVectorizer

from get_data import baixar_dados
from storage.database import Database
from utils.time_decorator import medir_tempo
from text import pre_processar_texto

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2/"
DATABASE_PATH = "data.db"


try:
    stopwords.words('portuguese')
except LookupError:
    nltk.download('stopwords')
nltk.download('rslp')


async def main():
    database = Database(DATABASE_PATH)

    await baixar_dados(database, BASE_URL)
    # await pre_processamento_discurso(database)
    #processar_dados_bow(database)


def processar_dados_bow(database):
    discursos = database.get_discursos(10)
    
    
    corpus = [json.loads(d["sumario_stemizado"]) for d in discursos]
    
    bow_vec = CountVectorizer(
        tokenizer=lambda x: x,   # não processa, usa direto
        preprocessor=None,
        lowercase=False
    )
    
    X_bow = bow_vec.fit_transform(corpus)
    df_bow = pd.DataFrame(X_bow.toarray(), columns=bow_vec.get_feature_names_out())
    print("Matriz Documento-Termo (BoW):")
    print(df_bow)
    
    
    
    


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
