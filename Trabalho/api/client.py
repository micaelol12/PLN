from datetime import timedelta, datetime

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from utils.pdf import baixar_e_extrair_pdf

class ApiClient:
    def __init__(self,base_url: str):
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=30)

    async def produzir_ids(self):
        ids = []
        
        r = await self.client.get(f"{self.base_url}deputados?pagina=1&ordem=ASC&ordenarPor=id")
        data = r.json()

        ids.extend([i["id"] for i in data['dados']])

        return ids
        
    
    async def get_preposicao_data(self, item_id: int):
        response = await self.client.get(f"{self.base_url}proposicoes/{item_id}")
        response.raise_for_status()
        json = response.json()

        return json
    
    async def buscar_preposicao_detalhes(self, id_preposicao):
        data = await self.get_preposicao_data(id_preposicao)
        url = data["dados"]["urlInteiroTeor"]

        if url:
            texto = await baixar_e_extrair_pdf(self.client, url)
            data["dados"]["texto_pdf"] = texto

        return data
    
    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def get_deputados(self, item_id: int):
        response = await self.client.get(f"{self.base_url}deputados/{item_id}")
        response.raise_for_status()
        json = response.json()
        json["id"] = item_id

        return json
    
    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def get_discursos(self, item_id: int):
        response = await self.client.get(f"{self.base_url}deputados/{item_id}/discursos?dataInicio=2018-01-01&dataFim=2022-01-01&ordenarPor=dataHoraInicio&ordem=DESC&pagina=1")
        response.raise_for_status()
        json = response.json()
        json["id"] = item_id

        return json
    
    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def get_preoposicoes(self, item_id: int):
        response = await self.client.get(f"{self.base_url}proposicoes?idDeputadoAutor={item_id}&codTipo=139,140,136,190,187,254&pagina=1&ordem=ASC&ordenarPor=id")
        response.raise_for_status()
        json = response.json()
        json["id"] = item_id

        return json
    
    async def close(self):
        await self.client.aclose()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def get_discursos_varios_anos(self, item_id: int):
        inicio = datetime(2018, 1, 1)
        fim =  datetime(2026, 1, 1)

        resultados = []

        atual_inicio = inicio

        while atual_inicio <= fim:
            # limite de 4 anos por requisição
            atual_fim = min(
                atual_inicio.replace(year=atual_inicio.year + 4),
                fim
            )

            url = (
                f"{self.base_url}deputados/{item_id}/discursos"
                f"?dataInicio={atual_inicio.strftime('%Y-%m-%d')}"
                f"&dataFim={atual_fim.strftime('%Y-%m-%d')}"
                f"&ordenarPor=dataHoraInicio"
                f"&ordem=DESC"
                f"&pagina=1"
            )

            response = await self.client.get(url)
            response.raise_for_status()
            data = response.json()

            # junta os dados (geralmente vem em data["dados"])
            resultados.extend(data.get("dados", []))

            # avança para o próximo intervalo (evita sobreposição)
            atual_inicio = atual_fim + timedelta(days=1)

        return {
            "id": item_id,
            "dados": resultados
        }