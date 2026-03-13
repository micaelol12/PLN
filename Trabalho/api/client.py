import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

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
    
    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def get_deputados(self, item_id: int):
        response = await self.client.get(f"{self.base_url}deputados/{item_id}")
        response.raise_for_status()
        json = response.json()
        json["id"] = item_id

        return json
    
    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def get_discursos(self, item_id: int):
        response = await self.client.get(f"{self.base_url}deputados/{item_id}/discursos?ordenarPor=dataHoraInicio&ordem=DESC&pagina=1")
        response.raise_for_status()
        json = response.json()
        json["id"] = item_id

        return json
    
    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def get_preoposicoes(self, item_id: int):
        response = await self.client.get(f"{self.base_url}proposicoes?idDeputadoAutor={item_id}&pagina=1&ordem=ASC&ordenarPor=id")
        response.raise_for_status()
        json = response.json()
        json["id"] = item_id

        return json
    
    async def close(self):
        await self.client.aclose()