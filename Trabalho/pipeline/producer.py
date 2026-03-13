async def produzir_ids(api_client):
    ids = []

    ids = await api_client.produzir_ids()

    return ids