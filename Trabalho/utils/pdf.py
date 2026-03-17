from pdfminer.high_level import extract_text
import io

def extrair_texto_pdf(pdf_bytes):

    with io.BytesIO(pdf_bytes) as pdf_file:
        texto = extract_text(pdf_file)

    return texto

async def baixar_pdf(client, url):

    response = await client.get(url)

    response.raise_for_status()

    content_type = response.headers.get("content-type", "")

    if "pdf" not in content_type:
        print("Não é PDF:", url)
        return None
    
    return response.content

async def baixar_e_extrair_pdf(client, url):

    pdf_bytes = await baixar_pdf(client, url)
    
    if pdf_bytes is None:
        return ""

    texto = extrair_texto_pdf(pdf_bytes)

    return texto

