from pdfminer.high_level import extract_text
import re
import io

def extrair_texto_pdf(pdf_bytes):

    with io.BytesIO(pdf_bytes) as pdf_file:
        texto = extract_text(pdf_file)

    return texto

async def baixar_pdf(client, url):
    try:
        response = await client.get(url)

        response.raise_for_status()
        
        if "text/html" in response.headers.get("content-type",""):
            redirect_url = extrair_redirect(response.text)

            if redirect_url:
                response = await client.get(redirect_url)

        if "pdf" not in response.headers.get("content-type",""):
            return None
        
        return response.content
    except:
        return None

async def baixar_e_extrair_pdf(client, url):

    pdf_bytes = await baixar_pdf(client, url)
    
    if pdf_bytes is None:
        return ""

    texto = extrair_texto_pdf(pdf_bytes)

    return texto

def extrair_redirect(html):

    match = re.search(r'URL=([^"]+)', html)

    if match:
        return match.group(1)

    return None