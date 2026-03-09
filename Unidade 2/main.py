import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd

URL = 'https://research.ebsco.com/c/5ovcoe/search/results?q=Din%C3%A2micas+Regionais+e+Desenvolvimento&autocorrect=n&expanders=fullText&expanders=concept&facetFilter=sourceTypes%3ANTUwRFM%3D&limiters=FT1%3AY&searchMode=all&searchSegment=all-results&skipResultsFetch=true&sqId=sq%3Ad3394bf3-ed6a-4543-8688-acba0eba1aa9'
WEB_DRIVER_TIMEOUT = 5
WEB_DRIVER_MAX_WORKERS = 10
HEADLESS = True

def criar_driver():
    options = webdriver.ChromeOptions()
    
    if HEADLESS:
        options.add_argument("--headless")
    
    return webdriver.Chrome(options=options)


def carrega_todos_links(driver):
    """
    Carrega todos os links da página, clicando no botão de carregar mais até ele deixar de aparecer

    Args:
        driver (webdriver): Driver.
    """
    while True:
        try:
            wait = WebDriverWait(driver, WEB_DRIVER_TIMEOUT)
            botao_mais = (By.CSS_SELECTOR, '[class="eb-button eb-button--default eb-pagination__button"]')
            botao = wait.until(
                EC.element_to_be_clickable(botao_mais)
            )
            driver.execute_script("arguments[0].click();", botao)

            time.sleep(2)
        except Exception as e:
            print("Não tem mais botão")
            break

def pega_links(driver):
    """
    Busca todos os links da página

    Args:
        driver (webdriver): Driver.
    Returns:
        list: Lista dos links encontrados.
    """
    wait = WebDriverWait(driver, WEB_DRIVER_TIMEOUT)
    links_selector = '[data-auto="result-item-title__link"]'
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, links_selector)))
    links = driver.find_elements(By.CSS_SELECTOR, links_selector)
    urls = [l.get_attribute("href") for l in links]

    print('Urls econtradas: ', urls)
    return urls

def pegar_texto(driver, selector):
    try:
        return driver.find_element(By.CSS_SELECTOR, selector).text
    except:
        return ""
    
def pegar_link(driver, selector):
    try:
        return driver.find_element(By.CSS_SELECTOR, selector).get_attribute("href")
    except:
        return ""
    
def pegar_dados_link(url):
    """
    Busca os dados da página do artigo

    Args:
        url (string): String da página.
    Returns:
        dict: Dados do artigo.
    """
    
    driver = criar_driver()
    try:
            driver.get(url)

            wait = WebDriverWait(driver, WEB_DRIVER_TIMEOUT)

            wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#TypDoc + ul li"))
            )

            dados = {}

            resumos = driver.find_elements(By.CSS_SELECTOR, "#Ab + ul li")
            palavras_chaves = driver.find_elements(By.CSS_SELECTOR, "#Su + ul li")

            dados["tipo_documento"] = pegar_texto(driver, "#TypDoc + ul li")
            dados["autor"] = pegar_texto(driver, "#Au + ul li")
            dados["titulo"] = pegar_texto(driver, "#Ti + ul li")
            dados["palavras_chave"] = [p.text for p in palavras_chaves] if palavras_chaves else ""
            dados["link_PDF"] = pegar_link(driver, "#URL + ul a")
            dados["fonte_url"] = driver.current_url
            dados["ano"] = pegar_texto(driver, "#Date + ul li")
            dados["resumo"] = resumos[0].text if resumos else ""
            
            print("Artigo processado com sucesso!", dados["titulo"])

            return dados
    except Exception as e:
            print("Erro ao pegar dados", e)
    finally:
            driver.quit()

def processar_links(urls):
    """
    Procesa todos os links e pega os dados artigos

    Args:
        url (list): Urls das páginas a serem processadas.
    Returns:
        list: Lista com todos os dados do artigo processados com sucesso.
    """
    dados = []
    
    with ThreadPoolExecutor(max_workers=WEB_DRIVER_MAX_WORKERS) as executor:
        futures = [executor.submit(pegar_dados_link, url) for url in urls]

        for future in as_completed(futures):
            try:
                resultado = future.result()
                
                if resultado:
                    dados.append(resultado)
            except Exception as e:
                print("Erro:", e)
                
    return dados

def salvar_excel(dados):
    if len(dados) == 0:
        return
    
    df = pd.DataFrame(dados)
    df.to_excel("Dados_UFRGS.xlsx", index=False)
    print("Arquivo Excel salvo com sucesso!")
        

driver = criar_driver()
driver.get(URL)

carrega_todos_links(driver)

urls = pega_links(driver)
dados = processar_links(urls)

salvar_excel(dados)


