import re
from bs4 import BeautifulSoup
from nltk.stem import RSLPStemmer
import unicodedata
import spacy
import nltk

nltk.download('rslp')
nlp = spacy.load("pt_core_news_sm")


def limpar_texto(texto: str):
    # remove HTML
    texto = BeautifulSoup(texto, "html.parser").get_text()
    # remove URLs
    texto = re.sub(r"http\S+", "", texto)
    # remove caracteres especiais (mantém acentos)
    texto = re.sub(r"[^a-zA-ZÀ-ÿ\s]", "", texto)
    # normaliza espaços
    texto = re.sub(r"\s+", " ", texto).strip()

    return texto


def remover_acentos(texto: str):
    texto = unicodedata.normalize('NFKD', texto)
    texto = ''.join(c for c in texto if not unicodedata.combining(c))
    return texto


def remover_stopwords(texto: str):
    doc = nlp(texto)
    return " ".join([token.text for token in doc if not token.is_stop])


def normalizar_texto(texto: str):
    texto = texto.lower()
    texto = remover_acentos(texto)
    texto = remover_stopwords(texto)
    
    return texto

def tokenizar_texto(texto: str):
    doc = nlp(texto)
    stemmer = RSLPStemmer()

    tokens = [token.text for token in doc if token.is_alpha]
    lemmas = [token.lemma_ for token in doc if token.is_alpha]
    stems = [stemmer.stem(token.text) for token in doc if token.is_alpha]

    return tokens, lemmas, stems

def pre_processar_texto(texto: str):
    texto_limpo = limpar_texto(texto)
    texto_normalizado = normalizar_texto(texto_limpo)
    tokens, lemmas, stems = tokenizar_texto(texto_normalizado)

    return texto_limpo, texto_normalizado, tokens, lemmas, stems
