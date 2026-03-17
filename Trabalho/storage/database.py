import sqlite3
import json

class Database:
    def __init__(self, path="data.db"):
        self.conn = sqlite3.connect(path)
        self._create_table()

    def _create_table(self):
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS id_deputado (
            id INTEGER PRIMARY KEY
        )
        """)
        
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS deputado (
            id INTEGER PRIMARY KEY,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            json TEXT
        )
        """)
        
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS discurso (
            id INTEGER PRIMARY KEY,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            json TEXT
        )
        """)
        
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS preposicao (
            id INTEGER PRIMARY KEY,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            json TEXT
        )
        """)
          
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS preposicao_detalhes (
            id INTEGER PRIMARY KEY,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            json TEXT
        )
        """)
        
    def save_ids(self,ids):
        data = [(i,) for i in ids]
           
        self.conn.executemany(
            "INSERT OR IGNORE INTO id_deputado (id) VALUES (?)",
            data
        )

        self.conn.commit()
        
    def save_deputado(self,item):
        dados = item["dados"]
        
        if len(dados) == 0:
            return
        
        self.conn.execute(
            "INSERT OR IGNORE INTO deputado (id, json) VALUES (?, ?)",
             (item["id"], json.dumps(dados))
        )

        self.conn.commit()
        
    def save_preposicoes(self,item):
        dados = item["dados"]
        
        if len(dados) == 0:
            return
        
        self.conn.execute(
            "INSERT OR IGNORE INTO preposicao (id, json) VALUES (?, ?)",
             (item["id"], json.dumps(dados))
        )

        self.conn.commit()
        
        
    def save_discursos(self,item):
        dados = item["dados"]
        
        if len(dados) == 0:
            return
        
        self.conn.execute(
            "INSERT OR IGNORE INTO discurso (id, json) VALUES (?, ?)",
             (item["id"], json.dumps(dados))
        )

        self.conn.commit()
        
    def save_preoposicao_detalhe(self,item):
        dados = item["dados"]
        
        if len(dados) == 0:
            return
        
        self.conn.execute(
            "INSERT OR IGNORE INTO preposicao_detalhes (id, json) VALUES (?, ?)",
             (dados["id"], json.dumps(dados))
        )

        self.conn.commit()
        
    def get_ids(self):
        cursor = self.conn.execute("SELECT id FROM id_deputado")

        return [row[0] for row in cursor.fetchall()]
    
    def get_deputados_ids(self):
        cursor = self.conn.execute("SELECT id FROM deputado")

        return [row[0] for row in cursor.fetchall()]
    
    def get_preposicoes_ids(self):
        cursor = self.conn.execute("SELECT id FROM preposicao")

        return [row[0] for row in cursor.fetchall()]
    
    
    def get_preposicoes_detalhes_ids(self):
        cursor = self.conn.execute("SELECT id FROM preposicao_detalhes")

        return [row[0] for row in cursor.fetchall()]
    
    
    def get_preposicoes_detalhes_com_pdf_ids(self):
        cursor = self.conn.execute("""
    SELECT 
    json_extract(json,'$.id') AS proposicao_id,
    json_extract(json,'$.texto_pdf') AS texto_pdf
FROM preposicao_detalhes
WHERE json_extract(json,'$.texto_pdf') IS NOT NULL
  AND TRIM(json_extract(json,'$.texto_pdf')) != ''; """)

        return [row[0] for row in cursor.fetchall()]
    
    
    def get_discursos_ids(self):
        cursor = self.conn.execute("SELECT id FROM discurso")

        return [row[0] for row in cursor.fetchall()]
    
    def get_preposicoes_ids(self):
        cursor = self.conn.execute("""
SELECT 
    json_extract(value, '$.id') AS proposicao_id
FROM preposicao,
json_each(preposicao.json)""")

        return [row[0] for row in cursor.fetchall()]
        