import sqlite3
import json

class Database:

    def __init__(self, path="data.db"):
        self.conn = sqlite3.connect(path)
        self._create_table()

    def _create_table(self):
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS IDS (
            id INTEGER PRIMARY KEY
        )
        """)
        
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS Deputados (
            id INTEGER PRIMARY KEY,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            json TEXT
        )
        """)
        
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS Discursos (
            id INTEGER PRIMARY KEY,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            json TEXT
        )
        """)
        
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS preposicoes (
            id INTEGER PRIMARY KEY,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            json TEXT
        )
        """)
        
    def save_ids(self,ids):
        data = [(i,) for i in ids]
           
        self.conn.executemany(
            "INSERT OR IGNORE INTO IDS (id) VALUES (?)",
            data
        )

        self.conn.commit()
        
    def save_deputado(self,item):
        dados = item["dados"]
        
        if len(dados) == 0:
            return
        
        self.conn.execute(
            "INSERT OR IGNORE INTO Deputados (id, json) VALUES (?, ?)",
             (item["id"], json.dumps(dados))
        )

        self.conn.commit()
        
    def save_preposicoes(self,item):
        dados = item["dados"]
        
        if len(dados) == 0:
            return
        
        self.conn.execute(
            "INSERT OR IGNORE INTO preposicoes (id, json) VALUES (?, ?)",
             (item["id"], json.dumps(dados))
        )

        self.conn.commit()
        
        
    def save_discursos(self,item):
        dados = item["dados"]
        
        if len(dados) == 0:
            return
        
        self.conn.execute(
            "INSERT OR IGNORE INTO discursos (id, json) VALUES (?, ?)",
             (item["id"], json.dumps(dados))
        )

        self.conn.commit()
        
    def get_ids(self):
        cursor = self.conn.execute("SELECT id FROM IDS")

        return [row[0] for row in cursor.fetchall()]
        