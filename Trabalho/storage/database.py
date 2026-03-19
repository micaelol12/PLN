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
            id INTEGER,
            id_discurso INTEGER,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            json TEXT,
            sumario TEXT,
            transcricao TEXT,
            PRIMARY KEY (id, id_discurso)
        );
        """)

        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS preposicao (
            id INTEGER,
            id_preposicao INTEGER,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            json TEXT,
            PRIMARY KEY (id, id_preposicao)
        )
        """)

        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS preposicao_detalhes (
            id INTEGER PRIMARY KEY,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            json TEXT,
            ementa TEXT,
            texto_pdf TEXT
        )
        """)

    def save_ids(self, ids):
        data = [(i,) for i in ids]

        self.conn.executemany(
            "INSERT OR IGNORE INTO id_deputado (id) VALUES (?)",
            data
        )

        self.conn.commit()

    def save_deputado(self, item):
        dados = item["dados"]

        if len(dados) == 0:
            return

        self.conn.execute(
            "INSERT OR IGNORE INTO deputado (id, json) VALUES (?, ?)",
            (item["id"], json.dumps(dados))
        )

        self.conn.commit()

    def save_preposicoes(self, item):
        dados = item["dados"]

        if len(dados) == 0:
            return

        for d in item["dados"]:
            self.conn.execute(
                "INSERT OR IGNORE INTO preposicao (id,id_preposicao, json) VALUES (?, ?, ?)",
                (item["id"], d["id"], json.dumps(d))
            )

        self.conn.commit()

    def save_discursos(self, item):
        dados = item["dados"]

        if len(dados) == 0:
            return

        id_discurso = 0
        for d in item["dados"]:
            self.conn.execute(
                "INSERT OR IGNORE INTO discurso (id, id_discurso, json, sumario, transcricao) VALUES (?,?,?,?,?)",
                (item["id"], id_discurso, json.dumps(
                    d), d["sumario"], d["transcricao"])
            )
            id_discurso += 1

        self.conn.commit()

    def save_preoposicao_detalhe(self, item):
        dados = item["dados"]

        if len(dados) == 0:
            return

        self.conn.execute(
            "INSERT OR IGNORE INTO preposicao_detalhes (id, json, ementa, texto_pdf) VALUES (?,?,?,?)",
            (dados["id"], json.dumps(dados),
             dados["ementa"], dados["texto_pdf"])
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
    id AS proposicao_id,
    json_extract(json,'$.texto_pdf') AS texto_pdf
FROM preposicao_detalhes
WHERE json_extract(json,'$.texto_pdf') IS NOT NULL
  AND TRIM(json_extract(json,'$.texto_pdf')) != ''; """)

        return [row[0] for row in cursor.fetchall()]

    def get_discursos_ids(self):
        cursor = self.conn.execute("SELECT id FROM discurso")

        return [row[0] for row in cursor.fetchall()]

    def get_preposicoes_ids(self):
        cursor = self.conn.execute(
            "SELECT preposicao.id_preposicao FROM preposicao")

        return [row[0] for row in cursor.fetchall()]
