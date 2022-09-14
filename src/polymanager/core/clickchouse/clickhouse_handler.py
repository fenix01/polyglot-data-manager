import requests
import logging
from polymanager.core.document_handler import DocumentHandler

class ClickhouseHandler(DocumentHandler):

    def __init__(
                self,
                hostname="127.0.0.1", port="8123"
                ):
        self.hostname = hostname
        self.port = port
        self.url = "http://{}:{}".format(self.hostname, self.port)
        self._log = logging.getLogger(__name__)

    def ping(self):
        res = requests.get(self.url+"/ping", timeout=120)
        return res.text

    def insert_schema(self, schema):
        gen_chema = schema.generate_schema()
        if gen_chema:
            has_database = "query=SHOW TABLES FROM {}".format(schema.get_namespace())
            res = requests.post(self.url+"?{}".format(has_database), timeout=120)
            if res.status_code == 404:
                #we create the database
                self.query("CREATE DATABASE {}".format(schema.get_namespace()))
            res = requests.post(self.url+"?{}".format(gen_chema), timeout=120)
            print(res.text)

    def get_tables(self, database):
        req = "query=show tables from {};".format(database)
        res = requests.get(self.url+"?{}".format(req), timeout=120)
        return res.text.splitlines()

    def query(self, query):
        req = "query={}".format(query)
        res = requests.post(self.url+"?{}".format(req), timeout=120)
        return res

    def truncate(self, table):
        req = "query=TRUNCATE TABLE {}".format(table)
        res = requests.post(self.url+"?{}".format(req), timeout=120)
        print(res.text)

    def delete_document(self, table, doc_id):
        req = "query=ALTER TABLE {} DELETE WHERE id={} SETTINGS mutations_sync = 2".format(table, doc_id)
        res = requests.post(self.url+"?{}".format(req), timeout=120)
        print(res.text)

    def delete_documents(self, table, docs_id):
        req = "query=ALTER TABLE {} DELETE WHERE id IN ({}) SETTINGS mutations_sync = 2".format(table, ",".join(docs_id))
        res = requests.post(self.url+"?{}".format(req), timeout=120)
        print(res.text)

    def delete_schema(self, schema):
        req = "query=DROP TABLE {}".format(schema.get_collection_name())
        res = requests.post(self.url+"?{}".format(req), timeout=120)
        req = "query=SHOW TABLES FROM {}".format(schema.get_namespace())
        res2 = requests.post(self.url+"?{}".format(req), timeout=120)
        if res2.status_code == 200:
            req = "query=DROP DATABASE {}".format(schema.get_namespace())
            res = requests.post(self.url+"?{}".format(req), timeout=120)

    def bulk_replace_documents(self, collection_name, documents_list):
        pass

    def bulk_add_documents(self, collection_name, documents_list):
        c_documents = []
        for document in documents_list:
            values = []
            for v in document.values():
                if isinstance(v, str):
                    values.append("'"+v+"'")
                elif isinstance(v, int) or isinstance(v, float):
                    values.append(str(v))
                else:
                    values.append(str(v))
            c_documents.append("("+ ",".join(values) +")")
        sql = "INSERT INTO {} ({}) VALUES {}".format(collection_name, ",".join(documents_list[0].keys()), ",".join(c_documents))
        res = self.query(sql)
        if res.status_code != 200:
            raise Exception(res.text)
        return res.text

    def replace_document(self, collection, document):
        values = []
        for k,v in document.items():
            if k == "id":
                continue
            if isinstance(v, str):
                values.append(k+"='"+v+"'")
            elif isinstance(v, int) or isinstance(v, float):
                values.append(k+"="+str(v))
            else:
                values.append(k+"="+str(v))
        sql = "ALTER TABLE {} UPDATE {} WHERE id={} SETTINGS mutations_sync = 2".format(
            collection,
            ",".join(values),
            document["id"])
        res = self.query(sql)
        return res.text