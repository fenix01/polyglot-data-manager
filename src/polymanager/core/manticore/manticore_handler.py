import json
import requests
import logging
from polymanager.core.document_handler import DocumentHandler

class ManticoreHandler(DocumentHandler):

    def __init__(
                self,
                hostname="127.0.0.1", port="9308"
                ):
        self.hostname = hostname
        self.port = port
        self.url = "http://{}:{}".format(self.hostname, self.port)
        self._log = logging.getLogger(__name__)

    def insert_schema(self, schema):
        gen_chema = schema.generate_schema()
        if gen_chema:
            res = requests.post(self.url+"/sql",data=gen_chema)
            print(res.text)

    def get_tables(self):
        req = "mode=raw&query=show tables;"
        res = requests.post(self.url+"/sql",data=req)
        return json.loads(res.text)

    def query(self, query):
        req = "mode=raw&query={}".format(query)
        res = requests.post(self.url+"/sql",data=req)
        return json.loads(res.text)

    def truncate(self, table):
        req = "mode=raw&query=TRUNCATE TABLE {}".format(table)
        res = requests.post(self.url+"/sql", data=req)
        print(res.text)

    def delete_document(self, table, doc_id):
        req = "mode=raw&query=DELETE FROM {} WHERE id={}".format(table, doc_id)
        res = requests.post(self.url+"/sql",
        data=req)
        print(res.text)

    def delete_documents(self, table, docs_id):
        req = "mode=raw&query=DELETE FROM {} WHERE id IN ({})".format(table, ",".join(docs_id))
        res = requests.post(self.url+"/sql",
        data=req)
        print(res.text)

    def delete_schema(self, schema):
        req = "mode=raw&query=DROP TABLE IF EXISTS {}".format(schema.get_collection_name())
        res = requests.post(self.url+"/sql",
        data=req)
        print(res.text)

    def build_add_documents(self, index_name, documents_list):
        ndjson = ""
        if not documents_list:
            raise Exception("documents list is empty")
        for document_obj in documents_list:
            command = dict()
            command["insert"] = dict()
            doc = command["insert"]
            doc["index"] = index_name
            doc["id"] = document_obj["id"]
            doc["doc"] = dict(document_obj)
            doc["doc"].pop("id")
            #doc["doc"].update(document_obj)
            ndjson += json.dumps(command) + "\n"
        return ndjson

    def build_replace_documents(self, index_name, documents_list):
        ndjson = ""
        if not documents_list:
            raise Exception("documents list is empty")
        for document_obj in documents_list:
            command = dict()
            command["replace"] = dict()
            doc = command["replace"]
            doc["index"] = index_name
            doc["id"] = document_obj["id"]
            doc["doc"] = dict(document_obj)
            doc["doc"].pop("id")
            #doc["doc"].update(document_obj)
            ndjson += json.dumps(command) + "\n"
        return ndjson

    def bulk_replace_documents(self, collection_name, documents_list):
        '''
        add a list of documents for indexation in manticore

        :param documents_list: A list of dict document with properties

        :return: A dict that tells for every node_id
        if it has been indexed or not
        '''
        url = self.url + "/json/bulk"
        ndjson = self.build_replace_documents(collection_name, documents_list)
        response = requests.get(
                                url,
                                headers={
                                    "Content-Type":
                                    "application/x-ndjson"
                                    },
                                data=ndjson
                                )

        res = {document["id"]: False for document in documents_list}
        if response.status_code == 200 or response.status_code == 500:
            parsed_resp = response.json()
            if "items" not in parsed_resp:
                raise Exception(response.json())
            for indice, item in enumerate(parsed_resp["items"]):
                node_id = documents_list[indice]["id"]
                if "replace" not in item:
                    raise Exception(response.json())
                insert_item = item["replace"]
                if insert_item["status"] == 200:
                    res[node_id] = True
            return res
        else:
            raise Exception(response.json())

    def bulk_add_documents(self, collection_name, documents_list):
        '''
        add a list of documents for indexation in manticore

        :param documents_list: A list of dict document with properties

        :return: A dict that tells for every node_id
        if it has been indexed or not
        '''
        url = self.url + "/json/bulk"
        ndjson = self.build_add_documents(collection_name, documents_list)
        response = requests.get(
                                url,
                                headers={
                                    "Content-Type":
                                    "application/x-ndjson"
                                    },
                                data=ndjson
                                )

        res = {document["id"]: False for document in documents_list}
        if response.status_code == 200 or response.status_code == 500:
            parsed_resp = response.json()
            if "items" not in parsed_resp:
                raise Exception(response.json())
            for indice, item in enumerate(parsed_resp["items"]):
                doc_id = documents_list[indice]["id"]
                if "insert" not in item:
                    raise Exception(response.json())
                insert_item = item["insert"]
                if insert_item["status"] == 201:
                    res[doc_id] = True
            return res
        else:
            raise Exception(response.json())

    def add_document(self, document):
        url = self.url + "/json/insert"
        doc = dict()
        doc.update(document)
        doc["index"] = self.index_name
        doc["id"] = int(document["node_id"], 16)
        doc["doc"] = dict()
        doc["doc"].update(document)

        response = requests.get(url, headers={"Content-Type":
                                "application/json"}, data=json.dumps(doc))
        if response.status_code != 200:
            raise Exception(response.json())
        parsed_resp = response.json()
        if parsed_resp["status"] != 201:
            return False
        if not parsed_resp["created"]:
            return False
        return True

    def page_documents(self, index_name, request, page, per_page, sort_attr, threats, countries):
        url = self.url + "/json/search"
        doc = dict()
        doc["index"] = index_name
        doc["limit"] = per_page
        doc["offset"] = (page - 1) * per_page
        doc["query"] = dict()
        doc["query"]["bool"] = dict()
        doc["query"]["bool"]["must"] = list()
        bool_query = doc["query"]["bool"]["must"]

        bool_query.append({"match" : {"_all" : request}})

        if threats:
            for threat in threats:
                bool_query.append({"equals" : {"threats" : threat}})
        if countries:
            for country in countries:
                bool_query.append({"equals" : {"countries" : country}})
        if sort_attr:
            doc["sort"] = [
                {sort_attr: {"order": "desc"}},
                {"_score": {"order": "desc"}}
                ]

        self._log.info(doc)
        response = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(doc))
        if response.status_code != 200:
            raise Exception(response.json())
        parsed_resp = response.json()
        if not parsed_resp["hits"]:
            raise Exception(response.json())
        return parsed_resp["hits"]["hits"]