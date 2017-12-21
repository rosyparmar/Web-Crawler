from elasticsearch import Elasticsearch
from html_info import html_info

class ES(object):
    def __init__(self, index_Name = None, doc_Type = None):
        if(index_Name == None or doc_Type == None):
            raise ("Cannot be none")
        self.index_Name = index_Name
        self.doc_Type = doc_Type
        request_body = {
            "settings": {
                "index": {
                    "store": {
                        "type": "default"
                    },
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            }
        }
        self.es = Elasticsearch()
        self.data = {}
        self.es.indices.create(index = index_Name,
                                body = request_body)

        self.es.indices.put_mapping(
            index= index_Name,
            doc_type=doc_Type,
            body={
                doc_Type: {
                    'properties': {
                    'url': {'type': 'string',
                            'store': True,
                            'index': 'not_analyzed'
                            },
                    'text': {'type': 'string',
                             'store': True,
                             'index': 'analyzed',
                             'term_vector': 'with_positions_offsets_payloads',
                             },
                    'title': {'type': 'string',
                              'store': True},
                    'httpheader': { 'type': 'string',
                                    'store': True},
                    'html': {'type': 'string'},
                    'outlinks': {'type': 'string'},
                    'inlinks': {'type': 'string'},
                    'author': {'type': 'string'}
                    }
                }
            })

    def convertInOutLinksToText(self, links):
        allLinks = []
        for link in links:
            allLinks.append(link)
        return ",".join(allLinks)

    def  addToES(self):
        self.es.bulk(index=self.index_Name, body = ind)
        return

    def updateElasticSearch(self, url, inlinks):
        if (not inlinks.url in self.data):
            action = {'update': {'_index': self.indexName, '_type': self.docType, '_id': url}}
            body = {
                "script": "ctx._source.inlinks+=new_link",
                "params": {"new_link": inlinks}
            }
        else:
            record = self.data[url]["record"]
            record["inlinks"] = record["inlinks"].append(inlinks)
            self.data[url] = {"action": self.data[url]["action"],
                                "record": self.body}

        if len(self.data) >= 50:
            index = []
            for record in self.data:
                index.append(self.data[record]["action"])
                index.append(self.data[record]["records"])
            self.records = {}

            self.es.bulk(index=self.indexName, body=index)
        return


if __name__ == '__main__':
    es = ES(index_Name = "app_dataset64", doc_Type = "indexes1")
    es.addToES()