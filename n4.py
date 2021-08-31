from logging import getLogger, StreamHandler, DEBUG
import re
import json
import os
from itertools import chain, cycle, takewhile
from timeit import timeit

from tqdm import tqdm
from more_itertools import chunked

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from neo4j import GraphDatabase

handler = StreamHandler()
handler.setLevel(DEBUG)
getLogger("neo4j").addHandler(handler)

index_name = "pile_index"

class KG:
    def __init__(self, uri, user, password, page_size=200):
        self.page_size = page_size
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def setup(self):
        query = '''CALL n10s.graphconfig.init({handleVocabUris: "KEEP", handleMultival: "ARRAY", keepCustomDataTypes: true});'''

        self.run(query)

        query = '''CREATE CONSTRAINT n10s_unique_uri ON (r:Resource)
                ASSERT r.uri IS UNIQUE;
                '''

        self.run(query)

    def load_data(self):
        queries = [
            '''CALL n10s.rdf.import.fetch("file:///home/yago/yago-wd-schema.nt", "N-Triples");''',
            '''CALL n10s.rdf.import.fetch("file:///home/yago/yago-wd-class.nt", "N-Triples");''',
            '''CALL n10s.rdf.import.fetch("file:///home/yago/yago-wd-facts.nt", "N-Triples");''',
            '''CALL n10s.rdf.import.fetch("file:///home/yago/yago-wd-labels.nt", "N-Triples");''',
        ]

        for query in queries:
            self.run(query)

    
    def export(self):
        '''CALL apoc.export.json.query("MATCH (s)-[r]->(o) RETURN s,r,o;",null, {useTypes:true, stream: true})
        YIELD data
        RETURN data;
        '''

        num_pages = 10

        for i in range(num_pages):
            query = f'''CALL apoc.export.json.query("MATCH (s)-[r]->(o) RETURN s,r,o SKIP {self.page_size * i+35000} LIMIT {self.page_size};",null, {{useTypes:true, stream: true}})
        YIELD data
        '''

            work_result, data = self.read(query)
            j = data[0]['data']

            rels = j.split('\n')

            for rel in map(json.loads, rels):
                s = rel['s']
                r = rel['r']
                o = rel['o']

                yield s, r, o

    def run(self, q):
        with self.driver.session() as sess:
            result = sess.run(q)
            for r in result:
                print(r)

    def read(self, q):
        with self.driver.session() as sess:
            result = sess.read_transaction(self._query, q)
            return result

    def _query(self, tx, query):
        ret = []
        result = tx.run(query)
        for k in result:
            ret.append(k)
        return result, ret
